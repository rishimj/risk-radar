"""A SQLite stand-in for the slice of the boto3 DynamoDB Table API RiskRadar uses.

Selected with DB_BACKEND=sqlite. It exists for the single-node deployment
(services/standalone), where running DynamoDB Local means running a JVM, and
real DynamoDB means AWS credentials and a bill. Neither belongs on a small
shared VM hosting a public demo.

Only what riskcore.db, riskcore.alerting and the webapp's store actually call is
implemented: put_item, get_item, update_item (plain `SET a = :x, ...`),
delete_item, query (equality key conditions, optionally ANDed, on the table or
a GSI, with Limit and ScanIndexForward), scan, and batch_writer. Anything else
raises NotImplementedError rather than silently doing the wrong thing.

Each logical table is one SQLite table of (pk, sk, doc JSON). GSI lookups use
json_extract with an expression index, which is plenty for a demo's row counts.
"""
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json
import re
import sqlite3
import threading

_SET = re.compile(r"^\s*SET\s+(.+)$", re.IGNORECASE | re.DOTALL)
_ASSIGN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(:[A-Za-z0-9_]+)\s*$")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _json_default(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


def _ident(name: str) -> str:
    """Table and attribute names are interpolated into SQL, so allow only identifiers."""
    if not _IDENT.match(name):
        raise ValueError(f"illegal identifier: {name!r}")
    return name


def _key_schema(spec: List[Dict]) -> Tuple[str, Optional[str]]:
    hash_key = next(k["AttributeName"] for k in spec if k["KeyType"] == "HASH")
    range_key = next((k["AttributeName"] for k in spec if k["KeyType"] == "RANGE"), None)
    return hash_key, range_key


def _equalities(condition) -> Dict[str, Any]:
    """Flatten Key(a).eq(x) [& Key(b).eq(y)] into {a: x, b: y}."""
    expr = condition.get_expression()
    op = expr["operator"]
    if op == "AND":
        out: Dict[str, Any] = {}
        for part in expr["values"]:
            out.update(_equalities(part))
        return out
    if op == "=":
        key, value = expr["values"]
        return {key.name: value}
    raise NotImplementedError(f"key condition operator {op!r}")


class Database:
    """One SQLite file; every Table shares its connection and lock."""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.specs: Dict[str, Dict] = {}

    def create_table(self, name: str, spec: Dict) -> bool:
        t = _ident(name)
        with self.lock:
            exists = self.conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)
            ).fetchone()
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {t} ("
                "pk TEXT NOT NULL, sk TEXT NOT NULL DEFAULT '', doc TEXT NOT NULL, "
                "PRIMARY KEY (pk, sk))"
            )
            for gsi in spec.get("GlobalSecondaryIndexes", []):
                h, _ = _key_schema(gsi["KeySchema"])
                idx = _ident(f"{t}__{gsi['IndexName'].replace('-', '_')}")
                self.conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {idx} "
                    f"ON {t} (json_extract(doc, '$.{_ident(h)}'))"
                )
            self.specs[name] = spec
        return not exists

    def table_names(self) -> List[str]:
        with self.lock:
            return [r[0] for r in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")]

    def table(self, name: str, spec: Dict) -> "Table":
        self.specs.setdefault(name, spec)
        return Table(self, name, spec)


class Table:
    def __init__(self, db: Database, name: str, spec: Dict):
        self.db = db
        self.name = _ident(name)
        self.spec = spec
        self.hash_key, self.range_key = _key_schema(spec["KeySchema"])

    # -- helpers ---------------------------------------------------------
    def _pk_sk(self, item: Dict) -> Tuple[str, str]:
        try:
            pk = str(item[self.hash_key])
            sk = str(item[self.range_key]) if self.range_key else ""
        except KeyError as exc:
            raise ValueError(f"{self.name}: missing key attribute {exc}") from None
        return pk, sk

    def _rows(self, sql: str, args: Iterable) -> List[Dict]:
        with self.db.lock:
            return [json.loads(r[0]) for r in self.db.conn.execute(sql, tuple(args))]

    # -- writes ----------------------------------------------------------
    def put_item(self, Item: Dict, **_) -> Dict:
        pk, sk = self._pk_sk(Item)
        doc = json.dumps(Item, default=_json_default, separators=(",", ":"))
        with self.db.lock:
            self.db.conn.execute(
                f"INSERT OR REPLACE INTO {self.name} (pk, sk, doc) VALUES (?, ?, ?)",
                (pk, sk, doc),
            )
        return {}

    def delete_item(self, Key: Dict, **_) -> Dict:
        pk, sk = self._pk_sk(Key)
        with self.db.lock:
            self.db.conn.execute(f"DELETE FROM {self.name} WHERE pk=? AND sk=?", (pk, sk))
        return {}

    def update_item(self, Key: Dict, UpdateExpression: str,
                    ExpressionAttributeValues: Dict, **_) -> Dict:
        m = _SET.match(UpdateExpression)
        if not m:
            raise NotImplementedError(f"update expression {UpdateExpression!r}")
        changes = {}
        for clause in m.group(1).split(","):
            a = _ASSIGN.match(clause)
            if not a:
                raise NotImplementedError(f"update clause {clause!r}")
            changes[a.group(1)] = ExpressionAttributeValues[a.group(2)]
        with self.db.lock:
            item = self.get_item(Key=Key).get("Item") or dict(Key)
            item.update(changes)
            self.put_item(Item=item)
        return {}

    @contextmanager
    def batch_writer(self, **_):
        yield self

    # -- reads -----------------------------------------------------------
    def get_item(self, Key: Dict, **_) -> Dict:
        pk, sk = self._pk_sk(Key)
        rows = self._rows(f"SELECT doc FROM {self.name} WHERE pk=? AND sk=?", (pk, sk))
        return {"Item": rows[0]} if rows else {}

    def scan(self, **_) -> Dict:
        items = self._rows(f"SELECT doc FROM {self.name}", ())
        return {"Items": items, "Count": len(items)}

    def query(self, KeyConditionExpression, IndexName: Optional[str] = None,
              Limit: Optional[int] = None, ScanIndexForward: bool = True, **extra) -> Dict:
        unsupported = set(extra) - {"Select", "ConsistentRead"}
        if unsupported:
            raise NotImplementedError(f"query options {sorted(unsupported)}")

        if IndexName:
            gsi = next((g for g in self.spec.get("GlobalSecondaryIndexes", [])
                        if g["IndexName"] == IndexName), None)
            if gsi is None:
                raise ValueError(f"{self.name}: no index {IndexName!r}")
            hash_key, range_key = _key_schema(gsi["KeySchema"])
        else:
            hash_key, range_key = self.hash_key, self.range_key

        wanted = _equalities(KeyConditionExpression)
        if hash_key not in wanted:
            raise ValueError(f"query must constrain the hash key {hash_key!r}")

        where, args = [], []
        for attr, value in wanted.items():
            if not IndexName and attr == self.hash_key:
                where.append("pk = ?")
            elif not IndexName and attr == self.range_key:
                where.append("sk = ?")
            else:
                where.append(f"json_extract(doc, '$.{_ident(attr)}') = ?")
            args.append(str(value))

        order = ""
        if range_key:
            col = "sk" if not IndexName else f"json_extract(doc, '$.{_ident(range_key)}')"
            order = f" ORDER BY {col} {'ASC' if ScanIndexForward else 'DESC'}"
        limit = ""
        if Limit is not None:
            limit = " LIMIT ?"
            args.append(int(Limit))

        items = self._rows(
            f"SELECT doc FROM {self.name} WHERE {' AND '.join(where)}{order}{limit}", args
        )
        return {"Items": items, "Count": len(items)}


_databases: Dict[str, Database] = {}
_lock = threading.Lock()


def database(path: str) -> Database:
    with _lock:
        if path not in _databases:
            _databases[path] = Database(path)
        return _databases[path]
