/* RiskRadar front end. No framework, no build step, no third-party script:
   the CSP allows scripts from this origin only.

   Safety rule for this file: every value that came from the server (headlines,
   sources, tickers, messages) reaches the DOM through esc() inside templates or
   through textContent. URLs additionally go through safeUrl(). */
const RiskRadar = (() => {
  "use strict";

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const safeUrl = (u) => (/^https?:\/\//i.test(String(u ?? "")) ? String(u) : "#");
  const SVG = "http://www.w3.org/2000/svg";
  const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  let uid = 0;

  function svgEl(tag, attrs = {}, parent) {
    const el = document.createElementNS(SVG, tag);
    for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
    if (parent) parent.appendChild(el);
    return el;
  }

  async function api(path, opts) {
    const res = await fetch(path, Object.assign({
      headers: { "Content-Type": "application/json" }, credentials: "same-origin",
    }, opts));
    if (!res.ok) {
      let detail = res.statusText;
      try { detail = (await res.json()).detail || detail; } catch (_) { /* not json */ }
      const err = new Error(detail);
      err.status = res.status;
      throw err;
    }
    return res.json();
  }

  function ago(iso) {
    if (!iso) return "";
    const secs = (Date.now() - new Date(iso).getTime()) / 1000;
    if (!isFinite(secs)) return "";
    if (secs < 45) return "just now";
    if (secs < 3600) return `${Math.max(1, Math.round(secs / 60))}m ago`;
    if (secs < 86400) return `${Math.round(secs / 3600)}h ago`;
    return `${Math.round(secs / 86400)}d ago`;
  }
  const clock = (iso) => {
    const d = new Date(iso);
    return isNaN(d) ? "" : d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  };
  const fmt = (v, d = 2) => (v === null || v === undefined || isNaN(v) ? "—" : Number(v).toFixed(d));
  const compact = (n) => new Intl.NumberFormat("en", { notation: n >= 10000 ? "compact" : "standard",
    maximumFractionDigits: 1 }).format(n);

  function chip(sentiment) {
    const v = Number(sentiment ?? 0);
    const cls = v <= -0.05 ? "neg" : v >= 0.05 ? "pos" : "neu";
    return `<span class="chip ${cls}" title="FinBERT sentiment ${v.toFixed(2)}"><i></i>${v > 0 ? "+" : ""}${v.toFixed(2)}</span>`;
  }

  /* ------------------------------------------------------------ tooltip */
  function tooltip(host) {
    const tip = document.createElement("div");
    tip.className = "tip";
    tip.setAttribute("role", "status");
    host.appendChild(tip);
    return {
      show(x, y, lines) {
        tip.textContent = "";
        lines.forEach(([text, cls], i) => {
          const el = document.createElement(i === 0 ? "strong" : "div");
          if (cls) el.className = cls;
          el.textContent = text;
          tip.appendChild(el);
        });
        const w = host.clientWidth;
        tip.style.left = `${Math.min(Math.max(x, 70), w - 70)}px`;
        tip.style.top = `${y}px`;
        tip.classList.add("on");
      },
      hide() { tip.classList.remove("on"); },
    };
  }

  /* ------------------------------------------------------------ sparkline
     One series (the ticker's alert score per window) against a reference line
     (that ticker's cut). Points above the cut get a status dot with a surface
     ring. Crosshair + tooltip on pointer and keyboard. */
  function sparkline(host, points, opts = {}) {
    host.textContent = "";
    const W = 300, H = opts.height || 64, padT = 8, padB = 4;
    const cut = opts.cut;
    const pts = (points || []).filter((p) => p && p.a !== undefined && p.a !== null);
    const svg = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "none",
      role: "img", "aria-label": opts.label || "Alert score history" }, host);
    svg.style.height = `${H}px`;
    if (pts.length < 2) {
      const t = svgEl("text", { x: W / 2, y: H / 2 + 4, "text-anchor": "middle", class: "axis-label" }, svg);
      t.textContent = pts.length ? "one window so far" : "waiting for news";
      return;
    }
    const max = Math.max(...pts.map((p) => p.a), cut || 0) * 1.12 || 1;
    const x = (i) => (i / (pts.length - 1)) * W;
    const y = (v) => padT + (1 - v / max) * (H - padT - padB);
    const gid = `sf${++uid}`;
    const defs = svgEl("defs", {}, svg);
    const lg = svgEl("linearGradient", { id: gid, x1: 0, y1: 0, x2: 0, y2: 1 }, defs);
    svgEl("stop", { offset: 0, "stop-color": "#635bff", "stop-opacity": 0.22 }, lg);
    svgEl("stop", { offset: 1, "stop-color": "#635bff", "stop-opacity": 0 }, lg);

    const line = pts.map((p, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p.a).toFixed(1)}`).join("");
    svgEl("path", { d: `${line}L${W},${H}L0,${H}Z`, fill: `url(#${gid})` }, svg);
    if (cut) {
      svgEl("line", { x1: 0, x2: W, y1: y(cut), y2: y(cut), class: "spark-cut",
        "vector-effect": "non-scaling-stroke" }, svg);
    }
    const path = svgEl("path", { d: line, class: "spark-line", "vector-effect": "non-scaling-stroke" }, svg);
    if (opts.animate && !reduced) {
      const len = path.getTotalLength ? path.getTotalLength() : 1000;
      path.style.setProperty("--len", Math.ceil(len * 1.5));
      path.classList.add("draw");
    }

    // dots: HTML overlay so they stay round under preserveAspectRatio="none"
    const overlay = document.createElement("div");
    overlay.style.cssText = "position:absolute;inset:0;pointer-events:none";
    host.style.position = "relative";
    host.appendChild(overlay);
    const dot = (i, cls) => {
      const d = document.createElement("span");
      d.className = cls;
      d.style.cssText = `position:absolute;left:${(x(i) / W) * 100}%;top:${y(pts[i].a)}px;` +
        "width:8px;height:8px;border-radius:50%;transform:translate(-50%,-50%);" +
        "box-shadow:0 0 0 2px #fff;";
      d.style.background = cls === "hot" ? "var(--crit)" : cls === "sim" ? "#fff" : "var(--accent)";
      if (cls === "sim") d.style.boxShadow = "0 0 0 2px var(--crit)";   // hollow: a simulated crossing
      overlay.appendChild(d);
    };
    if (cut) pts.forEach((p, i) => { if (p.a > cut) dot(i, p.sim ? "sim" : "hot"); });
    dot(pts.length - 1, "end");

    if (!opts.interactive) return;
    const cross = document.createElement("span");
    cross.style.cssText = "position:absolute;top:0;bottom:0;width:1px;background:var(--ink-4);opacity:0;transition:opacity .1s";
    overlay.appendChild(cross);
    const tip = tooltip(host);
    host.tabIndex = 0;
    let idx = pts.length - 1;
    const showAt = (i) => {
      idx = Math.max(0, Math.min(pts.length - 1, i));
      const p = pts[idx];
      const px = (x(idx) / W) * host.clientWidth;
      cross.style.left = `${px}px`;
      cross.style.opacity = 1;
      const rows = [[`score ${fmt(p.a)}`], [`${clock(p.t)} · ${p.n || 0} mention${p.n === 1 ? "" : "s"}`, "muted"]];
      if (cut) rows.push([p.a > cut ? `above cut ${fmt(cut)}` : `cut ${fmt(cut)}`, "muted"]);
      if (p.sim) rows.push(["simulated", "muted"]);
      tip.show(px, y(p.a), rows);
    };
    const hide = () => { cross.style.opacity = 0; tip.hide(); };
    host.addEventListener("pointermove", (e) => {
      const r = host.getBoundingClientRect();
      showAt(Math.round(((e.clientX - r.left) / r.width) * (pts.length - 1)));
    });
    host.addEventListener("pointerleave", hide);
    host.addEventListener("focus", () => showAt(idx));
    host.addEventListener("blur", hide);
    host.addEventListener("keydown", (e) => {
      if (e.key === "ArrowLeft") { showAt(idx - 1); e.preventDefault(); }
      if (e.key === "ArrowRight") { showAt(idx + 1); e.preventDefault(); }
    });
  }

  /* ------------------------------------------------------------ histogram
     The featured ticker's trailing distribution. Bars <= 24px, 2px gaps,
     rounded data-ends; the cut is a solid reference line; bins past it are the
     alert zone. Per-bar hover. */
  function histogram(host, dist, animate) {
    host.textContent = "";
    if (!dist || !dist.bins || !dist.bins.length) {
      host.innerHTML = `<p class="note">Baseline still warming up.</p>`;
      return;
    }
    const W = 620, H = 260, L = 34, R = 12, T = 30, B = 30;
    const bins = dist.bins, maxC = Math.max(...bins.map((b) => b.count), 1);
    const xmax = dist.max;
    const x = (v) => L + (v / xmax) * (W - L - R);
    const y = (c) => T + (1 - c / maxC) * (H - T - B);
    const svg = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, role: "img",
      "aria-label": `${dist.name} alert-score distribution: ${dist.samples} windows, cut ${fmt(dist.cut)}` }, host);

    // gridlines + y ticks
    const step = Math.max(1, Math.ceil(maxC / 4));
    for (let c = 0; c <= maxC; c += step) {
      svgEl("line", { x1: L, x2: W - R, y1: y(c), y2: y(c), stroke: "#eef1f5" }, svg);
      const t = svgEl("text", { x: L - 8, y: y(c) + 4, "text-anchor": "end", class: "axis-label" }, svg);
      t.textContent = c;
    }
    // alert zone wash
    if (dist.cut) {
      svgEl("rect", { x: x(dist.cut), y: T - 6, width: Math.max(0, W - R - x(dist.cut)),
        height: H - T - B + 6, fill: "#d03b3b", opacity: 0.05 }, svg);
      const z = svgEl("text", { x: W - R - 4, y: T + 8, "text-anchor": "end", class: "axis-label" }, svg);
      z.textContent = "alert zone";
    }
    const slot = (W - L - R) / bins.length;
    const bw = Math.min(24, slot - 2);
    const tip = tooltip(host);
    bins.forEach((b, i) => {
      if (!b.count) return;
      const bx = L + i * slot + (slot - bw) / 2;
      const top = y(b.count), h = H - B - top, r = Math.min(4, h, bw / 2);
      const d = `M${bx},${H - B}V${top + r}Q${bx},${top} ${bx + r},${top}H${bx + bw - r}Q${bx + bw},${top} ${bx + bw},${top + r}V${H - B}Z`;
      const hot = dist.cut && b.x0 >= dist.cut;
      const bar = svgEl("path", { d, fill: hot ? "#635bff" : "#b9b5ff", class: "hbar" }, svg);
      if (animate && !reduced) {
        bar.style.transformBox = "fill-box";
        bar.style.transformOrigin = "bottom";
        bar.animate([{ transform: "scaleY(0)" }, { transform: "scaleY(1)" }],
          { duration: 700, delay: i * 18, easing: "cubic-bezier(.215,.61,.355,1)", fill: "backwards" });
      }
      // hit target: the whole column, wider than the bar
      const hit = svgEl("rect", { x: L + i * slot, y: T, width: slot, height: H - T - B, fill: "transparent" }, svg);
      hit.addEventListener("pointerenter", () => {
        bar.setAttribute("opacity", 0.75);
        const px = ((bx + bw / 2) / W) * host.clientWidth;
        tip.show(px, (top / H) * host.clientHeight, [[`${b.count} window${b.count === 1 ? "" : "s"}`],
          [`score ${fmt(b.x0)}–${fmt(b.x1)}`, "muted"]]);
      });
      hit.addEventListener("pointerleave", () => { bar.removeAttribute("opacity"); tip.hide(); });
    });
    // x axis
    svgEl("line", { x1: L, x2: W - R, y1: H - B, y2: H - B, stroke: "#d5dbe3" }, svg);
    const xstep = xmax > 1.5 ? 0.5 : 0.25;
    for (let v = 0; v <= xmax + 1e-9; v += xstep) {
      const t = svgEl("text", { x: x(v), y: H - B + 18, "text-anchor": "middle", class: "axis-label" }, svg);
      t.textContent = v.toFixed(v % 1 ? 2 : 1);
    }
    // cut
    if (dist.cut) {
      svgEl("line", { x1: x(dist.cut), x2: x(dist.cut), y1: T - 14, y2: H - B, stroke: "#0a2540", "stroke-width": 1.5 }, svg);
      const lbl = svgEl("text", { x: x(dist.cut) - 6, y: T - 18, "text-anchor": "end",
        style: "font:650 12px var(--sans);fill:#0a2540" }, svg);
      lbl.textContent = `cut ${fmt(dist.cut)}`;
    }
    // latest window
    if (dist.latest !== null && dist.latest !== undefined) {
      const lx = x(Math.min(dist.latest, xmax));
      const g = svgEl("g", {}, svg);
      svgEl("circle", { cx: lx, cy: H - B, r: 6, fill: "#d03b3b", stroke: "#fff", "stroke-width": 2 }, g);
      if (!reduced) {
        const ring = svgEl("circle", { cx: lx, cy: H - B, r: 6, fill: "none", stroke: "#d03b3b" }, g);
        ring.animate([{ r: 6, opacity: 0.6 }, { r: 16, opacity: 0 }], { duration: 1800, iterations: Infinity });
      }
      const t = svgEl("text", { x: lx, y: H - B - 12, "text-anchor": "middle",
        style: "font:650 11px var(--sans);fill:#b42828" }, g);
      t.textContent = `latest ${fmt(dist.latest)}`;
    }
  }

  /* ------------------------------------------------------------ ticker cards */
  function tickerState(r) {
    if (!r.baseline_ready) return ["badge", `Warming up ${r.baseline_samples}/${r.min_samples}`];
    if (r.alert_score === null || r.alert_score === undefined) return ["badge", "No news yet"];
    if (r.alert_score > r.baseline) return ["badge crit", "▲ Above baseline"];
    return ["badge good", "● Normal"];
  }

  function renderTickers(host, rows, animate) {
    host._cards = host._cards || {};
    if (!rows.length) {
      host.innerHTML = `<div class="empty"><div class="glyph">◎</div>No tickers yet. <a href="/settings">Pick some</a>.</div>`;
      host._cards = {};
      return;
    }
    const seen = new Set();
    rows.forEach((r) => {
      seen.add(r.ticker);
      const sig = JSON.stringify([r.alert_score, r.baseline, r.window_end, r.history && r.history.length,
        r.history && r.history.length && r.history[r.history.length - 1].t, r.baseline_samples]);
      let card = host._cards[r.ticker];
      if (card && card.sig === sig) return;
      const [badgeCls, badgeText] = tickerState(r);
      const over = badgeCls.includes("crit");
      const el = card ? card.el : document.createElement("article");
      el.className = `tcard${over ? " over" : ""}`;
      el.innerHTML = `
        <div class="tcard-h">
          <div><div class="sym">${esc(r.ticker)}</div><div class="co">${esc(r.name)}</div></div>
          <div class="score"><b>${fmt(r.alert_score)}</b><small>${r.total_mentions ? `${r.total_mentions} mentions` : "alert score"}</small></div>
        </div>
        <div class="spark"></div>
        <div class="tcard-f">
          <span class="${esc(badgeCls)}">${esc(badgeText)}</span>
          <span>${r.baseline_ready ? `cut ${fmt(r.baseline)}` : ""}${r.window_end ? ` · ${clock(r.window_end)}` : ""}</span>
        </div>
        ${r.top_headline ? `<a class="headline" href="${esc(safeUrl(r.top_url))}" target="_blank"
            rel="noopener noreferrer" title="Most negative headline in the latest window">${esc(r.top_headline)}</a>` : ""}`;
      if (!card) {
        host.querySelectorAll(":scope > p, :scope > .empty").forEach((n) => n.remove());
        host.appendChild(el);
      }
      sparkline($(".spark", el), r.history, { cut: r.baseline_ready ? r.baseline : null,
        interactive: true, animate: animate || !card, label: `${r.ticker} alert score, last ${r.history ? r.history.length : 0} windows` });
      host._cards[r.ticker] = { el, sig };
    });
    Object.keys(host._cards).forEach((t) => {
      if (!seen.has(t)) { host._cards[t].el.remove(); delete host._cards[t]; }
    });
  }

  /* ------------------------------------------------------------ count-up */
  function countUp(el, to) {
    const from = Number(el.dataset.v || 0);
    el.dataset.v = to;
    if (reduced || from === to) { el.textContent = compact(to); return; }
    const t0 = performance.now(), dur = from ? 800 : 1600;
    const tick = (now) => {
      const k = Math.min(1, (now - t0) / dur), e = 1 - Math.pow(1 - k, 3);
      el.textContent = compact(Math.round(from + (to - from) * e));
      if (k < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  }

  /* ------------------------------------------------------------ chrome */
  function initChrome() {
    const nav = $("#nav");
    if (nav) {
      const onScroll = () => nav.classList.toggle("scrolled", window.scrollY > 8);
      onScroll();
      window.addEventListener("scroll", onScroll, { passive: true });
    }
    const io = "IntersectionObserver" in window ? new IntersectionObserver((entries) => {
      entries.forEach((e) => { if (e.isIntersecting) { e.target.classList.add("in"); io.unobserve(e.target); } });
    }, { rootMargin: "0px 0px -8% 0px" }) : null;
    $$(".reveal").forEach((el) => (io ? io.observe(el) : el.classList.add("in")));
    const paintAgo = () => $$("[data-ago]").forEach((el) => { el.textContent = ago(el.dataset.ago); });
    paintAgo();
    setInterval(paintAgo, 30000);
  }

  function poll(fn, ms) {
    let busy = false;
    const tick = async () => {
      if (busy || document.hidden) return;
      busy = true;
      try { await fn(); } catch (_) { /* transient: keep the last render */ }
      busy = false;
    };
    tick();
    return setInterval(tick, ms);
  }

  /* ============================================================ landing */
  function initLanding() {
    initChrome();
    let first = true;
    const statsSeen = { v: false };
    const statsEl = $(".stats");
    let latestTotals = null;
    const paintStats = () => {
      if (!latestTotals || !statsSeen.v) return;
      $$("[data-count]").forEach((el) => countUp(el, Number(latestTotals[el.dataset.count] || 0)));
    };
    if (statsEl && "IntersectionObserver" in window) {
      new IntersectionObserver((es) => {
        if (es.some((e) => e.isIntersecting)) { statsSeen.v = true; paintStats(); }
      }).observe(statsEl);
    } else statsSeen.v = true;

    poll(async () => {
      const data = await api("/api/public/overview");
      const byT = Object.fromEntries(data.tickers.map((t) => [t.ticker, t]));
      // hero preview
      $$("#preview-rows .mini-row").forEach((row) => {
        const t = byT[row.dataset.ticker];
        if (!t) return;
        const [cls] = tickerState(t);
        const state = cls.includes("crit") ? "above cut" : cls.includes("good") ? "normal" : "warming";
        $(".t", row).innerHTML = `${esc(t.ticker)}<small>${esc(t.name)}</small>`;
        $(".v", row).innerHTML = `${fmt(t.alert_score)}<small>${state}</small>`;
        sparkline($(".spark", row), (t.history || []).slice(-30),
          { cut: t.baseline_ready ? t.baseline : null, height: 38, animate: first });
      });
      latestTotals = data.totals;
      paintStats();
      const dist = data.distribution;
      if (dist) {
        $("#dist-title").textContent = `${dist.name}'s baseline, live`;
        $("#dist-sub").textContent = `${dist.samples.toLocaleString()} windows in the trailing 24 hours · median ${fmt(dist.median)}`;
        $("#dist-badge").textContent = `p${data.percentile}`;
      }
      const distHost = $("#dist-chart");
      if (distHost && (first || !distHost._sig || distHost._sig !== JSON.stringify(dist))) {
        histogram(distHost, dist, first);
        distHost._sig = JSON.stringify(dist);
      }
      renderTickers($("#landing-tickers"), data.tickers, first);
      first = false;
    }, 15000);
  }

  /* ============================================================ dashboard */
  function initDashboard() {
    initChrome();
    let lastUpdate = Date.now();
    const alertIds = new Set();
    let alertsPrimed = false;
    const headlineIds = new Set();
    let headlinesPrimed = false;
    let riskRows = [];

    setInterval(() => {
      const s = Math.round((Date.now() - lastUpdate) / 1000);
      const el = $("#updated");
      if (el) el.textContent = s < 3 ? "Live · just updated" : `Live · updated ${s}s ago`;
    }, 1000);

    const renderStatus = (data) => {
      const order = [["kafka", "Bus"], ["flink", "Stream"], ["enrichment", "Model"], ["redis", "Redis"]];
      $("#status").innerHTML = order.map(([key, label]) => {
        const s = data[key] || { ok: false, detail: "unknown" };
        return `<span class="status-pill${s.ok ? "" : " bad"}" title="${esc(s.detail)}">
          <span class="dot ${s.ok ? "good" : "crit"}" aria-hidden="true"></span>
          <b>${esc(s.label || label)}</b>${esc(s.ok ? s.detail : "down")}<span class="sr-only">${s.ok ? "healthy" : "unhealthy"}</span></span>`;
      }).join("");
    };

    const renderKpis = (rows) => {
      const live = rows.filter((r) => r.alert_score !== null && r.alert_score !== undefined);
      const top = live.sort((a, b) => (b.alert_score - (b.baseline || 0)) - (a.alert_score - (a.baseline || 0)))[0];
      $("#kpi-top").textContent = top ? top.ticker : "—";
      $("#kpi-top-d").textContent = top ? `score ${fmt(top.alert_score)}${top.baseline ? ` vs cut ${fmt(top.baseline)}` : ""}` : "no windows yet";
      $("#kpi-over").textContent = rows.filter((r) => r.baseline_ready && r.alert_score > r.baseline).length;
    };

    const renderAlerts = (rows) => {
      const list = $("#alert-list");
      $("#kpi-alerts").textContent = rows.length >= 25 ? "25+" : rows.length;
      $("#kpi-alerts-d").textContent = rows.length ? `latest ${ago(rows[0].fired_at)}` : "none yet on your tickers";
      if (!rows.length) {
        list.innerHTML = `<li class="empty" style="padding-left:0"><div class="glyph">◎</div>
          No alerts yet. They fire when a ticker leaves its own normal range, or try the simulator.</li>`;
        return;
      }
      const slack = { sent: "Slack: delivered", failed: "Slack: failed", no_webhook: "Slack: not connected",
        unverified: "Slack: not verified", simulated: "Slack: not sent (simulated)" };
      list.innerHTML = rows.slice(0, 12).map((a) => {
        const sim = a.source === "simulated";
        const fresh = alertsPrimed && !alertIds.has(a.alert_id);
        return `<li class="${fresh ? "enter" : ""}">
          <span class="node ${esc(a.severity)}${sim ? " sim" : ""}" aria-hidden="true"></span>
          <div class="row1"><span class="sym">${esc(a.ticker)}</span>
            ${sim ? `<span class="badge sim">simulated</span>` : `<span class="badge crit"><span class="ico">▲</span>${esc(a.severity)}</span>`}
            <span class="when" title="${esc(a.fired_at)}">${ago(a.fired_at)}</span></div>
          <div class="msg">score ${fmt(Number(a.alert_score) || a.risk_score)} vs baseline ${fmt(a.baseline_p)}</div>
          <div class="foot">${a.top_headline ? `<span>${esc(a.top_headline.slice(0, 120))}</span>` : ""}
            <span>${esc(slack[a.delivery_status] || "")}</span></div>
        </li>`;
      }).join("");
      rows.forEach((a) => alertIds.add(a.alert_id));
      alertsPrimed = true;
    };

    const renderHeadlines = (items) => {
      const list = $("#headline-list");
      if (!items.length) { list.innerHTML = `<li class="note">Waiting for the first articles…</li>`; return; }
      list.innerHTML = items.map((h) => {
        const fresh = headlinesPrimed && !headlineIds.has(h.article_id);
        return `<li class="${fresh ? "enter" : ""}">${chip(h.sentiment)}
          <div style="min-width:0">
            <a class="title" href="${esc(safeUrl(h.url))}" target="_blank" rel="noopener noreferrer">${esc(h.title)}</a>
            <div class="meta">${esc(h.source || "")} · ${ago(h.published_at)}
              ${(h.companies || []).map((c) => `<span class="tk">${esc(c.ticker)}</span>`).join("")}</div>
          </div></li>`;
      }).join("");
      items.forEach((h) => headlineIds.add(h.article_id));
      headlinesPrimed = true;
    };

    let firstRisk = true;
    const refreshRisk = async () => {
      const data = await api("/api/risk");
      riskRows = data.risk || [];
      $$(".pct").forEach((el) => { el.textContent = data.percentile; });
      renderTickers($("#risk"), riskRows, firstRisk);
      renderKpis([...riskRows]);
      firstRisk = false;
      lastUpdate = Date.now();
    };
    const refreshAlerts = async () => renderAlerts((await api("/api/alerts")).alerts || []);

    poll(async () => renderStatus(await api("/api/status")), 6000);
    poll(refreshRisk, 5000);
    poll(refreshAlerts, 5000);
    poll(async () => renderHeadlines((await api("/api/headlines?limit=25&relevant=true")).headlines || []), 7000);

    initSimulator({ getRisk: () => riskRows, refreshRisk, refreshAlerts, alertIds });
  }

  /* ------------------------------------------------------------ simulator */
  function initSimulator(ctx) {
    const btn = $("#simulate");
    if (!btn) return;
    const msg = $("#sim-msg");
    const steps = Object.fromEntries($$("#sim-steps li").map((li) => [li.dataset.step, li]));
    const set = (name, state) => {
      const li = steps[name];
      li.classList.remove("active", "done", "fail");
      if (state) li.classList.add(state);
      $(".s-ic", li).textContent = state === "done" ? "✓" : state === "fail" ? "!" : String(Object.keys(steps).indexOf(name) + 1);
    };
    const reset = () => Object.keys(steps).forEach((s) => set(s, null));
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

    btn.addEventListener("click", async () => {
      const ticker = $("#sim-ticker").value;
      reset();
      btn.disabled = true;
      msg.textContent = "";
      const before = (ctx.getRisk().find((r) => r.ticker === ticker) || {}).window_end;
      const knownAlerts = new Set(ctx.alertIds);
      const t0 = performance.now();
      set("publish", "active");
      try {
        await api("/api/simulate", { method: "POST", body: JSON.stringify({ ticker }) });
      } catch (e) {
        set("publish", "fail");
        msg.textContent = e.message;
        btn.disabled = false;
        return;
      }
      set("publish", "done");
      set("score", "active");
      await sleep(900);
      set("score", "done");
      set("window", "active");

      let windowDone = false;
      for (let i = 0; i < 40; i++) {
        await sleep(1500);
        try {
          await ctx.refreshRisk();
          const row = ctx.getRisk().find((r) => r.ticker === ticker) || {};
          if (!windowDone && row.simulated && row.window_end !== before) {
            windowDone = true;
            set("window", "done");
            set("alert", "active");
          }
          const alerts = (await api("/api/alerts")).alerts || [];
          const fired = alerts.find((a) => a.ticker === ticker && a.source === "simulated" && !knownAlerts.has(a.alert_id));
          if (fired) {
            if (!windowDone) set("window", "done");
            set("alert", "done");
            msg.textContent = `Alert fired ${((performance.now() - t0) / 1000).toFixed(1)}s after publishing: ` +
              `${fired.ticker} crossed its baseline of ${fmt(fired.baseline_p)}.`;
            await ctx.refreshAlerts();
            btn.disabled = false;
            return;
          }
        } catch (_) { /* keep waiting */ }
      }
      if (!windowDone) {
        set("window", "fail");
        msg.textContent = "The window hasn't closed yet. Windows close on event time, so it closes once newer headlines arrive (usually within two minutes).";
      } else {
        set("alert", "fail");
        msg.textContent = "The window closed but stayed under this ticker's cut, or the ticker is in its 10-minute cooldown.";
      }
      btn.disabled = false;
    });
  }

  /* ============================================================ onboarding / settings */
  function initOnboarding() {
    initChrome();
    const saveBtn = $("#save-watchlist");
    if (saveBtn) saveBtn.addEventListener("click", async () => {
      const tickers = $$("#picker input:checked").map((i) => i.value);
      const out = $("#watchlist-msg");
      if (!tickers.length) { out.textContent = "Pick at least one ticker."; return; }
      saveBtn.disabled = true;
      try {
        const res = await api("/api/watchlist", { method: "POST", body: JSON.stringify({ tickers }) });
        out.textContent = `Saved. Watching ${res.watching.join(", ")}.`;
        const next = saveBtn.dataset.next;
        if (next) setTimeout(() => { window.location.href = next; }, 500);
      } catch (e) { out.textContent = e.message; }
      saveBtn.disabled = false;
    });

    if (!$("#save-slack")) return;                  // guests have no Slack card
    const saveSlack = () => api("/api/slack", {
      method: "POST", body: JSON.stringify({ webhook_url: $("#slack-url").value }),
    });
    $("#save-slack").addEventListener("click", async () => {
      const out = $("#slack-msg");
      try { const r = await saveSlack(); out.textContent = r.configured ? "Saved. Send a test to verify it." : "Cleared."; }
      catch (e) { out.textContent = e.message; }
    });
    $("#test-slack").addEventListener("click", async () => {
      const out = $("#slack-msg");
      out.textContent = "Sending…";
      try {
        await saveSlack();
        const res = await api("/api/slack/test", { method: "POST" });
        out.textContent = res.sent ? "Delivered and verified. Alerts will reach this channel."
          : "Slack rejected the message. Double-check the webhook URL.";
      } catch (e) { out.textContent = e.message; }
    });
  }

  const exported = { initLanding, initDashboard, initOnboarding };
  // Pages pick their entry point with <script data-init="...">: the CSP allows
  // no inline script, so there is nowhere else to call it from.
  const init = document.currentScript && document.currentScript.dataset.init;
  if (init === "landing") initLanding();
  else if (init === "dashboard") initDashboard();
  else if (init === "onboarding") initOnboarding();
  else if (init === "chrome") initChrome();
  return exported;
})();
