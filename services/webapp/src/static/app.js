/* RiskRadar dashboard.
   No framework, no build step, 3s poll. Refetch holds the previous render at
   reduced opacity rather than flashing a skeleton. */
const RiskRadar = (() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  async function api(path, opts) {
    const res = await fetch(path, Object.assign({ headers: { "Content-Type": "application/json" } }, opts));
    if (!res.ok) {
      let detail = res.statusText;
      try { detail = (await res.json()).detail || detail; } catch (_) {}
      throw new Error(detail);
    }
    return res.json();
  }

  const pct = (v) => `${Math.round(Math.max(0, Math.min(1, v)) * 100)}%`;

  function ago(iso) {
    if (!iso) return "—";
    const secs = (Date.now() - new Date(iso).getTime()) / 1000;
    if (secs < 60) return "just now";
    if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
    if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
    return `${Math.floor(secs / 86400)}d ago`;
  }

  // Sentiment is polarity -> diverging. The value is always rendered next to
  // the swatch so the encoding is never colour-only.
  function chip(sentiment) {
    const v = Number(sentiment ?? 0);
    const cls = v <= -0.05 ? "neg" : v >= 0.05 ? "pos" : "neu";
    const sign = v > 0 ? "+" : "";
    return `<span class="chip ${cls}" title="sentiment ${v.toFixed(2)}">
      <i></i>${sign}${v.toFixed(2)}</span>`;
  }

  /* ---- status pills: icon + label, never colour alone ---- */
  function renderStatus(data) {
    const order = [["kafka", "Kafka"], ["flink", "Flink"], ["enrichment", "Enrichment"], ["redis", "Redis"]];
    $("#status").innerHTML = order.map(([key, label]) => {
      const s = data[key] || { ok: false, detail: "unknown" };
      const cls = s.ok ? "ok" : "bad";
      const ico = s.ok ? "✓" : "✕";
      return `<span class="pill ${cls}" title="${esc(s.detail)}">
        <span class="dot"></span><span class="ico">${ico}</span>
        <b>${label}</b> ${esc(s.detail)}</span>`;
    }).join("");
    $("#updated").textContent = `updated ${new Date().toLocaleTimeString()}`;
  }

  /* ---- risk meters ---- */
  function renderRisk(payload) {
    const rows = payload.risk || [];
    $("#pct").textContent = payload.percentile ?? 99;

    if (!rows.length) {
      $("#risk").innerHTML = `<p class="note">No tickers watched yet.
        <a href="/settings">Pick some</a>.</p>`;
      return;
    }

    $("#risk").innerHTML = rows.map((r) => {
      const risk = r.risk_score;
      const hasRisk = risk !== null && risk !== undefined;
      const cut = r.baseline;
      const over = hasRisk && cut !== null && cut !== undefined && risk > cut;

      const cutMark = (cut !== null && cut !== undefined)
        ? `<span class="cut" style="left:calc(${pct(cut)} - 1px)"
             title="alert cut ${cut.toFixed(3)}"></span>` : "";

      const state = !r.baseline_ready
        ? `<span class="warming">baseline warming up
             (${r.baseline_samples}/${r.min_samples} windows)</span>`
        : over
          ? `<span class="over-label">▲ above baseline</span>`
          : `<span>within normal range</span>`;

      const headline = r.top_headline
        ? (r.top_url
            ? `<a class="headline" href="${esc(r.top_url)}" target="_blank" rel="noopener">${esc(r.top_headline)}</a>`
            : `<span class="headline">${esc(r.top_headline)}</span>`)
        : `<span class="headline"></span>`;

      return `<div class="meter-row">
        <div class="meter-head">
          <span class="meter-name">${esc(r.ticker)}<small>${esc(r.name)}</small></span>
          <span class="meter-val">${hasRisk
            ? `<strong>${risk.toFixed(3)}</strong>`
            : `<span class="note">no data yet</span>`}</span>
        </div>
        <div class="track">
          <div class="fill ${over ? "over" : ""}" style="width:${hasRisk ? pct(risk) : 0}"></div>
          ${cutMark}
        </div>
        <div class="meter-foot">
          ${headline}
          <span style="flex:none">${state}</span>
        </div>
      </div>`;
    }).join("");

    // table-view twin
    $("#risk-table").querySelector("tbody").innerHTML = rows.map((r) => `
      <tr>
        <td>${esc(r.ticker)}</td>
        <td class="num">${r.risk_score?.toFixed(3) ?? "—"}</td>
        <td class="num">${r.baseline?.toFixed(3) ?? "—"}</td>
        <td class="num">${r.sentiment_score?.toFixed(2) ?? "—"}</td>
        <td class="num">${r.total_mentions ?? 0}</td>
        <td>${r.baseline_ready ? (r.risk_score > r.baseline ? "above baseline" : "normal")
                               : `warming (${r.baseline_samples}/${r.min_samples})`}</td>
      </tr>`).join("");
  }

  /* ---- headlines ---- */
  function renderHeadlines(payload) {
    const items = payload.headlines || [];
    if (!items.length) {
      $("#headlines").innerHTML = `<li class="note">Waiting for the first articles…</li>`;
      return;
    }
    $("#headlines").innerHTML = items.map((h) => `
      <li>
        ${chip(h.sentiment)}
        <div style="min-width:0">
          <a href="${esc(h.url)}" target="_blank" rel="noopener">${esc(h.title)}</a>
          <div class="meta">
            ${esc(h.source || "")} · ${ago(h.published_at)}
            <span class="tickers">${(h.companies || [])
              .map((c) => `<span class="tk">${esc(c.ticker)}</span>`).join("")}</span>
          </div>
        </div>
      </li>`).join("");
  }

  /* ---- alerts ---- */
  function renderAlerts(payload) {
    const rows = payload.alerts || [];
    const body = $("#alerts").querySelector("tbody");
    if (!rows.length) {
      body.innerHTML = `<tr><td colspan="5" class="note">
        No alerts yet. They fire when a ticker moves outside its own recent range.</td></tr>`;
      return;
    }
    const slack = { sent: "delivered", failed: "failed", no_webhook: "not configured",
                    not_delivered: "—", unknown: "—" };
    body.innerHTML = rows.map((a) => `
      <tr>
        <td title="${esc(a.fired_at)}">${ago(a.fired_at)}</td>
        <td>${esc(a.ticker)}
          ${a.source === "simulated" ? `<span class="tag sim">simulated</span>` : ""}</td>
        <td class="num">${Number(a.risk_score).toFixed(3)}</td>
        <td>${esc(a.severity)}</td>
        <td>${slack[a.delivery_status] || esc(a.delivery_status || "—")}</td>
      </tr>`).join("");
  }

  /* ---- polling ---- */
  function poll(fn, ms) {
    let busy = false;
    const tick = async () => {
      if (busy || document.hidden) return;
      busy = true;
      try { await fn(); } catch (e) { /* transient; keep the last render */ }
      busy = false;
    };
    tick();
    return setInterval(tick, ms);
  }

  function initDashboard() {
    const hold = (sel, fn) => async () => {
      const el = $(sel);
      if (el) el.classList.add("stale");
      try { await fn(); } finally { if (el) el.classList.remove("stale"); }
    };

    poll(hold("#status", async () => renderStatus(await api("/api/status"))), 5000);
    poll(hold("#risk", async () => renderRisk(await api("/api/risk"))), 3000);
    poll(hold("#headlines", async () => renderHeadlines(await api("/api/headlines?limit=25"))), 4000);
    poll(hold("#alerts", async () => renderAlerts(await api("/api/alerts"))), 4000);

    $("#simulate").addEventListener("click", async () => {
      const btn = $("#simulate");
      const out = $("#sim-steps");
      const ticker = $("#sim-ticker").value;
      btn.disabled = true;
      out.innerHTML = "Publishing synthetic articles…";
      try {
        const res = await api("/api/simulate", {
          method: "POST", body: JSON.stringify({ ticker }),
        });
        out.innerHTML = `Injected ${res.crisis_articles} negative +
          ${res.filler_articles} watermark articles for <b>${esc(res.ticker)}</b>.
          Expect an alert in ${res.expected_alert_seconds[0]}–${res.expected_alert_seconds[1]}s.`;
        let left = res.expected_alert_seconds[1];
        const countdown = setInterval(() => {
          left -= 1;
          if (left <= 0) { clearInterval(countdown); btn.disabled = false;
            out.innerHTML += " <b>Check the alerts table.</b>"; return; }
          btn.textContent = `Waiting… ${left}s`;
        }, 1000);
        setTimeout(() => { btn.textContent = "Inject synthetic event"; },
                   res.expected_alert_seconds[1] * 1000);
      } catch (e) {
        out.innerHTML = `<span style="color:var(--critical)">${esc(e.message)}</span>`;
        btn.disabled = false;
      }
    });
  }

  function initOnboarding() {
    $("#save-watchlist").addEventListener("click", async () => {
      const tickers = [...document.querySelectorAll("#picker input:checked")].map((i) => i.value);
      const msg = $("#watchlist-msg");
      if (!tickers.length) { msg.textContent = "Pick at least one ticker."; return; }
      try {
        const res = await api("/api/watchlist", {
          method: "POST", body: JSON.stringify({ tickers }),
        });
        msg.textContent = `Watching ${res.watching.join(", ")}.`;
      } catch (e) { msg.textContent = e.message; }
    });

    const saveSlack = async () => {
      const res = await api("/api/slack", {
        method: "POST", body: JSON.stringify({ webhook_url: $("#slack-url").value }),
      });
      return res;
    };

    $("#save-slack").addEventListener("click", async () => {
      const msg = $("#slack-msg");
      try { const r = await saveSlack();
        msg.textContent = r.configured ? "Saved." : "Cleared."; }
      catch (e) { msg.textContent = e.message; }
    });

    $("#test-slack").addEventListener("click", async () => {
      const msg = $("#slack-msg");
      msg.textContent = "Sending…";
      try {
        await saveSlack();
        const res = await api("/api/slack/test", { method: "POST" });
        msg.textContent = res.sent
          ? "Sent — check your Slack channel."
          : "Slack rejected the message. Double-check the webhook URL.";
      } catch (e) { msg.textContent = e.message; }
    });
  }

  return { initDashboard, initOnboarding };
})();
