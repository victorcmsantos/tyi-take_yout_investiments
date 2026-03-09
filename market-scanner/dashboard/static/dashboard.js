const grid = document.getElementById("signal-grid");
const matrixShell = document.getElementById("matrix-shell");
const signalCount = document.getElementById("signal-count");
const lastUpdate = document.getElementById("last-update");
const tickerFilter = document.getElementById("ticker-filter");
const rrFilter = document.getElementById("rr-filter");
const sortFilter = document.getElementById("sort-filter");
const setupFilter = document.getElementById("setup-filter");
const highlightScore = Number(document.querySelector(".page-shell")?.dataset.highlightScore || 75);
const initialSignalsNode = document.getElementById("initial-signals");
const initialTrackedTickersNode = document.getElementById("initial-tracked-tickers");
const tradeModal = document.getElementById("trade-modal");
const tradeModalForm = document.getElementById("trade-modal-form");
const tradeModalTicker = document.getElementById("trade-modal-ticker");
const tradeModalQuantity = document.getElementById("trade-modal-quantity");
const tradeModalPrice = document.getElementById("trade-modal-price");
const tradeModalInvested = document.getElementById("trade-modal-invested");
const tradeModalNotes = document.getElementById("trade-modal-notes");
let allSignals = initialSignalsNode ? JSON.parse(initialSignalsNode.textContent) : [];
let trackedTickers = new Set(
  initialTrackedTickersNode ? JSON.parse(initialTrackedTickersNode.textContent) : [],
);
let pendingTradeButton = null;

function parseServerDate(value) {
  if (!value) {
    return null;
  }
  const source = String(value).trim();
  if (!source) {
    return null;
  }
  // Normalize server timestamps:
  // - support "YYYY-MM-DD HH:mm:ss"
  // - trim microseconds to milliseconds for browser Date parser compatibility
  // - assume UTC when timezone is omitted
  let normalized = source.replace(" ", "T");
  normalized = normalized.replace(/(\.\d{3})\d+/, "$1");
  const hasTimezone = /(?:Z|[+-]\d{2}:\d{2})$/i.test(normalized);
  if (!hasTimezone) {
    normalized = `${normalized}Z`;
  }
  const parsed = new Date(normalized);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed;
  }
  const fallback = new Date(source.replace(" ", "T"));
  return Number.isNaN(fallback.getTime()) ? null : fallback;
}

function formatDateTime(value, fallback = "N/A") {
  const parsed = parseServerDate(value);
  if (!parsed) {
    return fallback;
  }
  return parsed.toLocaleString("pt-BR");
}

if (lastUpdate) {
  const rawValue = (lastUpdate.textContent || "").trim();
  if (rawValue) {
    lastUpdate.textContent = formatDateTime(rawValue, rawValue);
  }
}

function formatDecimal(value, digits = 2) {
  return Number(value).toFixed(digits);
}

function getRiskRewardRatio(signal) {
  return signal.trade_levels?.risk_reward_ratio == null
    ? null
    : Number(signal.trade_levels.risk_reward_ratio);
}

function getPotentialGainPct(signal) {
  return signal.trade_levels?.potential_gain_pct == null
    ? null
    : Number(signal.trade_levels.potential_gain_pct);
}

function getRiskRewardTier(signal) {
  const ratio = getRiskRewardRatio(signal);
  if (ratio == null) {
    return 0;
  }
  if (ratio > 4.0) {
    return 4;
  }
  if (ratio > 3.0) {
    return 3;
  }
  if (ratio > 2.0) {
    return 2;
  }
  return 0;
}

function getRiskRewardBadge(tier) {
  if (!tier) {
    return "";
  }
  return `<span class="signal-badge signal-badge--rr signal-badge--rr${tier}">RR &gt; ${tier}.0</span>`;
}

function hasMetricKeyword(signal, keyword) {
  const normalizedKeyword = String(keyword || "").toLowerCase();
  return (signal.metrics_triggered || []).some((metric) =>
    String(metric || "").toLowerCase().includes(normalizedKeyword),
  );
}

function matchesSetup(signal, setupKey) {
  if (!setupKey || setupKey === "all") {
    return true;
  }

  const hasBreakout = hasMetricKeyword(signal, "breakout");
  const hasVolume = hasMetricKeyword(signal, "volume");
  const hasTrend = hasMetricKeyword(signal, "trend");
  const hasMomentum = hasMetricKeyword(signal, "momentum");
  const hasRelativeStrength = hasMetricKeyword(signal, "relative strength")
    || hasMetricKeyword(signal, "ibov");
  const hasVolCompression = hasMetricKeyword(signal, "volatility compression");

  if (setupKey === "setup_1") {
    return hasBreakout && hasVolume && hasTrend;
  }
  if (setupKey === "setup_2") {
    return hasMomentum && hasRelativeStrength && hasVolume;
  }
  if (setupKey === "setup_3") {
    return hasVolCompression && hasBreakout;
  }
  return true;
}

function renderSignalTile(signal) {
  const metrics = signal.metrics_triggered.map((metric) => `<li>${metric}</li>`).join("");
  const tier = getRiskRewardTier(signal);
  const isTracked = trackedTickers.has(signal.ticker);
  const entryLow = Number(signal.trade_levels.entry_region.low).toFixed(2);
  const entryHigh = Number(signal.trade_levels.entry_region.high).toFixed(2);
  const objective = Number(signal.trade_levels.objective_price).toFixed(2);
  const stop = Number(signal.trade_levels.stop_price).toFixed(2);
  const gainPct = Number(signal.trade_levels.potential_gain_pct).toFixed(2);
  const riskPct = Number(signal.trade_levels.risk_pct).toFixed(2);
  const ratio = signal.trade_levels.risk_reward_ratio == null
    ? "N/A"
    : `1:${Number(signal.trade_levels.risk_reward_ratio).toFixed(2)}`;
  const tileClasses = [
    "signal-tile",
    signal.score >= highlightScore ? "signal-tile--highlight" : "",
    tier ? `signal-tile--rr-tier-${tier}` : "",
  ].filter(Boolean).join(" ");
  return `
    <article class="${tileClasses}">
      <header>
        <span class="signal-ticker">${signal.ticker}</span>
        <span class="signal-badge-group">
          <span class="signal-badge">BUY</span>
          ${getRiskRewardBadge(tier)}
        </span>
      </header>
      <div class="signal-value">
        <span class="label">Price</span>
        <strong>${Number(signal.price).toFixed(2)}</strong>
      </div>
      <div class="signal-value">
        <span class="label">Score</span>
        <strong>${Number(signal.score).toFixed(2)}</strong>
      </div>
      <div class="trade-plan">
        <div class="trade-plan-row">
          <span class="label">Região de Entrada</span>
          <strong>${entryLow} - ${entryHigh}</strong>
        </div>
        <div class="trade-plan-row trade-plan-row--up">
          <span class="label"><span class="trade-arrow trade-arrow--up">↑</span> Objetivo Sugerido</span>
          <strong>${objective} <span class="trade-pct">(+${gainPct}%)</span></strong>
        </div>
        <div class="trade-plan-row trade-plan-row--down">
          <span class="label"><span class="trade-arrow trade-arrow--down">↓</span> Stop Sugerido</span>
          <strong>${stop} <span class="trade-pct">(-${riskPct}%)</span></strong>
        </div>
        <div class="trade-plan-row trade-plan-row--ratio">
          <span class="label">Risco/Retorno</span>
          <strong>${ratio}</strong>
        </div>
      </div>
      <div class="tile-actions">
        <button
          class="trade-select-button ${isTracked ? "trade-select-button--active" : ""}"
          type="button"
          data-action="track-trade"
          data-ticker="${signal.ticker}"
          ${isTracked ? "disabled" : ""}
        >
          ${isTracked ? "Acompanhando" : "Comprei"}
        </button>
      </div>
      <div class="metrics-block">
        <span class="label">Metrics triggered</span>
        <ul>${metrics}</ul>
      </div>
    </article>
  `;
}

function applySignalFilters(signals) {
  const query = (tickerFilter?.value || "").trim().toUpperCase();
  const minRr = Number(rrFilter?.value || "all");
  const selectedSetup = setupFilter?.value || "all";
  return signals.filter((signal) => {
    const matchesTicker = !query || signal.ticker.toUpperCase().includes(query);
    const ratio = getRiskRewardRatio(signal);
    const matchesRr = Number.isNaN(minRr) || !minRr || (ratio != null && ratio > minRr);
    const setupMatch = matchesSetup(signal, selectedSetup);
    return matchesTicker && matchesRr && setupMatch;
  });
}

function sortSignals(signals) {
  const sortKey = sortFilter?.value || "count_desc";
  const sorted = [...signals];
  sorted.sort((left, right) => {
    if (sortKey === "score_desc") {
      return Number(right.score) - Number(left.score) || left.ticker.localeCompare(right.ticker);
    }
    if (sortKey === "gain_pct_desc") {
      return (getPotentialGainPct(right) || -1) - (getPotentialGainPct(left) || -1)
        || Number(right.score) - Number(left.score)
        || left.ticker.localeCompare(right.ticker);
    }
    if (sortKey === "rr_desc") {
      return (getRiskRewardRatio(right) || -1) - (getRiskRewardRatio(left) || -1)
        || Number(right.score) - Number(left.score)
        || left.ticker.localeCompare(right.ticker);
    }
    if (sortKey === "ticker_asc") {
      return left.ticker.localeCompare(right.ticker);
    }
    return right.metrics_triggered.length - left.metrics_triggered.length
      || Number(right.score) - Number(left.score)
      || ((getRiskRewardRatio(right) || -1) - (getRiskRewardRatio(left) || -1))
      || left.ticker.localeCompare(right.ticker);
  });
  return sorted;
}

function buildMatrixFromSignals(signals) {
  const metricFrequency = {};
  signals.forEach((signal) => {
    signal.metrics_triggered.forEach((metric) => {
      metricFrequency[metric] = (metricFrequency[metric] || 0) + 1;
    });
  });
  const columns = Object.keys(metricFrequency).sort((left, right) => {
    return metricFrequency[right] - metricFrequency[left] || left.localeCompare(right);
  });
  const rows = signals.map((signal) => ({
    ticker: signal.ticker,
    triggered_count: signal.metrics_triggered.length,
    score: signal.score,
    cells: Object.fromEntries(columns.map((column) => [column, signal.metrics_triggered.includes(column)])),
  }));
  return { columns, rows };
}

function renderSignals(signals) {
  signalCount.textContent = String(signals.length);
  if (!signals.length) {
    grid.innerHTML = `
      <div class="empty-state">
        No cards match the current filters. Adjust the ticker or RR filter to broaden the view.
      </div>
    `;
    return;
  }
  lastUpdate.textContent = formatDateTime(signals[0]?.created_at, "No signals yet");
  grid.innerHTML = signals.map(renderSignalTile).join("");
}

function renderMatrix(matrix) {
  if (!matrix.rows.length) {
    matrixShell.innerHTML = `
      <div class="empty-state">
        No matrix rows match the current filters. Adjust the ticker or RR filter to broaden the view.
      </div>
    `;
    return;
  }

  const columns = matrix.columns
    .map((column) => `<th>${column}</th>`)
    .join("");
  const rows = matrix.rows
    .map(
      (row) => `
        <tr>
          <td>${row.ticker}</td>
          <td>${row.triggered_count}</td>
          <td>${Number(row.score).toFixed(2)}</td>
          ${matrix.columns
            .map(
              (column) => `
                <td class="${row.cells[column] ? "matrix-hit" : "matrix-miss"}">
                  ${row.cells[column] ? "●" : "&middot;"}
                </td>
              `,
            )
            .join("")}
        </tr>
      `,
    )
    .join("");

  matrixShell.innerHTML = `
    <table class="signal-matrix" id="signal-matrix">
      <thead>
        <tr>
          <th>Ticker</th>
          <th>Count</th>
          <th>Score</th>
          ${columns}
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

async function refreshSignals() {
  try {
    const [signalsResponse, tradesResponse] = await Promise.all([
      fetch("/signals", { cache: "no-store" }),
      fetch("/trades", { cache: "no-store" }),
    ]);
    if (!signalsResponse.ok) {
      throw new Error(`Signal refresh failed with status ${signalsResponse.status}`);
    }
    if (!tradesResponse.ok) {
      throw new Error(`Trade refresh failed with status ${tradesResponse.status}`);
    }
    allSignals = await signalsResponse.json();
    const tradesPayload = await tradesResponse.json();
    trackedTickers = new Set(tradesPayload.tracked_tickers || []);
    renderDashboard();
  } catch (error) {
    console.error(error);
  }
}

function renderDashboard() {
  const filteredSignals = sortSignals(applySignalFilters(allSignals));
  renderSignals(filteredSignals);
  renderMatrix(buildMatrixFromSignals(filteredSignals));
}

function handleFilterChange() {
  renderDashboard();
}

async function handleTradeTracking(button) {
  const ticker = button.dataset.ticker;
  if (!ticker || trackedTickers.has(ticker)) {
    return;
  }
  const signal = allSignals.find((item) => item.ticker === ticker);
  if (!signal || !tradeModal || !tradeModalForm || !tradeModalTicker || !tradeModalQuantity || !tradeModalPrice || !tradeModalInvested) {
    return;
  }
  pendingTradeButton = button;
  tradeModalForm.dataset.ticker = ticker;
  tradeModalForm.dataset.pricingMode = "price";
  tradeModalTicker.value = ticker;
  tradeModalQuantity.value = "1";
  tradeModalPrice.value = formatDecimal(signal.price, 4);
  tradeModalInvested.value = formatDecimal(signal.price, 2);
  if (tradeModalNotes) {
    tradeModalNotes.value = "";
  }
  tradeModal.hidden = false;
  document.body.classList.add("modal-open");
  tradeModalQuantity.focus();
  tradeModalQuantity.select();
}

function closeTradeModal() {
  if (!tradeModal || !tradeModalForm) {
    return;
  }
  tradeModal.hidden = true;
  document.body.classList.remove("modal-open");
  tradeModalForm.reset();
  delete tradeModalForm.dataset.ticker;
  tradeModalForm.dataset.pricingMode = "price";
  pendingTradeButton = null;
}

function syncTradeModalValues(changedField) {
  if (!tradeModalForm || !tradeModalQuantity || !tradeModalPrice || !tradeModalInvested) {
    return;
  }
  const quantity = Number(tradeModalQuantity.value);
  const price = Number(tradeModalPrice.value);
  const investedAmount = Number(tradeModalInvested.value);
  const pricingMode = tradeModalForm.dataset.pricingMode || "price";

  if (changedField === "invested_amount") {
    if (Number.isFinite(quantity) && quantity > 0 && Number.isFinite(investedAmount) && investedAmount > 0) {
      tradeModalPrice.value = formatDecimal(investedAmount / quantity, 4);
    }
    tradeModalForm.dataset.pricingMode = "invested_amount";
    return;
  }

  if (changedField === "execution_price") {
    if (Number.isFinite(quantity) && quantity > 0 && Number.isFinite(price) && price > 0) {
      tradeModalInvested.value = formatDecimal(quantity * price, 2);
    }
    tradeModalForm.dataset.pricingMode = "price";
    return;
  }

  if (changedField === "quantity" && Number.isFinite(quantity) && quantity > 0) {
    if (pricingMode === "invested_amount" && Number.isFinite(investedAmount) && investedAmount > 0) {
      tradeModalPrice.value = formatDecimal(investedAmount / quantity, 4);
      return;
    }
    if (Number.isFinite(price) && price > 0) {
      tradeModalInvested.value = formatDecimal(quantity * price, 2);
    }
  }
}

async function submitTradeModal() {
  if (!tradeModalForm || !tradeModalTicker || !tradeModalQuantity || !tradeModalInvested || !pendingTradeButton) {
    return;
  }
  const ticker = tradeModalTicker.value;
  const quantity = Number(tradeModalQuantity.value);
  const investedAmount = Number(tradeModalInvested.value);
  const notes = tradeModalNotes ? tradeModalNotes.value.trim() : "";
  if (!ticker || !Number.isFinite(quantity) || quantity <= 0) {
    window.alert("Quantidade invalida.");
    return;
  }
  if (!Number.isFinite(investedAmount) || investedAmount <= 0) {
    window.alert("Valor investido invalido.");
    return;
  }
  pendingTradeButton.disabled = true;
  try {
    const response = await fetch("/trades", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ticker, quantity, invested_amount: investedAmount, notes }),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || `Trade create failed with status ${response.status}`);
    }
    trackedTickers.add(ticker);
    closeTradeModal();
    renderDashboard();
  } catch (error) {
    pendingTradeButton.disabled = false;
    console.error(error);
    window.alert(`Nao foi possivel acompanhar ${ticker}. ${error.message}`);
  }
}

tickerFilter?.addEventListener("input", handleFilterChange);
rrFilter?.addEventListener("change", handleFilterChange);
sortFilter?.addEventListener("change", handleFilterChange);
setupFilter?.addEventListener("change", handleFilterChange);
tradeModalForm?.addEventListener("input", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLInputElement)) {
    return;
  }
  if (!["quantity", "execution_price", "invested_amount"].includes(target.name)) {
    return;
  }
  syncTradeModalValues(target.name);
});
tradeModalForm?.addEventListener("submit", (event) => {
  event.preventDefault();
  submitTradeModal();
});
tradeModal?.addEventListener("click", (event) => {
  const actionTarget = event.target.closest("[data-action='close-trade-modal']");
  if (!actionTarget) {
    return;
  }
  closeTradeModal();
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && tradeModal && !tradeModal.hidden) {
    closeTradeModal();
  }
});
grid?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-action='track-trade']");
  if (!button) {
    return;
  }
  handleTradeTracking(button);
});
renderDashboard();
setInterval(refreshSignals, 60000);
