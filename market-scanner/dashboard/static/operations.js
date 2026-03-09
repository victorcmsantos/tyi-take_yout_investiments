const initialTradeDataNode = document.getElementById("initial-trade-data");
const operationsGrid = document.getElementById("operations-grid");
const historyShell = document.getElementById("history-shell");
const operationStats = document.getElementById("operation-stats");

let tradeData = initialTradeDataNode ? JSON.parse(initialTradeDataNode.textContent) : {
  active: [],
  history: [],
  summary: {},
};

function formatNumber(value) {
  return Number(value).toFixed(2);
}

function formatCurrency(value) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
  }).format(Number(value || 0));
}

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

function formatDateTime(value) {
  const parsed = parseServerDate(value);
  if (!parsed) {
    return "N/A";
  }
  return parsed.toLocaleString("pt-BR");
}

function renderStatusBadge(trade) {
  return `<span class="status-badge status-badge--${trade.status_tone}">${trade.status_label}</span>`;
}

function renderOperationStats() {
  const summary = tradeData.summary || {};
  operationStats.innerHTML = `
    <div class="stat-card">
      <span class="stat-label">Em andamento</span>
      <strong>${summary.tracked_count || 0}</strong>
    </div>
    <div class="stat-card">
      <span class="stat-label">Histórico</span>
      <strong>${summary.history_count || 0}</strong>
    </div>
    <div class="stat-card">
      <span class="stat-label">Sucesso</span>
      <strong>${(summary.success || 0) + (summary.closed_profit || 0)}</strong>
    </div>
    <div class="stat-card">
      <span class="stat-label">Fracasso</span>
      <strong>${(summary.failure || 0) + (summary.closed_loss || 0)}</strong>
    </div>
    <div class="stat-card">
      <span class="stat-label">Capital aberto</span>
      <strong>${formatCurrency(summary.open_invested_amount || 0)}</strong>
    </div>
    <div class="stat-card">
      <span class="stat-label">PnL aberto</span>
      <strong class="${(summary.open_pnl_amount || 0) >= 0 ? "pnl-positive" : "pnl-negative"}">
        ${formatCurrency(summary.open_pnl_amount || 0)}
      </strong>
    </div>
  `;
}

function renderActiveTrades() {
  const activeTrades = tradeData.active || [];
  if (!activeTrades.length) {
    operationsGrid.innerHTML = `
      <div class="empty-state">
        Nenhuma operação aberta no momento. Marque um card como comprado no dashboard para começar a acompanhar.
      </div>
    `;
    return;
  }

  operationsGrid.innerHTML = activeTrades.map((trade) => `
    <article class="operation-card operation-card--${trade.status_tone}">
      <header class="operation-card__header">
        <div>
          <span class="signal-ticker">${trade.ticker}</span>
          ${renderStatusBadge(trade)}
        </div>
        <button
          class="trade-close-button"
          type="button"
          data-action="close-trade"
          data-trade-id="${trade.id}"
        >
          Encerrar agora
        </button>
      </header>
      <div class="operation-grid">
        <div class="signal-value">
          <span class="label">Entrada</span>
          <strong>${formatNumber(trade.entry_price)}</strong>
        </div>
        <div class="signal-value">
          <span class="label">Quantidade</span>
          <strong>${trade.quantity}</strong>
        </div>
        <div class="signal-value">
          <span class="label">Investido</span>
          <strong>${formatCurrency(trade.invested_amount)}</strong>
        </div>
        <div class="signal-value">
          <span class="label">Último preço</span>
          <strong>${trade.last_price == null ? "N/A" : formatNumber(trade.last_price)}</strong>
        </div>
        <div class="signal-value">
          <span class="label">Valor atual</span>
          <strong>${formatCurrency(trade.current_market_value)}</strong>
        </div>
        <div class="signal-value">
          <span class="label">PnL atual %</span>
          <strong class="${trade.current_pnl_pct >= 0 ? "pnl-positive" : "pnl-negative"}">
            ${trade.current_pnl_pct >= 0 ? "+" : ""}${formatNumber(trade.current_pnl_pct)}%
          </strong>
        </div>
        <div class="signal-value">
          <span class="label">PnL atual R$</span>
          <strong class="${trade.current_pnl_amount >= 0 ? "pnl-positive" : "pnl-negative"}">
            ${formatCurrency(trade.current_pnl_amount)}
          </strong>
        </div>
        <div class="signal-value">
          <span class="label">Score</span>
          <strong>${formatNumber(trade.score)}</strong>
        </div>
      </div>
      <div class="trade-plan">
        <div class="trade-plan-row">
          <span class="label">Região de Entrada</span>
          <strong>${formatNumber(trade.entry_region.low)} - ${formatNumber(trade.entry_region.high)}</strong>
        </div>
        <div class="trade-plan-row trade-plan-row--up">
          <span class="label"><span class="trade-arrow trade-arrow--up">↑</span> Objetivo</span>
          <strong>${formatNumber(trade.objective_price)}</strong>
        </div>
        <div class="trade-plan-row trade-plan-row--down">
          <span class="label"><span class="trade-arrow trade-arrow--down">↓</span> Stop</span>
          <strong>${formatNumber(trade.stop_price)}</strong>
        </div>
        <div class="trade-plan-row trade-plan-row--ratio">
          <span class="label">Risco/Retorno</span>
          <strong>${trade.risk_reward_ratio == null ? "N/A" : `1:${formatNumber(trade.risk_reward_ratio)}`}</strong>
        </div>
      </div>
      <div class="meta-strip">
        <span>Aberta em ${formatDateTime(trade.opened_at)}</span>
        <span>Última checagem ${formatDateTime(trade.last_checked_at)}</span>
      </div>
      <form class="trade-edit-form" data-trade-id="${trade.id}">
        <div class="trade-edit-grid">
          <label class="trade-edit-field">
            <span class="label">Quantidade</span>
            <input class="filter-input" name="quantity" type="number" min="0.000001" step="0.000001" value="${trade.quantity}" />
          </label>
          <label class="trade-edit-field">
            <span class="label">Valor investido</span>
            <input class="filter-input" name="invested_amount" type="number" min="0.01" step="0.01" value="${trade.invested_amount}" />
          </label>
          <label class="trade-edit-field">
            <span class="label">Objetivo</span>
            <input class="filter-input" name="objective_price" type="number" min="0.0001" step="0.0001" value="${trade.objective_price}" />
          </label>
          <label class="trade-edit-field">
            <span class="label">Stop</span>
            <input class="filter-input" name="stop_price" type="number" min="0.0001" step="0.0001" value="${trade.stop_price}" />
          </label>
        </div>
        <label class="trade-edit-field">
          <span class="label">Observações</span>
          <textarea class="trade-notes-input" name="notes" rows="2" placeholder="Contexto da entrada, ajuste de risco, parcial...">${trade.notes || ""}</textarea>
        </label>
        <div class="trade-form-actions">
          <button class="trade-save-button" type="submit">Salvar ajuste</button>
        </div>
      </form>
      <div class="metrics-block">
        <span class="label">Sinais ativos na entrada</span>
        <ul>${trade.metrics_triggered.map((metric) => `<li>${metric}</li>`).join("")}</ul>
      </div>
    </article>
  `).join("");
}

function renderHistory() {
  const history = tradeData.history || [];
  if (!history.length) {
    historyShell.innerHTML = `
      <div class="empty-state">
        Ainda não existe histórico encerrado. As tentativas finalizadas aparecem aqui automaticamente.
      </div>
    `;
    return;
  }

  const rows = history.map((trade) => `
    <tr>
      <td>${trade.ticker}</td>
      <td>${renderStatusBadge(trade)}</td>
      <td>${trade.quantity}</td>
      <td>${formatCurrency(trade.invested_amount)}</td>
      <td>${formatNumber(trade.entry_price)}</td>
      <td>${trade.exit_price == null ? "N/A" : formatNumber(trade.exit_price)}</td>
      <td class="${(trade.realized_pnl_pct || 0) >= 0 ? "pnl-positive" : "pnl-negative"}">
        ${(trade.realized_pnl_pct || 0) >= 0 ? "+" : ""}${formatNumber(trade.realized_pnl_pct || 0)}%
      </td>
      <td class="${(trade.realized_pnl_amount || 0) >= 0 ? "pnl-positive" : "pnl-negative"}">
        ${formatCurrency(trade.realized_pnl_amount || 0)}
      </td>
      <td>${trade.exit_reason || "N/A"}</td>
      <td>${formatDateTime(trade.opened_at)}</td>
      <td>${formatDateTime(trade.closed_at)}</td>
    </tr>
  `).join("");

  historyShell.innerHTML = `
    <table class="history-table">
      <thead>
        <tr>
          <th>Ticker</th>
          <th>Status</th>
          <th>Qtd</th>
          <th>Investido</th>
          <th>Entrada</th>
          <th>Saída</th>
          <th>PnL %</th>
          <th>PnL R$</th>
          <th>Motivo</th>
          <th>Abertura</th>
          <th>Fechamento</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderOperationsPage() {
  renderOperationStats();
  renderActiveTrades();
  renderHistory();
}

async function refreshTrades() {
  try {
    const response = await fetch("/trades", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Trade refresh failed with status ${response.status}`);
    }
    tradeData = await response.json();
    renderOperationsPage();
  } catch (error) {
    console.error(error);
  }
}

async function handleCloseTrade(button) {
  const tradeId = button.dataset.tradeId;
  if (!tradeId) {
    return;
  }
  button.disabled = true;
  try {
    const response = await fetch(`/trades/${tradeId}/close`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || `Trade close failed with status ${response.status}`);
    }
    await refreshTrades();
  } catch (error) {
    button.disabled = false;
    console.error(error);
    window.alert(`Nao foi possivel encerrar a operacao. ${error.message}`);
  }
}

async function handleTradeUpdate(form) {
  const tradeId = form.dataset.tradeId;
  if (!tradeId) {
    return;
  }
  const submitButton = form.querySelector(".trade-save-button");
  if (submitButton) {
    submitButton.disabled = true;
  }
  const formData = new FormData(form);
  const payload = {
    quantity: Number(String(formData.get("quantity") || "").replace(",", ".")),
    invested_amount: Number(String(formData.get("invested_amount") || "").replace(",", ".")),
    objective_price: Number(String(formData.get("objective_price") || "").replace(",", ".")),
    stop_price: Number(String(formData.get("stop_price") || "").replace(",", ".")),
    notes: String(formData.get("notes") || ""),
  };
  try {
    const response = await fetch(`/trades/${tradeId}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const errorPayload = await response.json().catch(() => ({}));
      throw new Error(errorPayload.detail || `Trade update failed with status ${response.status}`);
    }
    await refreshTrades();
  } catch (error) {
    if (submitButton) {
      submitButton.disabled = false;
    }
    console.error(error);
    window.alert(`Nao foi possivel atualizar a operacao. ${error.message}`);
  }
}

operationsGrid?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-action='close-trade']");
  if (!button) {
    return;
  }
  handleCloseTrade(button);
});
operationsGrid?.addEventListener("submit", (event) => {
  const form = event.target.closest(".trade-edit-form");
  if (!form) {
    return;
  }
  event.preventDefault();
  handleTradeUpdate(form);
});

renderOperationsPage();
setInterval(refreshTrades, 60000);
