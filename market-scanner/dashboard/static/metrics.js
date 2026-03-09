const menuNode = document.getElementById("metric-menu");
const detailNode = document.getElementById("metric-detail");
const initialCatalogNode = document.getElementById("initial-metric-catalog");
const searchNode = document.getElementById("metric-search");
const countNode = document.getElementById("metric-count");

const METRIC_SECTIONS = [
  {
    key: "trend",
    label: "Tendência",
    metrics: [
      "rsi",
      "momentum",
      "momentum_90",
      "trend_strength",
      "distance_from_sma200",
      "distance_52w_high",
      "relative_strength_vs_ibov",
      "higher_high_score",
      "breakout_20",
    ],
  },
  {
    key: "volatility",
    label: "Volatilidade",
    metrics: ["atr_percent", "volatility_compression", "bollinger_position", "range_expansion"],
  },
  {
    key: "flow",
    label: "Fluxo",
    metrics: ["volume_spike", "vwap_distance"],
  },
];

let metricCatalog = initialCatalogNode ? JSON.parse(initialCatalogNode.textContent) : [];
let activeMetricKey = metricCatalog[0]?.key || null;
let searchQuery = "";
let lastSaveMessage = "";

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function findActiveMetric() {
  const visibleMetrics = getVisibleMetrics();
  if (!activeMetricKey || !visibleMetrics.length) {
    return null;
  }
  const activeMetric = visibleMetrics.find((metric) => metric.key === activeMetricKey) || null;
  if (activeMetric) {
    return activeMetric;
  }
  activeMetricKey = visibleMetrics[0].key;
  return visibleMetrics[0];
}

function getVisibleMetrics() {
  const query = searchQuery.trim().toLowerCase();
  if (!query) {
    return metricCatalog;
  }
  return metricCatalog.filter((metric) => {
    const haystack = [
      metric.label || "",
      metric.key || "",
      metric.details || "",
      metric.formula || "",
    ].join(" ").toLowerCase();
    return haystack.includes(query);
  });
}

function getSectionKey(metricKey) {
  const section = METRIC_SECTIONS.find((entry) => entry.metrics.includes(metricKey));
  return section?.key || "other";
}

function getGroupedMetrics(metrics) {
  const grouped = {};
  METRIC_SECTIONS.forEach((section) => {
    grouped[section.key] = { label: section.label, items: [] };
  });
  grouped.other = { label: "Outras", items: [] };

  metrics.forEach((metric) => {
    grouped[getSectionKey(metric.key)].items.push(metric);
  });

  return [...METRIC_SECTIONS.map((section) => section.key), "other"]
    .map((key) => ({ key, ...grouped[key] }))
    .filter((entry) => entry.items.length > 0);
}

function renderMenu() {
  if (!menuNode) {
    return;
  }
  if (!metricCatalog.length) {
    menuNode.innerHTML = `<div class="empty-state">Nenhuma métrica disponível.</div>`;
    if (countNode) {
      countNode.textContent = "0 métricas";
    }
    return;
  }
  const visibleMetrics = getVisibleMetrics();
  if (countNode) {
    const total = metricCatalog.length;
    const visible = visibleMetrics.length;
    countNode.textContent = visible === total ? `${total} métricas` : `${visible} de ${total} métricas`;
  }
  if (!visibleMetrics.length) {
    menuNode.innerHTML = `<div class="empty-state">Nenhuma métrica encontrada.</div>`;
    return;
  }
  if (!visibleMetrics.some((metric) => metric.key === activeMetricKey)) {
    activeMetricKey = visibleMetrics[0].key;
  }

  const groupedMetrics = getGroupedMetrics(visibleMetrics);
  menuNode.innerHTML = groupedMetrics
    .map((group) => `
      <section class="metric-group">
        <h3 class="metric-group__title">${escapeHtml(group.label)}</h3>
        <div class="metric-group__items">
          ${group.items
            .map((metric) => `
              <button
                type="button"
                class="metric-menu__item ${metric.key === activeMetricKey ? "metric-menu__item--active" : ""}"
                data-metric-key="${metric.key}"
              >
                <span class="metric-menu__top">
                  <span class="metric-menu__label">${escapeHtml(metric.label)}</span>
                </span>
                <span class="metric-menu__key">${escapeHtml(metric.key)}</span>
              </button>
            `)
            .join("")}
        </div>
      </section>
    `)
    .join("");
}

function renderDetail() {
  if (!detailNode) {
    return;
  }
  const metric = findActiveMetric();
  if (!metric) {
    detailNode.innerHTML = `<div class="empty-state">Selecione uma métrica no menu lateral.</div>`;
    return;
  }

  const parameterRows = (metric.parameters || [])
    .map((parameter) => `
      <label class="metric-param-row">
        <span class="label">${escapeHtml(parameter.label)}</span>
        <input
          class="filter-input"
          type="number"
          data-param-key="${parameter.key}"
          min="${parameter.min}"
          max="${parameter.max}"
          step="${parameter.step}"
          value="${parameter.value}"
          required
        />
        <small>${escapeHtml(parameter.description || "")}</small>
      </label>
    `)
    .join("");

  const hasEditableParams = (metric.parameters || []).length > 0;
  detailNode.innerHTML = `
    <header class="metrics-detail__header">
      <div>
        <p class="eyebrow">Métrica Selecionada</p>
        <h2>${escapeHtml(metric.label)}</h2>
        <p class="metrics-detail__meta">${escapeHtml(metric.key)}</p>
      </div>
      <span class="signal-badge">${escapeHtml(metric.key)}</span>
    </header>

    <section class="metrics-detail__block">
      <span class="label">Detalhes</span>
      <p class="hero-copy">${escapeHtml(metric.details || "Sem descrição.")}</p>
    </section>

    <section class="metrics-detail__block">
      <span class="label">Fórmula</span>
      <pre class="metric-formula">${escapeHtml(metric.formula || "Sem fórmula.")}</pre>
    </section>

    <form class="metrics-form" id="metric-form">
      <input type="hidden" name="metric_key" value="${escapeHtml(metric.key)}" />
      <section class="metrics-detail__block">
        <span class="label">Parâmetros editáveis</span>
        <div class="metric-param-grid">
          ${parameterRows || '<p class="hero-copy">Esta métrica não possui parâmetros editáveis.</p>'}
        </div>
      </section>
      <div class="metric-form-actions">
        <button class="trade-save-button" type="submit" ${hasEditableParams ? "" : "disabled"}>
          Salvar e rodar em todos os tickers
        </button>
        <span class="metric-save-status" id="metric-save-status">${escapeHtml(lastSaveMessage)}</span>
      </div>
    </form>
  `;
}

async function submitMetricForm(form) {
  const statusNode = document.getElementById("metric-save-status");
  const submitButton = form.querySelector("button[type='submit']");
  if (!submitButton) {
    return;
  }
  const metricKey = String(new FormData(form).get("metric_key") || "");
  if (!metricKey) {
    return;
  }

  const parameterInputs = Array.from(form.querySelectorAll("[data-param-key]"));
  const parameters = {};
  for (const input of parameterInputs) {
    const key = input.dataset.paramKey;
    const rawValue = input.value;
    const numeric = Number(rawValue);
    if (!key || !Number.isFinite(numeric)) {
      window.alert("Preencha todos os parâmetros com valores numéricos válidos.");
      return;
    }
    parameters[key] = numeric;
  }

  submitButton.disabled = true;
  if (statusNode) {
    statusNode.textContent = "Salvando e recalculando todos os tickers...";
  }

  try {
    const response = await fetch(`/metrics/catalog/${encodeURIComponent(metricKey)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ parameters }),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || `Falha ao salvar (${response.status})`);
    }
    const payload = await response.json();
    metricCatalog = payload.catalog || metricCatalog;
    activeMetricKey = payload.metric_key || metricKey;
    const summary = payload.scan_summary || {};
    lastSaveMessage =
      `Concluído. Processados: ${summary.tickers_processed || 0} de ${summary.tickers_loaded || 0}. ` +
      `Sinais: ${summary.signals_triggered || 0}.`;
    renderMenu();
    renderDetail();
  } catch (error) {
    lastSaveMessage = "";
    if (statusNode) {
      statusNode.textContent = "";
    }
    window.alert(`Não foi possível salvar: ${error.message}`);
  } finally {
    submitButton.disabled = false;
  }
}

menuNode?.addEventListener("click", (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }
  const button = event.target.closest("[data-metric-key]");
  if (!button) {
    return;
  }
  activeMetricKey = button.dataset.metricKey || activeMetricKey;
  renderMenu();
  renderDetail();
});

detailNode?.addEventListener("submit", (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }
  const form = event.target.closest("#metric-form");
  if (!form) {
    return;
  }
  event.preventDefault();
  submitMetricForm(form);
});

searchNode?.addEventListener("input", () => {
  searchQuery = searchNode.value || "";
  renderMenu();
  renderDetail();
});

renderMenu();
renderDetail();
