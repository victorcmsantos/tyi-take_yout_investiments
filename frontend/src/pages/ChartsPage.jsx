import { Fragment, useEffect, useMemo, useRef, useState } from 'react'
import { Chart as ChartJS } from 'chart.js/auto'
import { Bar, Doughnut, Line, Pie } from 'react-chartjs-2'
import ChartDataLabels from 'chartjs-plugin-datalabels'
import StatePanel from '../components/StatePanel'
import { useApiQuery } from '../hooks/useApiQuery'
import { usePersistedState } from '../persistedState'

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`
const brlCompact = (value) => `R$ ${Number(value || 0).toLocaleString('pt-BR', { notation: 'compact', maximumFractionDigits: 2 })}`
const MONTH_ORDER = [
  ['JAN', 'jan'],
  ['FEV', 'fev'],
  ['MAR', 'mar'],
  ['ABR', 'abr'],
  ['MAI', 'mai'],
  ['JUN', 'jun'],
  ['JUL', 'jul'],
  ['AGO', 'ago'],
  ['SET', 'set'],
  ['OUT', 'out'],
  ['NOV', 'nov'],
  ['DEZ', 'dez'],
]

ChartJS.register(ChartDataLabels)

const TERMINAL_TEXT = '#8096ad'
const TERMINAL_GRID = 'rgba(128, 150, 173, 0.16)'
const TERMINAL_PALETTE = ['#0f8a77', '#1f6feb', '#c48b2d', '#d95f2f', '#cf3f5b', '#6c63ff', '#2bb0c9', '#708090']
const TERMINAL_MONO = 'IBM Plex Mono, SFMono-Regular, monospace'

function withHexAlpha(color, alpha = 'FF') {
  return /^#[0-9a-f]{6}$/i.test(color) ? `${color}${alpha}` : color
}

function chartLegend(display = true) {
  return {
    display,
    labels: {
      color: TERMINAL_TEXT,
      usePointStyle: true,
      pointStyle: 'circle',
      boxWidth: 10,
      boxHeight: 10,
      padding: 14,
      font: { size: 11, weight: '700' },
    },
  }
}

const chartTooltip = {
  backgroundColor: 'rgba(7, 18, 31, 0.96)',
  titleColor: '#f4fbff',
  bodyColor: '#dbe8f5',
  borderColor: 'rgba(43, 176, 201, 0.32)',
  borderWidth: 1,
  padding: 12,
  displayColors: true,
  titleFont: { family: TERMINAL_MONO, size: 11, weight: '700' },
  bodyFont: { family: TERMINAL_MONO, size: 11 },
  footerFont: { family: TERMINAL_MONO, size: 10 },
}

function makeScale(ticksConfig) {
  return {
    ticks: ticksConfig,
    grid: {
      color: TERMINAL_GRID,
      drawBorder: false,
    },
    border: {
      display: false,
    },
  }
}

function buildCartesianOptions({ yScale, legend = true, stacked = false, indexAxis = 'x' } = {}) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    indexAxis,
    interaction: { intersect: false, mode: 'index' },
    scales: {
      x: {
        ticks: {
          color: TERMINAL_TEXT,
          maxRotation: 0,
          autoSkipPadding: 14,
          font: { size: 11, family: TERMINAL_MONO },
        },
        grid: {
          display: false,
          drawBorder: false,
        },
        border: {
          display: false,
        },
        stacked,
      },
      y: {
        ...yScale,
        stacked,
      },
    },
    plugins: {
      legend: chartLegend(legend),
      tooltip: chartTooltip,
    },
  }
}

function formatSliceLabel({ label, value, total, minPct = 8 }) {
  const amount = Number(value || 0)
  const sum = Number(total || 0)
  if (!Number.isFinite(amount) || !Number.isFinite(sum) || sum <= 0) return ''
  const pct = (amount / sum) * 100
  if (pct < minPct) return ''
  return `${label} ${pct.toFixed(1)}%`
}

function buildCircularOptions({ legend = false, datalabels }) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: chartLegend(legend),
      tooltip: chartTooltip,
      datalabels,
    },
  }
}

function normalizeChoice(value, options, fallback) {
  return options.includes(value) ? value : fallback
}

function normalizeStringList(value, allowed, fallback) {
  if (!Array.isArray(value)) return fallback
  const filtered = value.filter((item) => allowed.includes(item))
  return filtered.length > 0 ? [...new Set(filtered)] : fallback
}

function csvCell(value) {
  const text = String(value ?? '')
  return `"${text.replace(/"/g, '""')}"`
}

function downloadTextFile(filename, content, mimeType = 'text/plain;charset=utf-8') {
  const blob = new Blob([content], { type: mimeType })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  document.body.appendChild(anchor)
  anchor.click()
  anchor.remove()
  URL.revokeObjectURL(url)
}

function exportRowsAsCsv(filename, headers, rows) {
  const csvLines = [
    headers.map(csvCell).join(','),
    ...rows.map((row) => row.map(csvCell).join(',')),
  ]
  downloadTextFile(filename, csvLines.join('\n'), 'text/csv;charset=utf-8')
}

function exportChartDataAsCsv(filename, chartData) {
  const labels = Array.isArray(chartData?.labels) ? chartData.labels : []
  const datasets = Array.isArray(chartData?.datasets) ? chartData.datasets : []
  const headers = ['label', ...datasets.map((dataset) => dataset.label || 'serie')]
  const rows = labels.map((label, idx) => [
    label,
    ...datasets.map((dataset) => dataset?.data?.[idx] ?? ''),
  ])
  exportRowsAsCsv(filename, headers, rows)
}

function exportChartAsImage(filename, chartInstance) {
  const chart = chartInstance?.chartInstance || chartInstance
  if (!chart || typeof chart.toBase64Image !== 'function') return
  const anchor = document.createElement('a')
  anchor.href = chart.toBase64Image('image/png', 1)
  anchor.download = filename
  document.body.appendChild(anchor)
  anchor.click()
  anchor.remove()
}

function ChartPanel({ title, subtitle, className = '', children, controls, actions }) {
  return (
    <article className={`card chart-card ${className}`.trim()}>
      <div className="chart-panel-head">
        <div>
          <h3>{title}</h3>
          {subtitle ? <p className="subtitle">{subtitle}</p> : null}
        </div>
        {controls ? <div className="chart-panel-controls">{controls}</div> : null}
      </div>
      {actions ? <div className="chart-panel-actions">{actions}</div> : null}
      {children}
    </article>
  )
}

function DataSection({ id, title, subtitle, controls, children }) {
  return (
    <section id={id} className="chart-data-section">
      <div className="chart-data-section-head">
        <div>
          <h2>{title}</h2>
          {subtitle ? <p className="subtitle">{subtitle}</p> : null}
        </div>
        {controls ? <div className="chart-data-section-controls">{controls}</div> : null}
      </div>
      {children}
    </section>
  )
}

function ChartsPage({ selectedPortfolioIds }) {
  const [range, setRange] = usePersistedState('charts.range.v1', '12m')
  const [scope, setScope] = usePersistedState('charts.scope.v1', 'all')
  const [annualMetrics, setAnnualMetrics] = usePersistedState('charts.annual-metrics.v1', ['invested', 'incomes'])
  const [annualCategories, setAnnualCategories] = usePersistedState('charts.annual-categories.v1', ['br', 'us', 'fii', 'cripto', 'fixa'])
  const [tickerSortBy, setTickerSortBy] = usePersistedState('charts.ticker-sort-by.v1', 'period_value')
  const [tickerSortMetric, setTickerSortMetric] = usePersistedState('charts.ticker-sort-metric.v1', 'incomes')
  const [tickerSortMonthKey, setTickerSortMonthKey] = usePersistedState('charts.ticker-sort-month.v1', 'total')
  const [tickerSortDir, setTickerSortDir] = usePersistedState('charts.ticker-sort-dir.v1', 'desc')
  const [patrimonyTypeRange, setPatrimonyTypeRange] = usePersistedState('charts.patrimony-type-range.v1', '12m')
  const [patrimonyTypeMetric, setPatrimonyTypeMetric] = usePersistedState('charts.patrimony-type-metric.v2', 'net')
  const [hiddenBlocks, setHiddenBlocks] = usePersistedState('charts.hidden-blocks.v1', [])
  const [loadingMessage, setLoadingMessage] = useState('Atualizando graficos...')
  const [showLayoutControls, setShowLayoutControls] = useState(false)
  const [hoveredPatrimonySeriesKey, setHoveredPatrimonySeriesKey] = useState('')
  const previousPortfoliosRef = useRef('')
  const chartRefs = useRef({})
  const rangeValue = normalizeChoice(range, ['6m', '12m', '24m', '60m'], '12m')
  const scopeValue = normalizeChoice(scope, ['all', 'br', 'us', 'fiis', 'crypto'], 'all')
  const annualMetricsValue = normalizeStringList(annualMetrics, ['invested', 'incomes'], ['invested', 'incomes'])
  const annualCategoriesValue = normalizeStringList(annualCategories, ['br', 'us', 'fii', 'cripto', 'fixa'], ['br', 'us', 'fii', 'cripto', 'fixa'])
  const tickerSortByValue = normalizeChoice(tickerSortBy, ['ticker', 'period_value'], 'period_value')
  const tickerSortMetricValue = normalizeChoice(tickerSortMetric, ['invested', 'incomes'], 'incomes')
  const tickerSortMonthKeyValue = typeof tickerSortMonthKey === 'string' ? tickerSortMonthKey : 'total'
  const tickerSortDirValue = normalizeChoice(tickerSortDir, ['asc', 'desc'], 'desc')
  const patrimonyTypeRangeValue = normalizeChoice(patrimonyTypeRange, ['6m', '12m', '24m', '60m'], '12m')
  const patrimonyTypeMetricValue = normalizeChoice(patrimonyTypeMetric, ['value', 'pnl', 'net'], 'net')
  const hiddenBlocksValue = Array.isArray(hiddenBlocks) ? hiddenBlocks.filter((item) => typeof item === 'string') : []
  const {
    data: corePayload,
    loading: loadingCore,
    refreshing: refreshingCore,
    error,
  } = useApiQuery('/api/charts/core', {
    params: {
      portfolio_id: selectedPortfolioIds,
    },
  })
  const {
    data: benchmarkPayload,
    loading: loadingBenchmark,
    refreshing: refreshingBenchmark,
    error: benchmarkError,
  } = useApiQuery('/api/charts/benchmark', {
    params: {
      portfolio_id: selectedPortfolioIds,
      range: rangeValue,
      scope: scopeValue,
    },
  })
  const {
    data: tickerPayload,
    loading: loadingTicker,
    refreshing: refreshingTicker,
    error: tickerError,
  } = useApiQuery('/api/charts/ticker-summary', {
    params: {
      portfolio_id: selectedPortfolioIds,
      months: 8,
    },
  })
  const {
    data: patrimonyByTypePayload,
    loading: loadingPatrimonyByType,
    refreshing: refreshingPatrimonyByType,
    error: patrimonyByTypeError,
  } = useApiQuery('/api/charts/patrimony-open-pnl-by-type', {
    params: {
      portfolio_id: selectedPortfolioIds,
      range: patrimonyTypeRangeValue,
    },
    initialData: { labels: [], datasets: [] },
  })

  useEffect(() => {
    const currentPortfolioKey = JSON.stringify(selectedPortfolioIds || [])
    const portfolioChanged = previousPortfoliosRef.current !== currentPortfolioKey
    previousPortfoliosRef.current = currentPortfolioKey
    setLoadingMessage(portfolioChanged ? 'Lendo carteiras selecionadas...' : 'Atualizando graficos...')
  }, [selectedPortfolioIds])

  const onToggleMetric = (metric) => {
    setAnnualMetrics((current) => {
      const base = normalizeStringList(current, ['invested', 'incomes'], ['invested', 'incomes'])
      const has = base.includes(metric)
      if (has) {
        const next = base.filter((item) => item !== metric)
        return next.length > 0 ? next : base
      }
      return [...base, metric]
    })
  }

  const onToggleCategory = (category) => {
    setAnnualCategories((current) => {
      const base = normalizeStringList(current, ['br', 'us', 'fii', 'cripto', 'fixa'], ['br', 'us', 'fii', 'cripto', 'fixa'])
      const has = base.includes(category)
      if (has) {
        const next = base.filter((item) => item !== category)
        return next.length > 0 ? next : base
      }
      return [...base, category]
    })
  }

  const moneyScale = useMemo(() => makeScale({
    callback: (value) => brl(value),
    color: TERMINAL_TEXT,
    font: { size: 11, family: TERMINAL_MONO },
  }), [])

  const percentScale = useMemo(() => makeScale({
    callback: (value) => `${Number(value || 0).toFixed(2)}%`,
    color: TERMINAL_TEXT,
    font: { size: 11, family: TERMINAL_MONO },
  }), [])

  const annualSummary = useMemo(() => {
    const rows = corePayload?.monthly_class_summary || []
    const yearMap = new Map()
    for (const row of rows) {
      const label = String(row.label || '').toLowerCase()
      const [monthKey, yearShort] = label.split('/')
      if (!monthKey || !yearShort) continue
      const year = 2000 + Number(yearShort)
      if (!Number.isFinite(year)) continue
      if (!yearMap.has(year)) {
        yearMap.set(
          year,
          Object.fromEntries(MONTH_ORDER.map(([, key]) => [key, { invested: 0, incomes: 0 }])),
        )
      }
      const yearEntry = yearMap.get(year)
      let invested = 0
      let incomes = 0
      if (annualCategoriesValue.includes('br')) {
        invested += Number(row.br_invested || 0)
        incomes += Number(row.br_incomes || 0)
      }
      if (annualCategoriesValue.includes('us')) {
        invested += Number(row.us_invested || 0)
        incomes += Number(row.us_incomes || 0)
      }
      if (annualCategoriesValue.includes('fii')) {
        invested += Number(row.fii_invested || 0)
        incomes += Number(row.fii_incomes || 0)
      }
      if (annualCategoriesValue.includes('cripto')) {
        invested += Number(row.cripto_invested || 0)
        incomes += Number(row.cripto_incomes || 0)
      }
      if (annualCategoriesValue.includes('fixa')) {
        invested += Number(row.fixa_invested || 0)
        incomes += Number(row.fixa_incomes || 0)
      }
      if (yearEntry[monthKey]) {
        yearEntry[monthKey] = { invested, incomes }
      }
    }
    const years = [...yearMap.keys()].sort((a, b) => a - b).map((year) => {
      const byMonth = yearMap.get(year)
      const investedValues = MONTH_ORDER.map(([, key]) => Number(byMonth[key]?.invested || 0))
      const incomesValues = MONTH_ORDER.map(([, key]) => Number(byMonth[key]?.incomes || 0))
      return {
        label: String(year),
        invested_total: investedValues.reduce((acc, v) => acc + v, 0),
        incomes_total: incomesValues.reduce((acc, v) => acc + v, 0),
        invested_values: investedValues,
        incomes_values: incomesValues,
      }
    })
    return { months: MONTH_ORDER.map(([label]) => label), years }
  }, [corePayload, annualCategoriesValue])

  if (loadingCore && !corePayload) {
    return (
      <StatePanel
        busy
        eyebrow="Graficos"
        title="Montando os paines visuais"
        description="Carregando distribuicao, benchmark e historico por ticker."
      />
    )
  }
  if (error) return <p className="error">{error}</p>
  if (!corePayload) {
    return (
      <StatePanel
        eyebrow="Graficos"
        title="Ainda nao ha base suficiente para os graficos"
        description="Selecione outra carteira ou alimente mais movimentacoes para liberar os paineis comparativos."
      />
    )
  }

  const categoryChart = corePayload.category_chart || { labels: [], values: [] }
  const topAssetsChart = corePayload.top_assets_chart || { labels: [], values: [] }
  const allocationByGroupCharts = corePayload.allocation_by_group_charts || []
  const cardsChart = corePayload.cards_chart || { labels: [], values: [] }
  const resultByCategoryChart = corePayload.result_by_category_chart || { labels: [], values: [] }
  const classesChart = corePayload.classes_chart || { labels: [], values: [] }
  const fixedInvestmentChart = corePayload.fixed_income_investment_chart || { labels: [], values: [] }
  const fixedDistributorChart = corePayload.fixed_income_distributor_chart || { labels: [], values: [] }
  const fixedIssuerChart = corePayload.fixed_income_issuer_chart || { labels: [], investment_values: [], income_values: [] }
  const monthlyIncomeChart = corePayload.monthly_income_chart || { labels: [], fii_values: [], acoes_values: [] }
  const benchmarkChart = benchmarkPayload || { labels: [], datasets: [] }
  const monthlyClassSummary = corePayload.monthly_class_summary || []
  const monthlyTickerSummary = tickerPayload || { months: [], totals: [], rows: [] }

  const onToggleTickerSort = (field) => {
    if (tickerSortByValue === field) {
      return setTickerSortDir((current) => (normalizeChoice(current, ['asc', 'desc'], 'desc') === 'asc' ? 'desc' : 'asc'))
    }
    setTickerSortBy(field)
    setTickerSortDir(field === 'ticker' ? 'asc' : 'desc')
  }

  const tickerSortLabel = (label, field) => {
    if (tickerSortByValue !== field) return label
    return `${label} ${tickerSortDirValue === 'asc' ? '↑' : '↓'}`
  }

  const sortedTickerRows = [...(monthlyTickerSummary.rows || [])]
  sortedTickerRows.sort((left, right) => {
    const factor = tickerSortDirValue === 'asc' ? 1 : -1
    if (tickerSortByValue === 'ticker') {
      const a = String(left.ticker || '')
      const b = String(right.ticker || '')
      return a.localeCompare(b, 'pt-BR') * factor
    }
    let a = 0
    let b = 0
    if (tickerSortMonthKeyValue === 'total') {
      a = Number(tickerSortMetricValue === 'invested' ? left.total_invested : left.total_incomes)
      b = Number(tickerSortMetricValue === 'invested' ? right.total_invested : right.total_incomes)
    } else {
      a = Number(left.months?.[tickerSortMonthKeyValue]?.[tickerSortMetricValue] || 0)
      b = Number(right.months?.[tickerSortMonthKeyValue]?.[tickerSortMetricValue] || 0)
    }
    if (a === b) return String(left.ticker || '').localeCompare(String(right.ticker || ''), 'pt-BR')
    return (a - b) * factor
  })

  const classesTotal = (classesChart.values || []).reduce((acc, value) => acc + Number(value || 0), 0)
  const categoryTotal = (categoryChart.values || []).reduce((acc, value) => acc + Number(value || 0), 0)
  const donutColors = ['#0f8a77', '#1f6feb', '#c48b2d', '#d95f2f']

  const benchmarkData = {
    labels: benchmarkChart.labels || [],
    datasets: (benchmarkChart.datasets || []).map((series, idx) => ({
      label: series.label,
      data: series.values,
      borderColor: series.color,
      backgroundColor: idx === 0 ? 'rgba(15, 138, 119, 0.14)' : 'transparent',
      borderWidth: idx === 0 ? 2.4 : 1.8,
      fill: idx === 0,
      tension: 0.18,
      pointRadius: 0,
      pointHoverRadius: 4,
    })),
  }

  const classesData = {
    labels: classesChart.labels || [],
    datasets: [{ label: 'Valor', data: classesChart.values || [], backgroundColor: ['#1f6feb', '#0f8a77'] }],
  }

  const categoryData = {
    labels: categoryChart.labels || [],
    datasets: [{ label: 'Valor', data: categoryChart.values || [], backgroundColor: donutColors, borderWidth: 1 }],
  }

  const resultByCategoryData = {
    labels: resultByCategoryChart.labels || [],
    datasets: [{
      label: 'Resultado',
      data: resultByCategoryChart.values || [],
      backgroundColor: (resultByCategoryChart.values || []).map((v) => (Number(v || 0) >= 0 ? '#1f6feb' : '#cf3f5b')),
    }],
  }

  const cardsData = {
    labels: cardsChart.labels || [],
    datasets: [{
      label: 'Valor',
      data: cardsChart.values || [],
      backgroundColor: ['#0f8a77', '#1f6feb', '#c48b2d'],
    }],
  }

  const monthlyIncomeData = {
    labels: monthlyIncomeChart.labels || [],
    datasets: [
      {
        label: 'FIIs',
        data: monthlyIncomeChart.fii_values || [],
        borderColor: '#0f8a77',
        backgroundColor: 'rgba(15, 138, 119, 0.15)',
        tension: 0.25,
        pointRadius: 0,
        pointHoverRadius: 4,
      },
      {
        label: 'Acoes',
        data: monthlyIncomeChart.acoes_values || [],
        borderColor: '#1f6feb',
        backgroundColor: 'rgba(31, 111, 235, 0.15)',
        tension: 0.25,
        pointRadius: 0,
        pointHoverRadius: 4,
      },
    ],
  }

  const topAssetsData = {
    labels: topAssetsChart.labels || [],
    datasets: [{ label: 'Valor', data: topAssetsChart.values || [], backgroundColor: '#1f6feb' }],
  }

  const fixedInvestmentData = {
    labels: fixedInvestmentChart.labels || [],
    datasets: [{ label: 'Valor aplicado', data: fixedInvestmentChart.values || [], backgroundColor: '#1f6feb' }],
  }

  const fixedDistributorData = {
    labels: fixedDistributorChart.labels || [],
    datasets: [{ label: 'Valor aplicado', data: fixedDistributorChart.values || [], backgroundColor: '#2bb0c9' }],
  }

  const fixedIssuerData = {
    labels: fixedIssuerChart.labels || [],
    datasets: [
      {
        label: 'aplicação',
        data: fixedIssuerChart.investment_values || [],
        backgroundColor: '#1f6feb',
        stack: 'fixed-issuer',
      },
      {
        label: 'rendimento',
        data: fixedIssuerChart.income_values || [],
        backgroundColor: '#0f8a77',
        stack: 'fixed-issuer',
      },
    ],
  }

  const patrimonyByTypeRaw = patrimonyByTypePayload || { labels: [], datasets: [] }
  const isPatrimonyOpenMetric = patrimonyTypeMetricValue === 'pnl'
  const patrimonyByTypeDatasets = (patrimonyByTypeRaw.datasets || []).map((series, idx) => {
    const color = TERMINAL_PALETTE[idx % TERMINAL_PALETTE.length]
    const datasetKey = `${series.family}:${series.key || series.label || idx}`
    const isHighlighted = !hoveredPatrimonySeriesKey || hoveredPatrimonySeriesKey === datasetKey
    const values = patrimonyTypeMetricValue === 'value'
      ? (series.value_values || [])
      : patrimonyTypeMetricValue === 'pnl'
        ? (series.pnl_values || [])
        : (series.net_values || [])
    return {
      key: datasetKey,
      label: `${series.label}${series.family === 'fixa' ? ' · RF' : ' · RV'}`,
      data: values.map((value) => Number(value || 0)),
      borderColor: isHighlighted ? color : withHexAlpha(color, '45'),
      backgroundColor: isPatrimonyOpenMetric
        ? (isHighlighted ? withHexAlpha(color, '66') : withHexAlpha(color, '18'))
        : (isHighlighted ? withHexAlpha(color, '22') : withHexAlpha(color, '10')),
      borderWidth: isHighlighted ? (isPatrimonyOpenMetric ? 2.4 : 2.4) : 1.2,
      tension: 0.22,
      fill: isPatrimonyOpenMetric ? (idx === 0 ? 'origin' : '-1') : false,
      pointRadius: isHighlighted ? 1.5 : 0,
      pointHoverRadius: 5,
      spanGaps: true,
      stack: isPatrimonyOpenMetric ? 'patrimony-open' : undefined,
      borderDash: !isPatrimonyOpenMetric && series.family === 'fixa' ? [8, 5] : undefined,
      isActiveOpenSeries: Number(series.value_values?.[series.value_values.length - 1] || 0) > 0
        || Number(series.invested_values?.[series.invested_values.length - 1] || 0) > 0,
    }
  }).filter((dataset) => {
    const hasVisibleHistory = dataset.data.some((value) => Number(value || 0) !== 0)
    if (!hasVisibleHistory) return false
    if (!isPatrimonyOpenMetric) return true
    return dataset.isActiveOpenSeries
  }).map(({ isActiveOpenSeries, ...dataset }) => dataset)

  const patrimonyByTypeData = {
    labels: patrimonyByTypeRaw.labels || [],
    datasets: patrimonyByTypeDatasets,
  }
  const hoveredPatrimonySeriesLabel = patrimonyByTypeDatasets.find((dataset) => dataset.key === hoveredPatrimonySeriesKey)?.label || ''
  const setHoveredPatrimonyByIndex = (datasetIndex) => {
    if (typeof datasetIndex !== 'number' || datasetIndex < 0) {
      setHoveredPatrimonySeriesKey('')
      return
    }
    setHoveredPatrimonySeriesKey(patrimonyByTypeDatasets[datasetIndex]?.key || '')
  }
  const handlePatrimonyChartHover = (event, _activeElements, chart) => {
    const nearest = chart?.getElementsAtEventForMode?.(event, 'nearest', { intersect: true }, false) || []
    if (nearest.length > 0) {
      setHoveredPatrimonyByIndex(nearest[0]?.datasetIndex)
      return
    }
    setHoveredPatrimonySeriesKey('')
  }

  const allocationCharts = allocationByGroupCharts.map((item, idx) => {
    const colors = (item.values || []).map((_, colorIdx) => TERMINAL_PALETTE[colorIdx % TERMINAL_PALETTE.length])
    return {
      key: `allocation-${idx}`,
      title: item.title,
      labels: item.labels || [],
      values: item.values || [],
      weights: item.weights || [],
      data: {
        labels: item.labels || [],
        datasets: [{ label: 'Patrimonio', data: item.values || [], backgroundColor: colors, borderRadius: 8, borderSkipped: false }],
      },
    }
  })

  const pieValueInsideOptions = buildCircularOptions({
    legend: true,
    datalabels: {
      color: '#ffffff',
      anchor: 'center',
      align: 'center',
      formatter: (value, ctx) => formatSliceLabel({
        label: classesData.labels?.[ctx.dataIndex] || '',
        value,
        total: classesTotal,
        minPct: 12,
      }),
      font: { weight: '700', size: 11 },
    },
  })

  const categoryDonutOptions = buildCircularOptions({
    legend: false,
    datalabels: {
      color: TERMINAL_TEXT,
      anchor: 'end',
      align: 'end',
      offset: 6,
      formatter: (value, ctx) => {
        const label = categoryData.labels?.[ctx.dataIndex] || ''
        const sliceLabel = formatSliceLabel({ label, value, total: categoryTotal, minPct: 9 })
        if (!sliceLabel) return ''
        return `${sliceLabel} | ${brlCompact(value)}`
      },
      font: { size: 9, weight: '600' },
    },
  })

  const resultByCategoryOptions = {
    ...buildCartesianOptions({ yScale: moneyScale, legend: false }),
    plugins: {
      ...buildCartesianOptions({ yScale: moneyScale, legend: false }).plugins,
      datalabels: {
        color: (ctx) => (Number(ctx.raw || 0) >= 0 ? '#255eb5' : '#b63838'),
        anchor: (ctx) => (Number(ctx.raw || 0) >= 0 ? 'end' : 'start'),
        align: (ctx) => (Number(ctx.raw || 0) >= 0 ? 'end' : 'start'),
        offset: 4,
        formatter: (value) => brlCompact(value),
        font: { weight: '700', size: 11 },
      },
    },
  }

  const consolidatedOptions = {
    ...buildCartesianOptions({ yScale: moneyScale, legend: false }),
    plugins: {
      ...buildCartesianOptions({ yScale: moneyScale, legend: false }).plugins,
      datalabels: {
        color: '#ffffff',
        anchor: 'center',
        align: 'center',
        formatter: (value) => brlCompact(value),
        font: { weight: '700', size: 11 },
      },
    },
  }

  const benchmarkOptions = buildCartesianOptions({ yScale: percentScale, legend: true })
  const incomeLineOptions = buildCartesianOptions({ yScale: moneyScale, legend: true })
  const patrimonyOpenAreaOptions = {
    ...buildCartesianOptions({ yScale: moneyScale, legend: true, stacked: true }),
    interaction: { intersect: true, mode: 'nearest' },
    onHover: handlePatrimonyChartHover,
    plugins: {
      ...buildCartesianOptions({ yScale: moneyScale, legend: true, stacked: true }).plugins,
      legend: {
        ...chartLegend(true),
        onHover: (_event, item) => setHoveredPatrimonyByIndex(item?.datasetIndex),
        onLeave: () => setHoveredPatrimonySeriesKey(''),
      },
      filler: {
        propagate: true,
      },
    },
    scales: {
      ...buildCartesianOptions({ yScale: moneyScale, legend: true, stacked: true }).scales,
      y: {
        ...buildCartesianOptions({ yScale: moneyScale, legend: true, stacked: true }).scales.y,
        grace: '8%',
        grid: {
          color: (ctx) => (Number(ctx.tick?.value || 0) === 0 ? 'rgba(219, 232, 245, 0.32)' : TERMINAL_GRID),
          lineWidth: (ctx) => (Number(ctx.tick?.value || 0) === 0 ? 1.4 : 1),
          drawBorder: false,
        },
      },
    },
  }
  const patrimonyLineOptions = {
    ...incomeLineOptions,
    interaction: { intersect: true, mode: 'nearest' },
    onHover: handlePatrimonyChartHover,
    plugins: {
      ...incomeLineOptions.plugins,
      legend: {
        ...chartLegend(true),
        onHover: (_event, item) => setHoveredPatrimonyByIndex(item?.datasetIndex),
        onLeave: () => setHoveredPatrimonySeriesKey(''),
      },
    },
  }
  const barMoneyOptions = buildCartesianOptions({ yScale: moneyScale, legend: false })
  const fixedIssuerOptions = {
    ...buildCartesianOptions({ yScale: moneyScale, legend: true, stacked: true }),
    plugins: {
      ...buildCartesianOptions({ yScale: moneyScale, legend: true, stacked: true }).plugins,
      datalabels: {
        color: TERMINAL_TEXT,
        anchor: 'end',
        align: 'end',
        offset: 4,
        formatter: (_, ctx) => {
          const dataIndex = ctx.dataIndex
          const datasets = ctx.chart.data.datasets || []
          const currentIndex = ctx.datasetIndex
          const currentValue = Number(ctx.raw || 0)
          if (!Number.isFinite(currentValue)) return ''

          const lastVisibleIndex = datasets.reduce((lastIndex, dataset, index) => {
            const value = Number(dataset?.data?.[dataIndex] || 0)
            return value > 0 ? index : lastIndex
          }, -1)

          if (currentIndex !== lastVisibleIndex) return ''

          const total = datasets.reduce((sum, dataset) => (
            sum + Number(dataset?.data?.[dataIndex] || 0)
          ), 0)
          if (!Number.isFinite(total) || total <= 0) return ''
          return brlCompact(total)
        },
        font: { weight: '700', size: 10 },
      },
    },
  }
  const allocationBarOptionsFor = (chart) => ({
    ...buildCartesianOptions({ yScale: moneyScale, legend: false, indexAxis: 'y' }),
    plugins: {
      ...buildCartesianOptions({ yScale: moneyScale, legend: false, indexAxis: 'y' }).plugins,
      datalabels: {
        color: TERMINAL_TEXT,
        anchor: 'end',
        align: 'right',
        offset: 6,
        formatter: (value, ctx) => {
          const i = ctx.dataIndex
          const label = chart.labels?.[i] || ''
          const weight = Number(chart.weights?.[i] || 0)
          if (weight < 4) return ''
          return `${label} ${weight.toFixed(1)}%`
        },
        font: { size: 10, weight: '700' },
      },
    },
  })

  const analyticsHighlights = [
    {
      label: 'Benchmark',
      value: `${benchmarkChart.labels?.length || 0} pts`,
      meta: `${benchmarkChart.datasets?.length || 0} série(s)`,
    },
    {
      label: 'Patrimônio por tipo',
      value: `${patrimonyByTypeDatasets.length}`,
      meta: patrimonyTypeMetricValue === 'value'
        ? 'séries patrimoniais'
        : patrimonyTypeMetricValue === 'pnl'
          ? 'aberto por tipo'
          : 'resultado líquido',
    },
    {
      label: 'Top ativos',
      value: `${topAssetsChart.labels?.length || 0}`,
      meta: 'ativos ranqueados',
    },
    {
      label: 'Alocação',
      value: `${allocationCharts.length}`,
      meta: 'painel(is) segmentado(s)',
    },
    {
      label: 'Ticker ledger',
      value: `${sortedTickerRows.length}`,
      meta: 'linhas por ativo',
    },
  ]
  const baseSectionLinks = [
    { href: '#patrimonio', label: 'Patrimônio' },
    { href: '#composicao', label: 'Composição' },
    { href: '#renda-fixa', label: 'Renda fixa' },
    { href: '#ledger-anual', label: 'Ledger anual' },
    { href: '#ticker-ledger', label: 'Ticker ledger' },
    { href: '#classe-ledger', label: 'Classe ledger' },
  ]
  const blockOptions = [
    { key: 'benchmark', label: 'Benchmark', description: 'Rentabilidade comparada com índices e benchmarks.' },
    { key: 'monthlyIncome', label: 'Proventos mês a mês', description: 'Fluxo mensal entre FIIs e ações.' },
    { key: 'patrimonyByType', label: 'Patrimônio por tipo', description: 'Evolução patrimonial e lucro/loss por classe e tipo.' },
    { key: 'classesPie', label: 'Renda variável x fixa', description: 'Mistura principal da carteira.' },
    { key: 'categoryDonut', label: 'Distribuição por tipo', description: 'Peso relativo por classe de ativo.' },
    { key: 'resultCategory', label: 'Resultado por categoria', description: 'Ganho ou perda por agrupamento.' },
    { key: 'cardsBar', label: 'Consolidado da carteira', description: 'Resumo absoluto por grande bloco.' },
    { key: 'topAssets', label: 'Top ativos', description: 'Ranking de concentração por patrimônio.' },
    ...allocationCharts.map((item) => ({
      key: item.key,
      label: item.title,
      description: 'Distribuição interna por agrupamento em barra horizontal.',
    })),
    { key: 'fixedInvestment', label: 'Aplicação renda fixa', description: 'Volume aplicado por recorte do book fixo.' },
    { key: 'fixedDistributor', label: 'Distribuidor renda fixa', description: 'Concentração da distribuição da carteira.' },
    { key: 'fixedIssuer', label: 'Emissor renda fixa', description: 'Principal e rendimento por emissor.' },
    { key: 'ledgerAnual', label: 'Ledger anual', description: 'Tabela comparativa por ano e mês.' },
    { key: 'tickerLedger', label: 'Ticker ledger', description: 'Aportes e proventos por ativo.' },
    { key: 'classLedger', label: 'Classe ledger', description: 'Resumo mensal por classe.' },
  ]
  const hiddenSet = new Set(hiddenBlocksValue)

  const isVisible = (key) => !hiddenSet.has(key)
  const sectionLinks = baseSectionLinks.filter((item) => {
    if (item.href === '#patrimonio') return isVisible('patrimonyByType')
    if (item.href === '#composicao') {
      return isVisible('classesPie')
        || isVisible('categoryDonut')
        || isVisible('resultCategory')
        || isVisible('cardsBar')
        || isVisible('topAssets')
        || allocationCharts.some((chart) => isVisible(chart.key))
    }
    if (item.href === '#renda-fixa') return isVisible('fixedInvestment') || isVisible('fixedDistributor') || isVisible('fixedIssuer')
    if (item.href === '#ledger-anual') return isVisible('ledgerAnual')
    if (item.href === '#ticker-ledger') return isVisible('tickerLedger')
    if (item.href === '#classe-ledger') return isVisible('classLedger')
    return true
  })
  const toggleBlockVisibility = (key) => {
    setHiddenBlocks((current) => (
      current.includes(key)
        ? current.filter((item) => item !== key)
        : [...current, key]
    ))
  }
  const showAllBlocks = () => setHiddenBlocks([])

  const registerChartRef = (key) => (instance) => {
    if (instance) chartRefs.current[key] = instance
    else delete chartRefs.current[key]
  }

  const annualLedgerRows = annualSummary.months.map((month, monthIdx) => [
    month,
    ...annualSummary.years.flatMap((year) => [
      annualMetricsValue.includes('invested') ? [year.invested_values?.[monthIdx] ?? 0] : [],
      annualMetricsValue.includes('incomes') ? [year.incomes_values?.[monthIdx] ?? 0] : [],
    ]),
  ])
  const annualLedgerHeaders = [
    'mes',
    ...annualSummary.years.flatMap((year) => [
      annualMetricsValue.includes('invested') ? `${year.label} investidos` : null,
      annualMetricsValue.includes('incomes') ? `${year.label} proventos` : null,
    ].filter(Boolean)),
  ]
  const tickerLedgerHeaders = [
    'ticker',
    ...monthlyTickerSummary.months.flatMap((month) => [`${month.label} investidos`, `${month.label} proventos`]),
  ]
  const tickerLedgerRows = sortedTickerRows.map((row) => [
    row.ticker,
    ...monthlyTickerSummary.months.flatMap((month) => {
      const values = row.months?.[month.key] || { invested: 0, incomes: 0 }
      return [values.invested, values.incomes]
    }),
  ])
  const classLedgerHeaders = [
    'data',
    'br investidos',
    'br proventos',
    'fii investidos',
    'fii proventos',
    'fixa investidos',
    'fixa proventos',
    'cripto investidos',
    'cripto proventos',
    'total investidos',
    'total proventos',
  ]
  const classLedgerRows = monthlyClassSummary.map((row) => [
    row.label,
    row.br_invested,
    row.br_incomes,
    row.fii_invested,
    row.fii_incomes,
    row.fixa_invested,
    row.fixa_incomes,
    row.cripto_invested,
    row.cripto_incomes,
    row.total_invested,
    row.total_incomes,
  ])

  const makeChartActions = (key, filenameBase, chartData) => (
    <div className="dashboard-pref-actions">
      <button type="button" className="icon-btn" onClick={() => exportChartDataAsCsv(`${filenameBase}.csv`, chartData)}>
        CSV
      </button>
      <button
        type="button"
        className="icon-btn"
        onClick={() => exportChartAsImage(`${filenameBase}.png`, chartRefs.current[key])}
      >
        PNG
      </button>
      <button type="button" className="icon-btn" onClick={() => toggleBlockVisibility(key)}>
        Ocultar
      </button>
    </div>
  )

  const makeTableControls = (headers, rows, filenameBase, blockKey, extraControls = null) => (
    <div className="chart-section-toolbar">
      {extraControls}
      <div className="dashboard-pref-actions">
        <button type="button" className="icon-btn" onClick={() => exportRowsAsCsv(`${filenameBase}.csv`, headers, rows)}>
          CSV
        </button>
        <button type="button" className="icon-btn" onClick={() => toggleBlockVisibility(blockKey)}>
          Ocultar
        </button>
      </div>
    </div>
  )

  const annualFilterControls = (
    <div className="inline-filters inline-filters-checks">
      <label><input type="checkbox" checked={annualMetricsValue.includes('invested')} onChange={() => onToggleMetric('invested')} /> Investidos</label>
      <label><input type="checkbox" checked={annualMetricsValue.includes('incomes')} onChange={() => onToggleMetric('incomes')} /> Proventos</label>
      <label><input type="checkbox" checked={annualCategoriesValue.includes('br')} onChange={() => onToggleCategory('br')} /> BR</label>
      <label><input type="checkbox" checked={annualCategoriesValue.includes('us')} onChange={() => onToggleCategory('us')} /> US</label>
      <label><input type="checkbox" checked={annualCategoriesValue.includes('fii')} onChange={() => onToggleCategory('fii')} /> FIIs</label>
      <label><input type="checkbox" checked={annualCategoriesValue.includes('cripto')} onChange={() => onToggleCategory('cripto')} /> Cripto</label>
      <label><input type="checkbox" checked={annualCategoriesValue.includes('fixa')} onChange={() => onToggleCategory('fixa')} /> FIXA</label>
    </div>
  )

  const tickerFilterControls = (
    <div className="inline-filters chart-inline-controls">
      <label>
        Ordenar por
        <select value={tickerSortMetricValue} onChange={(e) => setTickerSortMetric(e.target.value)}>
          <option value="invested">investidos</option>
          <option value="incomes">proventos</option>
        </select>
      </label>
      <label>
        Mês/ano
        <select value={tickerSortMonthKeyValue} onChange={(e) => setTickerSortMonthKey(e.target.value)}>
          <option value="total">Total (todos os meses)</option>
          {monthlyTickerSummary.months.map((month) => (
            <option key={month.key} value={month.key}>{month.label}</option>
          ))}
        </select>
      </label>
      <label>
        Direção
        <select value={tickerSortDirValue} onChange={(e) => setTickerSortDir(e.target.value)}>
          <option value="desc">Maior para menor</option>
          <option value="asc">Menor para maior</option>
        </select>
      </label>
    </div>
  )

  const patrimonyByTypeControls = (
    <div className="inline-filters chart-inline-controls">
      <label>
        Período
        <select value={patrimonyTypeRangeValue} onChange={(e) => setPatrimonyTypeRange(e.target.value)}>
          <option value="6m">6 Meses</option>
          <option value="12m">12 Meses</option>
          <option value="24m">24 Meses</option>
          <option value="60m">5 Anos</option>
        </select>
      </label>
      <label>
        Foco
        <select value={patrimonyTypeMetricValue} onChange={(e) => setPatrimonyTypeMetric(e.target.value)}>
          <option value="net">Líquido</option>
          <option value="value">Patrimônio</option>
          <option value="pnl">Aberto</option>
        </select>
      </label>
    </div>
  )

  return (
    <section className="charts-terminal-page">
      <header className="card charts-terminal-hero">
        <div className="charts-terminal-hero-top">
          <div>
            <small className="charts-terminal-eyebrow">Analytics terminal</small>
            <h1>Graficos</h1>
            <p className="subtitle">Comparativos, alocação, benchmark e ledgers históricos em leitura operacional.</p>
          </div>
          <div className="dashboard-hero-actions">
            <button type="button" className="icon-btn" onClick={() => setShowLayoutControls((current) => !current)}>
              {showLayoutControls ? 'Fechar layout' : 'Personalizar paineis'}
            </button>
            <small>{refreshingCore ? loadingMessage : `${hiddenBlocksValue.length} painel(is) oculto(s)`}</small>
          </div>
        </div>
        <div className="charts-terminal-meta-strip">
          {analyticsHighlights.map((item) => (
            <div key={item.label} className="charts-terminal-meta-item">
              <span>{item.label}</span>
              <strong>{item.value}</strong>
              <small>{item.meta}</small>
            </div>
          ))}
        </div>
        <nav className="charts-terminal-nav" aria-label="Navegação dos gráficos">
          {sectionLinks.map((item) => (
            <a key={item.href} href={item.href} className="charts-terminal-nav-link">
              {item.label}
            </a>
          ))}
        </nav>
      </header>

      {showLayoutControls ? (
        <section className="card dashboard-customizer charts-visibility-panel">
          <div className="dashboard-customizer-head">
            <div>
              <small className="dashboard-customizer-eyebrow">Workspace controls</small>
              <h2>Layout dos painéis</h2>
              <p>As preferências de filtros e visibilidade ficam salvas no navegador desta máquina.</p>
            </div>
            <div className="dashboard-pref-actions">
              <button type="button" className="icon-btn" onClick={showAllBlocks} disabled={hiddenBlocksValue.length === 0}>
                Mostrar todos
              </button>
            </div>
          </div>
          <div className="dashboard-customizer-grid">
            <div className="dashboard-customizer-section">
              <h3>Painéis e tabelas</h3>
              <div className="dashboard-pref-list">
                {blockOptions.map((item) => {
                  const visible = isVisible(item.key)
                  return (
                    <div key={item.key} className={`dashboard-pref-row ${visible ? '' : 'muted'}`.trim()}>
                      <div>
                        <strong>{item.label}</strong>
                        <small>{item.description}</small>
                      </div>
                      <button
                        type="button"
                        className="icon-btn"
                        onClick={() => toggleBlockVisibility(item.key)}
                      >
                        {visible ? 'Ocultar' : 'Mostrar'}
                      </button>
                    </div>
                  )
                })}
              </div>
            </div>
          </div>
        </section>
      ) : null}

      <div className="charts-feature-grid">
        {isVisible('benchmark') ? (
          <ChartPanel
            title="Rentabilidade comparada com indices"
            subtitle="Painel principal para leitura relativa contra benchmarks."
            className="chart-card-feature chart-card-benchmark"
            controls={(
              <div className="inline-filters chart-inline-controls">
                <label>
                  Periodo benchmark
                  <select value={rangeValue} onChange={(e) => setRange(e.target.value)}>
                    <option value="6m">6 Meses</option>
                    <option value="12m">12 Meses</option>
                    <option value="24m">24 Meses</option>
                    <option value="60m">5 Anos</option>
                  </select>
                </label>
                <label>
                  Escopo benchmark
                  <select value={scopeValue} onChange={(e) => setScope(e.target.value)}>
                    <option value="all">Todos os tipos</option>
                    <option value="br">Somente BR</option>
                    <option value="us">Somente US</option>
                    <option value="fiis">Somente FIIs</option>
                    <option value="crypto">Somente Cripto</option>
                  </select>
                </label>
              </div>
            )}
            actions={makeChartActions('benchmark', 'benchmark-comparado', benchmarkData)}
          >
            {(loadingBenchmark || refreshingBenchmark) && <p className="subtitle">Atualizando benchmark...</p>}
            {benchmarkError && <p className="error">{benchmarkError}</p>}
            <div className="chart-canvas-wrap">
              <Line ref={registerChartRef('benchmark')} data={benchmarkData} options={benchmarkOptions} />
            </div>
          </ChartPanel>
        ) : null}

        {isVisible('monthlyIncome') ? (
          <ChartPanel
            title="Proventos mes a mes"
            subtitle="Fluxo mensal separado entre FIIs e ações."
            className="chart-card-feature"
            actions={makeChartActions('monthlyIncome', 'proventos-mensais', monthlyIncomeData)}
          >
            <div className="chart-canvas-wrap">
              <Line ref={registerChartRef('monthlyIncome')} data={monthlyIncomeData} options={incomeLineOptions} />
            </div>
          </ChartPanel>
        ) : null}
      </div>

      {isVisible('patrimonyByType') ? (
        <DataSection
          id="patrimonio"
          title="Evolução patrimonial por tipo"
          subtitle="Leitura mensal separando renda variável e renda fixa, com foco em patrimônio ou resultado aberto."
          controls={(
            <div className="chart-section-toolbar">
              {patrimonyByTypeControls}
              <div className="dashboard-pref-actions">
                <button type="button" className="icon-btn" onClick={() => exportChartDataAsCsv('patrimonio-por-tipo.csv', patrimonyByTypeData)}>
                  CSV
                </button>
                <button
                  type="button"
                  className="icon-btn"
                  onClick={() => exportChartAsImage('patrimonio-por-tipo.png', chartRefs.current.patrimonyByType)}
                >
                  PNG
                </button>
                <button type="button" className="icon-btn" onClick={() => toggleBlockVisibility('patrimonyByType')}>
                  Ocultar
                </button>
              </div>
            </div>
          )}
        >
          {(loadingPatrimonyByType || refreshingPatrimonyByType) && <p className="subtitle">Atualizando evolução patrimonial...</p>}
          {patrimonyByTypeError && <p className="error">{patrimonyByTypeError}</p>}
          {patrimonyByTypeData.labels.length > 0 && patrimonyByTypeData.datasets.length > 0 ? (
            <ChartPanel
              title={patrimonyTypeMetricValue === 'value'
                ? 'Patrimônio por tipo de ativo'
                : patrimonyTypeMetricValue === 'pnl'
                  ? 'Resultado em aberto por tipo de ativo'
                  : 'Resultado líquido por tipo de ativo'}
              subtitle={patrimonyTypeMetricValue === 'value'
                ? `${hoveredPatrimonySeriesLabel ? `Destaque: ${hoveredPatrimonySeriesLabel}. ` : ''}Patrimônio estimado por série. Linhas sólidas = renda variável; tracejadas = renda fixa.`
                : patrimonyTypeMetricValue === 'pnl'
                  ? `${hoveredPatrimonySeriesLabel ? `Destaque: ${hoveredPatrimonySeriesLabel}. ` : ''}Áreas empilhadas mostrando o resultado em aberto positivo e negativo por tipo, apenas para posições ainda abertas.`
                  : `${hoveredPatrimonySeriesLabel ? `Destaque: ${hoveredPatrimonySeriesLabel}. ` : ''}Resultado líquido acumulado por série, incluindo realizado e posição aberta. Linhas sólidas = renda variável; tracejadas = renda fixa.`}
              actions={null}
            >
              <div className="chart-canvas-wrap">
                <Line
                  ref={registerChartRef('patrimonyByType')}
                  data={patrimonyByTypeData}
                  options={isPatrimonyOpenMetric ? patrimonyOpenAreaOptions : patrimonyLineOptions}
                />
              </div>
            </ChartPanel>
          ) : (
            <p className="subtitle">Sem dados suficientes para montar a evolução patrimonial por tipo no período selecionado.</p>
          )}
        </DataSection>
      ) : null}

      {(isVisible('classesPie')
        || isVisible('categoryDonut')
        || isVisible('resultCategory')
        || isVisible('cardsBar')
        || isVisible('topAssets')
        || allocationCharts.some((item) => isVisible(item.key))) ? (
        <DataSection
          id="composicao"
          title="Composição da carteira"
          subtitle="Leitura macro de alocação, mix de classes e concentração."
        >
          <div className="charts-grid charts-grid-analytics">
            {isVisible('classesPie') ? (
              <ChartPanel
                title="Renda Variavel x Renda Fixa"
                subtitle={`Soma consolidada: ${brl(classesTotal)}`}
                actions={makeChartActions('classesPie', 'renda-variavel-vs-fixa', classesData)}
              >
                <div className="chart-canvas-wrap">
                  <Pie ref={registerChartRef('classesPie')} data={classesData} options={pieValueInsideOptions} />
                </div>
              </ChartPanel>
            ) : null}

            {isVisible('categoryDonut') ? (
              <ChartPanel
                title="Distribuicao por tipo de ativo"
                subtitle="Peso relativo por classe dentro da carteira."
                actions={makeChartActions('categoryDonut', 'distribuicao-por-tipo', categoryData)}
              >
                <div className="chart-canvas-wrap">
                  <Doughnut ref={registerChartRef('categoryDonut')} data={categoryData} options={categoryDonutOptions} />
                </div>
              </ChartPanel>
            ) : null}

            {isVisible('resultCategory') ? (
              <ChartPanel
                title="Resultado por categoria"
                subtitle="Comparativo de ganho ou perda por agrupamento."
                actions={makeChartActions('resultCategory', 'resultado-por-categoria', resultByCategoryData)}
              >
                <div className="chart-canvas-wrap">
                  <Bar ref={registerChartRef('resultCategory')} data={resultByCategoryData} options={resultByCategoryOptions} />
                </div>
              </ChartPanel>
            ) : null}

            {isVisible('cardsBar') ? (
              <ChartPanel
                title="Consolidado da carteira"
                subtitle="Resumo absoluto de valor por grande bloco."
                actions={makeChartActions('cardsBar', 'consolidado-carteira', cardsData)}
              >
                <div className="chart-canvas-wrap">
                  <Bar ref={registerChartRef('cardsBar')} data={cardsData} options={consolidatedOptions} />
                </div>
              </ChartPanel>
            ) : null}

            {isVisible('topAssets') ? (
              <ChartPanel
                title="Top 10 ativos por valor em carteira"
                subtitle="Ranking de concentração por patrimônio atual."
                actions={makeChartActions('topAssets', 'top-ativos-por-valor', topAssetsData)}
              >
                <div className="chart-canvas-wrap">
                  <Bar ref={registerChartRef('topAssets')} data={topAssetsData} options={barMoneyOptions} />
                </div>
              </ChartPanel>
            ) : null}

            {allocationCharts.map((item) => (
              isVisible(item.key) ? (
                <ChartPanel
                  key={item.key}
                  title={item.title}
                  subtitle="Distribuição interna por agrupamento em barra horizontal."
                  actions={makeChartActions(item.key, `${item.key}-alocacao`, item.data)}
                >
                  <div className="chart-canvas-wrap">
                    <Bar ref={registerChartRef(item.key)} data={item.data} options={allocationBarOptionsFor(item)} />
                  </div>
                </ChartPanel>
              ) : null
            ))}
          </div>
        </DataSection>
      ) : null}

      {(isVisible('fixedInvestment') || isVisible('fixedDistributor') || isVisible('fixedIssuer')) ? (
        <DataSection
          id="renda-fixa"
          title="Renda fixa"
          subtitle="Patrimônio, distribuidores e emissores em leitura separada."
        >
          <div className="charts-grid charts-grid-fixed">
            {isVisible('fixedInvestment') ? (
              <ChartPanel
                title="Aplicação Renda Fixa"
                subtitle="Volume aplicado por recorte do book fixo."
                actions={makeChartActions('fixedInvestment', 'investimento-renda-fixa', fixedInvestmentData)}
              >
                <div className="chart-canvas-wrap">
                  <Bar ref={registerChartRef('fixedInvestment')} data={fixedInvestmentData} options={barMoneyOptions} />
                </div>
              </ChartPanel>
            ) : null}

            {isVisible('fixedDistributor') ? (
              <ChartPanel
                title="Distribuidor Renda Fixa"
                subtitle="Quem está concentrando a distribuição da carteira."
                actions={makeChartActions('fixedDistributor', 'distribuidor-renda-fixa', fixedDistributorData)}
              >
                <div className="chart-canvas-wrap">
                  <Bar ref={registerChartRef('fixedDistributor')} data={fixedDistributorData} options={barMoneyOptions} />
                </div>
              </ChartPanel>
            ) : null}

            {isVisible('fixedIssuer') ? (
              <ChartPanel
                title="Emissor"
                subtitle="Separação entre principal investido e rendimento por emissor."
                actions={makeChartActions('fixedIssuer', 'emissor-renda-fixa', fixedIssuerData)}
              >
                <div className="chart-canvas-wrap">
                  <Bar ref={registerChartRef('fixedIssuer')} data={fixedIssuerData} options={fixedIssuerOptions} />
                </div>
              </ChartPanel>
            ) : null}
          </div>
        </DataSection>
      ) : null}

      {isVisible('ledgerAnual') ? (
        <DataSection
          id="ledger-anual"
          title="Ledger anual"
          subtitle="Tabela comparativa por ano com investidos e proventos mês a mês."
          controls={makeTableControls(annualLedgerHeaders, annualLedgerRows, 'ledger-anual', 'ledgerAnual', annualFilterControls)}
        >
          <div className="table-wrap">
            <table className="annual-month-table">
              <thead>
                <tr>
                  <th />
                  {annualSummary.years.map((year) => (
                    <th key={year.label}>
                      {year.label}
                      {annualMetricsValue.includes('invested') && <div>Inv: {brl(year.invested_total)}</div>}
                      {annualMetricsValue.includes('incomes') && <div>Prov: {brl(year.incomes_total)}</div>}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {annualSummary.months.map((month, monthIdx) => (
                  <tr key={month}>
                    <th>{month.toUpperCase()}</th>
                    {annualSummary.years.map((year) => (
                      <td key={`${year.label}-${month}`}>
                        {annualMetricsValue.includes('invested') && <div>Inv: {brl(year.invested_values?.[monthIdx])}</div>}
                        {annualMetricsValue.includes('incomes') && <div>Prov: {brl(year.incomes_values?.[monthIdx])}</div>}
                      </td>
                    ))}
                  </tr>
                ))}
                {annualSummary.years.length === 0 && (
                  <tr>
                    <td colSpan={2}>Sem dados para tabela anual.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </DataSection>
      ) : null}

      {isVisible('tickerLedger') ? (
        <DataSection
          id="ticker-ledger"
          title="Resumo mensal por ticker"
          subtitle="Ledger por ativo para leitura de aportes e proventos por mês."
          controls={makeTableControls(tickerLedgerHeaders, tickerLedgerRows, 'ticker-ledger', 'tickerLedger', tickerFilterControls)}
        >
          {(loadingTicker || refreshingTicker) && <p className="subtitle">Atualizando resumo por ticker...</p>}
          {tickerError && <p className="error">{tickerError}</p>}
          <div className="table-wrap">
            <table className="ticker-month-table">
              <thead>
                <tr>
                  <th rowSpan={2} className="ticker-month-sticky">
                    <button type="button" className="th-sort-btn" onClick={() => onToggleTickerSort('ticker')}>
                      {tickerSortLabel('Ticker', 'ticker')}
                    </button>
                  </th>
                  {monthlyTickerSummary.months.map((month) => (
                    <th key={month.key} colSpan={2}>{month.label}</th>
                  ))}
                </tr>
                <tr>
                  {monthlyTickerSummary.months.map((month) => (
                    <Fragment key={`sub-${month.key}`}>
                      <th>investidos</th>
                      <th>proventos</th>
                    </Fragment>
                  ))}
                </tr>
              </thead>
              <tbody>
                {monthlyTickerSummary.totals.length > 0 && (
                  <tr className="ticker-month-total-row">
                    <td className="ticker-month-sticky">
                      <strong>Total</strong>
                    </td>
                    {monthlyTickerSummary.totals.map((total) => (
                      <Fragment key={`totals-${total.month_key}`}>
                        <td><strong>{brl(total.invested)}</strong></td>
                        <td><strong>{brl(total.incomes)}</strong></td>
                      </Fragment>
                    ))}
                  </tr>
                )}
                {sortedTickerRows.map((row) => (
                  <tr key={row.ticker}>
                    <td className="ticker-month-sticky">
                      <strong>{row.ticker}</strong>
                    </td>
                    {monthlyTickerSummary.months.map((month) => {
                      const values = row.months?.[month.key] || { invested: 0, incomes: 0 }
                      return (
                        <Fragment key={`${row.ticker}-${month.key}`}>
                          <td>{brl(values.invested)}</td>
                          <td>{brl(values.incomes)}</td>
                        </Fragment>
                      )
                    })}
                  </tr>
                ))}
                {sortedTickerRows.length === 0 && (
                  <tr>
                    <td colSpan={Math.max(1, 1 + (monthlyTickerSummary.months.length * 2))}>
                      Sem dados por ticker para o periodo selecionado.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </DataSection>
      ) : null}

      {isVisible('classLedger') ? (
        <DataSection
          id="classe-ledger"
          title="Resumo mensal por classe"
          subtitle="Investidos e proventos agregados por bloco da carteira."
          controls={makeTableControls(classLedgerHeaders, classLedgerRows, 'classe-ledger', 'classLedger')}
        >
          <div className="table-wrap">
            <table className="monthly-table">
              <thead>
                <tr>
                  <th rowSpan={2}>data</th>
                  <th colSpan={2}>BR</th>
                  <th colSpan={2}>FII</th>
                  <th colSpan={2}>FIXA</th>
                  <th colSpan={2}>Cripto</th>
                  <th colSpan={2}>TOTAL</th>
                </tr>
                <tr>
                  <th>investidos</th><th>proventos</th>
                  <th>investidos</th><th>proventos</th>
                  <th>investidos</th><th>proventos</th>
                  <th>investidos</th><th>proventos</th>
                  <th>investidos</th><th>proventos</th>
                </tr>
              </thead>
              <tbody>
                {monthlyClassSummary.map((row) => (
                  <tr key={row.label}>
                    <td>{row.label}</td>
                    <td>{brl(row.br_invested)}</td><td>{brl(row.br_incomes)}</td>
                    <td>{brl(row.fii_invested)}</td><td>{brl(row.fii_incomes)}</td>
                    <td>{brl(row.fixa_invested)}</td><td>{brl(row.fixa_incomes)}</td>
                    <td>{brl(row.cripto_invested)}</td><td>{brl(row.cripto_incomes)}</td>
                    <td><strong>{brl(row.total_invested)}</strong></td><td><strong>{brl(row.total_incomes)}</strong></td>
                  </tr>
                ))}
                {monthlyClassSummary.length === 0 && (
                  <tr>
                    <td colSpan={11}>Sem dados mensais para o periodo selecionado.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </DataSection>
      ) : null}
    </section>
  )
}

export default ChartsPage
