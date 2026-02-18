import { useEffect, useMemo, useRef, useState } from 'react'
import { Chart as ChartJS } from 'chart.js/auto'
import { Bar, Doughnut, Line, Pie } from 'react-chartjs-2'
import ChartDataLabels from 'chartjs-plugin-datalabels'
import { apiGet } from '../api'

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

function ChartsPage({ selectedPortfolioIds }) {
  const [payload, setPayload] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [range, setRange] = useState('12m')
  const [scope, setScope] = useState('all')
  const [annualMetrics, setAnnualMetrics] = useState(['invested', 'incomes'])
  const [annualCategories, setAnnualCategories] = useState(['br', 'us', 'fii', 'cripto', 'fixa'])
  const [loadingMessage, setLoadingMessage] = useState('Atualizando graficos...')
  const previousPortfoliosRef = useRef('')

  useEffect(() => {
    let active = true
    const currentPortfolioKey = JSON.stringify(selectedPortfolioIds || [])
    const portfolioChanged = previousPortfoliosRef.current !== currentPortfolioKey
    previousPortfoliosRef.current = currentPortfolioKey
    setLoading(true)
    setLoadingMessage(portfolioChanged ? 'Lendo carteiras selecionadas...' : 'Atualizando graficos...')
    setError('')
    ;(async () => {
      try {
        const data = await apiGet('/api/charts/dashboard', {
          portfolio_id: selectedPortfolioIds,
          range,
          scope,
        })
        if (!active) return
        setPayload(data)
      } catch (err) {
        if (!active) return
        setError(err.message)
      } finally {
        if (active) setLoading(false)
      }
    })()
    return () => {
      active = false
    }
  }, [selectedPortfolioIds, range, scope])

  const onToggleMetric = (metric) => {
    setAnnualMetrics((current) => {
      const has = current.includes(metric)
      if (has) return current.filter((item) => item !== metric)
      return [...current, metric]
    })
  }

  const onToggleCategory = (category) => {
    setAnnualCategories((current) => {
      const has = current.includes(category)
      if (has) {
        const next = current.filter((item) => item !== category)
        return next.length > 0 ? next : current
      }
      return [...current, category]
    })
  }

  const moneyScale = useMemo(() => ({
    ticks: { callback: (value) => brl(value) },
  }), [])

  const percentScale = useMemo(() => ({
    ticks: { callback: (value) => `${Number(value || 0).toFixed(2)}%` },
  }), [])

  const annualSummary = useMemo(() => {
    const rows = payload?.monthly_class_summary || []
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
      if (annualCategories.includes('br')) {
        invested += Number(row.br_invested || 0)
        incomes += Number(row.br_incomes || 0)
      }
      if (annualCategories.includes('us')) {
        invested += Number(row.us_invested || 0)
        incomes += Number(row.us_incomes || 0)
      }
      if (annualCategories.includes('fii')) {
        invested += Number(row.fii_invested || 0)
        incomes += Number(row.fii_incomes || 0)
      }
      if (annualCategories.includes('cripto')) {
        invested += Number(row.cripto_invested || 0)
        incomes += Number(row.cripto_incomes || 0)
      }
      if (annualCategories.includes('fixa')) {
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
  }, [payload, annualCategories])

  if (loading && !payload) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>
  if (!payload) return <p>Sem dados.</p>

  const categoryChart = payload.category_chart || { labels: [], values: [] }
  const topAssetsChart = payload.top_assets_chart || { labels: [], values: [] }
  const allocationByGroupCharts = payload.allocation_by_group_charts || []
  const cardsChart = payload.cards_chart || { labels: [], values: [] }
  const resultByCategoryChart = payload.result_by_category_chart || { labels: [], values: [] }
  const classesChart = payload.classes_chart || { labels: [], values: [] }
  const fixedInvestmentChart = payload.fixed_income_investment_chart || { labels: [], values: [] }
  const fixedDistributorChart = payload.fixed_income_distributor_chart || { labels: [], values: [] }
  const fixedIssuerChart = payload.fixed_income_issuer_chart || { labels: [], investment_values: [], income_values: [] }
  const monthlyIncomeChart = payload.monthly_income_chart || { labels: [], fii_values: [], acoes_values: [] }
  const benchmarkChart = payload.benchmark_chart || { labels: [], datasets: [] }
  const monthlyClassSummary = payload.monthly_class_summary || []

  const classesTotal = (classesChart.values || []).reduce((acc, value) => acc + Number(value || 0), 0)
  const categoryTotal = (categoryChart.values || []).reduce((acc, value) => acc + Number(value || 0), 0)
  const donutColors = ['#0f7b6c', '#2d6cdf', '#ea8a2f', '#6f42c1']

  const benchmarkData = {
    labels: benchmarkChart.labels || [],
    datasets: (benchmarkChart.datasets || []).map((series, idx) => ({
      label: series.label,
      data: series.values,
      borderColor: series.color,
      backgroundColor: idx === 0 ? 'rgba(111, 143, 231, 0.12)' : 'transparent',
      borderWidth: idx === 0 ? 2.2 : 1.7,
      fill: idx === 0,
      tension: 0.22,
      pointRadius: 2,
    })),
  }

  const classesData = {
    labels: classesChart.labels || [],
    datasets: [{ label: 'Valor', data: classesChart.values || [], backgroundColor: ['#2d6cdf', '#0f7b6c'] }],
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
      backgroundColor: (resultByCategoryChart.values || []).map((v) => (Number(v || 0) >= 0 ? '#3f7edb' : '#d74a4a')),
    }],
  }

  const cardsData = {
    labels: cardsChart.labels || [],
    datasets: [{
      label: 'Valor',
      data: cardsChart.values || [],
      backgroundColor: ['#0f7b6c', '#2d6cdf', '#f2a93b'],
    }],
  }

  const monthlyIncomeData = {
    labels: monthlyIncomeChart.labels || [],
    datasets: [
      {
        label: 'FIIs',
        data: monthlyIncomeChart.fii_values || [],
        borderColor: '#0f7b6c',
        backgroundColor: 'rgba(15, 123, 108, 0.15)',
        tension: 0.25,
      },
      {
        label: 'Acoes',
        data: monthlyIncomeChart.acoes_values || [],
        borderColor: '#2d6cdf',
        backgroundColor: 'rgba(45, 108, 223, 0.15)',
        tension: 0.25,
      },
    ],
  }

  const topAssetsData = {
    labels: topAssetsChart.labels || [],
    datasets: [{ label: 'Valor', data: topAssetsChart.values || [], backgroundColor: '#2d6cdf' }],
  }

  const fixedInvestmentData = {
    labels: fixedInvestmentChart.labels || [],
    datasets: [{ label: 'Valor aplicado', data: fixedInvestmentChart.values || [], backgroundColor: '#4b80db' }],
  }

  const fixedDistributorData = {
    labels: fixedDistributorChart.labels || [],
    datasets: [{ label: 'Valor aplicado', data: fixedDistributorChart.values || [], backgroundColor: '#4b80db' }],
  }

  const fixedIssuerData = {
    labels: fixedIssuerChart.labels || [],
    datasets: [
      { label: 'investimento', data: fixedIssuerChart.investment_values || [], backgroundColor: '#2f67cb' },
      { label: 'rendimento', data: fixedIssuerChart.income_values || [], backgroundColor: '#3f8a2f' },
    ],
  }

  const allocationCharts = allocationByGroupCharts.map((item, idx) => {
    const palette = ['#3498db', '#2ecc71', '#f1c40f', '#e67e22', '#e74c3c', '#9b59b6', '#1abc9c', '#34495e', '#ff7f50', '#6a89cc']
    const colors = (item.values || []).map((_, colorIdx) => palette[colorIdx % palette.length])
    return {
      key: `allocation-${idx}`,
      title: item.title,
      labels: item.labels || [],
      values: item.values || [],
      weights: item.weights || [],
      data: {
        labels: item.labels || [],
        datasets: [{ label: 'Patrimonio', data: item.values || [], backgroundColor: colors, borderWidth: 1 }],
      },
    }
  })

  const pieValueInsideOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      datalabels: {
        color: '#ffffff',
        anchor: 'center',
        align: 'center',
        formatter: (value) => brlCompact(value),
        font: { weight: '700', size: 11 },
      },
    },
  }

  const categoryDonutOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      datalabels: {
        color: '#2d3748',
        anchor: 'end',
        align: 'end',
        offset: 6,
        formatter: (value, ctx) => {
          const label = categoryData.labels?.[ctx.dataIndex] || ''
          const pct = categoryTotal > 0 ? (Number(value || 0) / categoryTotal) * 100 : 0
          return `${label} | ${brlCompact(value)} | ${pct.toFixed(2)}%`
        },
        font: { size: 9, weight: '600' },
      },
    },
  }

  const resultByCategoryOptions = {
    responsive: true,
    maintainAspectRatio: false,
    scales: { y: moneyScale },
    plugins: {
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
    responsive: true,
    maintainAspectRatio: false,
    scales: { y: moneyScale },
    plugins: {
      datalabels: {
        color: '#ffffff',
        anchor: 'center',
        align: 'center',
        formatter: (value) => brlCompact(value),
        font: { weight: '700', size: 11 },
      },
    },
  }

  const allocationOptionsFor = (chart) => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      datalabels: {
        color: '#2d3748',
        anchor: 'end',
        align: 'end',
        offset: 6,
        formatter: (_, ctx) => {
          const i = ctx.dataIndex
          const label = chart.labels?.[i] || ''
          const value = chart.values?.[i] || 0
          const weight = chart.weights?.[i] || 0
          return `${label} | ${brlCompact(value)} | ${Number(weight).toFixed(2)}%`
        },
        font: { size: 9, weight: '600' },
      },
    },
  })

  return (
    <section>
      <h1>Graficos</h1>
      {loading && <p className="subtitle">{loadingMessage}</p>}

      <div className="charts-grid">
        <article className="card chart-card">
          <h3>Renda Variavel x Renda Fixa</h3>
          <div className="chart-canvas-wrap">
            <Pie data={classesData} options={pieValueInsideOptions} />
          </div>
          <p className="subtitle">Soma: {brl(classesTotal)}</p>
        </article>

        <article className="card chart-card">
          <h3>Distribuicao por tipo de ativo</h3>
          <div className="chart-canvas-wrap">
            <Doughnut data={categoryData} options={categoryDonutOptions} />
          </div>
        </article>

        <article className="card chart-card">
          <h3>Resultado por categoria</h3>
          <div className="chart-canvas-wrap">
            <Bar data={resultByCategoryData} options={resultByCategoryOptions} />
          </div>
        </article>

        <article className="card chart-card">
          <h3>Consolidado da carteira</h3>
          <div className="chart-canvas-wrap">
            <Bar data={cardsData} options={consolidatedOptions} />
          </div>
        </article>

        <article className="card chart-card">
          <div className="chart-head-inline">
            <h3>Rentabilidade comparada com indices</h3>
            <div className="inline-filters">
              <label>
                Periodo benchmark
                <select value={range} onChange={(e) => setRange(e.target.value)}>
                  <option value="6m">6 Meses</option>
                  <option value="12m">12 Meses</option>
                  <option value="24m">24 Meses</option>
                  <option value="60m">5 Anos</option>
                </select>
              </label>
              <label>
                Escopo benchmark
                <select value={scope} onChange={(e) => setScope(e.target.value)}>
                  <option value="all">Todos os tipos</option>
                  <option value="br">Somente BR</option>
                  <option value="us">Somente US</option>
                  <option value="fiis">Somente FIIs</option>
                  <option value="crypto">Somente Cripto</option>
                </select>
              </label>
            </div>
          </div>
          <div className="chart-canvas-wrap">
            <Line data={benchmarkData} options={{ responsive: true, maintainAspectRatio: false, scales: { y: percentScale } }} />
          </div>
        </article>

        <article className="card chart-card">
          <h3>Proventos mes a mes (FIIs x Acoes)</h3>
          <div className="chart-canvas-wrap">
            <Line data={monthlyIncomeData} options={{ responsive: true, maintainAspectRatio: false, scales: { y: moneyScale } }} />
          </div>
        </article>

        <article className="card chart-card">
          <h3>Top 10 ativos por valor em carteira</h3>
          <div className="chart-canvas-wrap">
            <Bar data={topAssetsData} options={{ responsive: true, maintainAspectRatio: false, scales: { y: moneyScale } }} />
          </div>
        </article>

        {allocationCharts.map((item) => (
          <article key={item.key} className="card chart-card">
            <h3>{item.title}</h3>
            <div className="chart-canvas-wrap">
              <Doughnut data={item.data} options={allocationOptionsFor(item)} />
            </div>
          </article>
        ))}

        <article className="card chart-card">
          <h3>Investimento Renda Fixa</h3>
          <div className="chart-canvas-wrap">
            <Bar data={fixedInvestmentData} options={{ responsive: true, maintainAspectRatio: false, scales: { y: moneyScale } }} />
          </div>
        </article>

        <article className="card chart-card">
          <h3>Distribuidor Renda Fixa</h3>
          <div className="chart-canvas-wrap">
            <Bar data={fixedDistributorData} options={{ responsive: true, maintainAspectRatio: false, scales: { y: moneyScale } }} />
          </div>
        </article>

        <article className="card chart-card">
          <h3>Emissor</h3>
          <div className="chart-canvas-wrap">
            <Bar data={fixedIssuerData} options={{ responsive: true, maintainAspectRatio: false, scales: { y: moneyScale } }} />
          </div>
        </article>
      </div>

      <section id="investidos-ano">
        <h2 style={{ marginTop: 22 }}>Investidos por mes (ano)</h2>
        <div className="inline-filters inline-filters-checks">
          <label><input type="checkbox" checked={annualMetrics.includes('invested')} onChange={() => onToggleMetric('invested')} /> Investidos</label>
          <label><input type="checkbox" checked={annualMetrics.includes('incomes')} onChange={() => onToggleMetric('incomes')} /> Proventos</label>
          <label><input type="checkbox" checked={annualCategories.includes('br')} onChange={() => onToggleCategory('br')} /> BR</label>
          <label><input type="checkbox" checked={annualCategories.includes('us')} onChange={() => onToggleCategory('us')} /> US</label>
          <label><input type="checkbox" checked={annualCategories.includes('fii')} onChange={() => onToggleCategory('fii')} /> FIIs</label>
          <label><input type="checkbox" checked={annualCategories.includes('cripto')} onChange={() => onToggleCategory('cripto')} /> Cripto</label>
          <label><input type="checkbox" checked={annualCategories.includes('fixa')} onChange={() => onToggleCategory('fixa')} /> FIXA</label>
        </div>
        <div className="table-wrap">
          <table className="annual-month-table">
            <thead>
              <tr>
                <th />
                {annualSummary.years.map((year) => (
                  <th key={year.label}>
                    {year.label}
                    {annualMetrics.includes('invested') && <div>Inv: {brl(year.invested_total)}</div>}
                    {annualMetrics.includes('incomes') && <div>Prov: {brl(year.incomes_total)}</div>}
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
                      {annualMetrics.includes('invested') && <div>Inv: {brl(year.invested_values?.[monthIdx])}</div>}
                      {annualMetrics.includes('incomes') && <div>Prov: {brl(year.incomes_values?.[monthIdx])}</div>}
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
      </section>

      <section>
        <h2 style={{ marginTop: 22 }}>Resumo mensal por classe</h2>
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
      </section>
    </section>
  )
}

export default ChartsPage
