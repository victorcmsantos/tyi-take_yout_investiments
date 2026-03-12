import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import 'chart.js/auto'
import { Line } from 'react-chartjs-2'
import { apiGet } from '../api'

const CATEGORY_META = [
  { key: 'br_stocks', label: 'Acoes BR' },
  { key: 'us_stocks', label: 'Acoes US' },
  { key: 'crypto', label: 'Cripto' },
  { key: 'fiis', label: 'FIIs' },
]

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`
const marketStatusLabel = (item) => (item?.market_data?.is_stale ? 'Desatualizado' : 'Atualizado')
const shortText = (value, limit = 120) => {
  const text = String(value || '').trim()
  if (!text) return ''
  if (text.length <= limit) return text
  return `${text.slice(0, limit - 3).trim()}...`
}

const BUCKET_META = [
  { key: 'increase', label: 'Parecem para aumentar', tone: 'up' },
  { key: 'hold', label: 'Parecem para segurar', tone: 'neutral' },
  { key: 'reduce', label: 'Merecem reduzir', tone: 'down' },
]
const DAILY_RANGE_OPTIONS = [
  { key: '30d', label: '30d' },
  { key: '90d', label: '90d' },
  { key: '180d', label: '180d' },
  { key: '1y', label: '1 ano' },
]

function PortfolioPage({ selectedPortfolioIds }) {
  const [snapshot, setSnapshot] = useState(null)
  const [dailySeries, setDailySeries] = useState({ labels: [], values: [], included_tickers: [], missing_tickers: [] })
  const [dailyRange, setDailyRange] = useState('90d')
  const [dailyLoading, setDailyLoading] = useState(false)
  const [dailyError, setDailyError] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [sortBy, setSortBy] = useState('name')
  const [sortDir, setSortDir] = useState('asc')
  const [openGroups, setOpenGroups] = useState({})

  const toggleSort = (field) => {
    if (sortBy === field) {
      setSortDir((current) => (current === 'asc' ? 'desc' : 'asc'))
      return
    }
    setSortBy(field)
    setSortDir('desc')
  }

  const sortLabel = (label, field) => {
    if (sortBy !== field) return label
    return `${label} ${sortDir === 'asc' ? '↑' : '↓'}`
  }

  const toggleGroup = (key) => {
    setOpenGroups((current) => ({ ...current, [key]: !current[key] }))
  }

  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    ;(async () => {
      try {
        const data = await apiGet('/api/portfolio/snapshot', {
          portfolio_id: selectedPortfolioIds,
          sort_by: sortBy,
          sort_dir: sortDir,
        })
        if (!active) return
        setSnapshot(data)
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
  }, [selectedPortfolioIds, sortBy, sortDir])

  useEffect(() => {
    let active = true
    setDailyLoading(true)
    setDailyError('')
    ;(async () => {
      try {
        const data = await apiGet('/api/charts/variable-income-value-daily', {
          portfolio_id: selectedPortfolioIds,
          range: dailyRange,
        })
        if (!active) return
        setDailySeries(data || { labels: [], values: [], included_tickers: [], missing_tickers: [] })
      } catch (err) {
        if (!active) return
        setDailyError(err.message)
      } finally {
        if (active) setDailyLoading(false)
      }
    })()
    return () => {
      active = false
    }
  }, [selectedPortfolioIds, dailyRange])

  if (loading && !snapshot) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>
  if (!snapshot) return <p>Sem dados.</p>

  const tacticalSummary = snapshot.tactical_summary || {}
  const tacticalCounts = tacticalSummary.summary || {}
  const concentrationAlerts = tacticalSummary.concentration_alerts || []
  const thresholds = tacticalSummary.thresholds || {}
  const dailyLabels = Array.isArray(dailySeries?.labels) ? dailySeries.labels : []
  const dailyValues = Array.isArray(dailySeries?.values) ? dailySeries.values : []
  const hasDailyPoints = dailyValues.some((value) => Number.isFinite(Number(value)))
  const dailyChartData = {
    labels: dailyLabels,
    datasets: [
      {
        label: 'Valor estimado (R$)',
        data: dailyValues,
        borderColor: '#0f8a77',
        backgroundColor: 'rgba(15, 138, 119, 0.15)',
        fill: true,
        tension: 0.22,
        pointRadius: 0,
        pointHoverRadius: 3,
      },
    ],
  }
  const dailyChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      y: {
        ticks: {
          callback: (value) => brl(value),
        },
      },
    },
    plugins: {
      legend: { display: false },
    },
  }

  return (
    <section>
      <h1>Renda Variavel</h1>
      {loading && <p>Atualizando ordenacao...</p>}
      <div className="cards">
        <article className="card"><h3>Patrimonio</h3><p>{brl(snapshot.total_value)}</p></article>
        <article className="card"><h3>Investido</h3><p>{brl(snapshot.invested_value)}</p></article>
        <article className="card"><h3>Aberto (R$)</h3><p className={snapshot.open_pnl_value >= 0 ? 'up' : 'down'}>{brl(snapshot.open_pnl_value)}</p></article>
        <article className="card"><h3>Aberto (%)</h3><p className={snapshot.open_pnl_pct >= 0 ? 'up' : 'down'}>{snapshot.open_pnl_pct.toFixed(2)}%</p></article>
        <article className="card"><h3>Proventos mes atual</h3><p>{brl(snapshot.incomes_current_month)}</p></article>
        <article className="card"><h3>Proventos 3 meses</h3><p>{brl(snapshot.incomes_3m)}</p></article>
        <article className="card"><h3>Proventos 12 meses</h3><p>{brl(snapshot.incomes_12m)}</p></article>
        <article className="card"><h3>Proventos total</h3><p>{brl(snapshot.total_incomes)}</p></article>
      </div>

      <article className="card chart-card">
        <div className="chart-head-inline">
          <div>
            <h3>Valor da renda variavel (dia a dia)</h3>
            <p className="subtitle">Serie estimada em BRL usando cotacoes historicas e composicao atual da carteira.</p>
          </div>
          <label>
            Periodo
            <select value={dailyRange} onChange={(event) => setDailyRange(String(event.target.value || '90d'))}>
              {DAILY_RANGE_OPTIONS.map((item) => (
                <option key={item.key} value={item.key}>{item.label}</option>
              ))}
            </select>
          </label>
        </div>
        {dailyLoading && <p className="subtitle">Atualizando grafico...</p>}
        {!dailyLoading && dailyError && <p className="error">{dailyError}</p>}
        {!dailyLoading && !dailyError && hasDailyPoints && (
          <div className="chart-canvas-wrap">
            <Line data={dailyChartData} options={dailyChartOptions} />
          </div>
        )}
        {!dailyLoading && !dailyError && !hasDailyPoints && (
          <p className="subtitle">Sem historico diario suficiente para montar o grafico no periodo selecionado.</p>
        )}
        <small>
          Ativos com historico: {(dailySeries?.included_tickers || []).length}
          {(dailySeries?.missing_tickers || []).length > 0 ? ` | Sem historico: ${(dailySeries?.missing_tickers || []).length}` : ''}
        </small>
      </article>

      <article className="card detail-card portfolio-tactical-card">
        <div className="analysis-head">
          <div>
            <h3>Resumo da carteira inteira</h3>
            <p className="subtitle">Leitura consolidada das posições com base no OpenClaw, preço atual, preço médio e peso na carteira.</p>
          </div>
        </div>

        <div className="analysis-metrics">
          <div className="analysis-metric">
            <span className="analysis-label">Aumentar</span>
            <strong className="up">{Number(tacticalCounts.increase_count || 0)}</strong>
          </div>
          <div className="analysis-metric">
            <span className="analysis-label">Segurar</span>
            <strong>{Number(tacticalCounts.hold_count || 0)}</strong>
          </div>
          <div className="analysis-metric">
            <span className="analysis-label">Reduzir</span>
            <strong className="down">{Number(tacticalCounts.reduce_count || 0)}</strong>
          </div>
        </div>

        <div className="portfolio-tactical-grid">
          {BUCKET_META.map((bucket) => {
            const items = tacticalSummary[bucket.key] || []
            return (
              <section key={bucket.key} className="portfolio-tactical-section">
                <div className="portfolio-tactical-head">
                  <span className={`analysis-pill ${bucket.tone}`}>{bucket.label}</span>
                </div>
                {items.length > 0 ? (
                  <div className="portfolio-tactical-list">
                    {items.slice(0, 5).map((item) => (
                      <article key={`${bucket.key}-${item.ticker}`} className="portfolio-tactical-item">
                        <div className="portfolio-tactical-item-head">
                          <Link to={`/ativo/${item.ticker}`}>{item.ticker}</Link>
                          <span className="meta-chip">{Number(item.weight || 0).toFixed(2)}%</span>
                        </div>
                        <p className="portfolio-tactical-item-title">{item.name}</p>
                        <p className="portfolio-tactical-item-meta">
                          Humor: <strong className={item.mood_key === 'positive' ? 'up' : (item.mood_key === 'negative' ? 'down' : '')}>{item.mood_label}</strong>
                          {' · '}
                          Sinal: <strong>{item.structured_action_label}</strong>
                        </p>
                        <p className="portfolio-tactical-item-meta">
                          Gap vs medio: <strong className={Number(item.price_gap_pct || 0) >= 0 ? 'up' : 'down'}>{Number(item.price_gap_pct || 0).toFixed(2)}%</strong>
                          {' · '}
                          Aberto: <strong className={Number(item.open_pnl_pct || 0) >= 0 ? 'up' : 'down'}>{Number(item.open_pnl_pct || 0).toFixed(2)}%</strong>
                        </p>
                        <p className="subtitle portfolio-tactical-item-rationale">{shortText(item.rationale)}</p>
                      </article>
                    ))}
                  </div>
                ) : (
                  <p className="subtitle">Nenhum ativo nesta faixa agora.</p>
                )}
              </section>
            )
          })}

          <section className="portfolio-tactical-section">
            <div className="portfolio-tactical-head">
              <span className="analysis-pill down">Concentracao excessiva</span>
            </div>
            {concentrationAlerts.length > 0 ? (
              <div className="portfolio-tactical-list">
                {concentrationAlerts.map((alert, idx) => (
                  <article key={`${alert.kind}-${alert.ticker || alert.category || idx}`} className="portfolio-tactical-item">
                    <div className="portfolio-tactical-item-head">
                      {alert.ticker ? <Link to={`/ativo/${alert.ticker}`}>{alert.ticker}</Link> : <strong>{alert.label}</strong>}
                      <span className="meta-chip">{Number(alert.weight || 0).toFixed(2)}%</span>
                    </div>
                    {!alert.ticker && <p className="portfolio-tactical-item-title">{alert.label}</p>}
                    <p className="subtitle portfolio-tactical-item-rationale">{alert.detail}</p>
                  </article>
                ))}
              </div>
            ) : (
              <p className="subtitle">
                Sem alerta forte de concentracao usando os limites atuais de {Number(thresholds.single_position_weight_pct || 0).toFixed(0)}% por ativo e {Number(thresholds.category_weight_pct || 0).toFixed(0)}% por classe.
              </p>
            )}
          </section>
        </div>
      </article>

      <div className="accordion-wrap">
        {CATEGORY_META.map((meta) => {
          const items = snapshot.grouped_positions?.[meta.key] || []
          const summary = snapshot.group_summaries?.[meta.key] || {}
          const totalValue = Number(summary.total_value || 0)
          const groupWeight = snapshot.total_value > 0 ? (totalValue / snapshot.total_value) * 100 : 0
          const isOpen = !!openGroups[meta.key]

          return (
            <section key={meta.key} className="asset-group">
              <button
                type="button"
                className="asset-group-summary-btn"
                onClick={() => toggleGroup(meta.key)}
              >
                <div className="asset-group-summary">
                  <div>
                    <strong>{meta.label}</strong>
                    <small>{items.length} ativo(s)</small>
                  </div>
                  <div className="asset-group-metrics">
                    <div className="metric-item">
                      <span className="metric-label">Valor total</span>
                      <strong>{brl(totalValue)}</strong>
                    </div>
                    <div className="metric-item">
                      <span className="metric-label">Variacao</span>
                      <strong className={Number(summary.open_pnl_pct || 0) >= 0 ? 'up' : 'down'}>
                        {Number(summary.open_pnl_pct || 0).toFixed(2)}%
                      </strong>
                    </div>
                    <div className="metric-item">
                      <span className="metric-label">Aberto (R$)</span>
                      <strong className={Number(summary.open_pnl_value || 0) >= 0 ? 'up' : 'down'}>
                        {brl(summary.open_pnl_value)}
                      </strong>
                    </div>
                    <div className="metric-item">
                      <span className="metric-label">% na carteira</span>
                      <strong>{Number(groupWeight).toFixed(2)}%</strong>
                    </div>
                  </div>
                  <span className={`asset-group-chevron ${isOpen ? 'open' : ''}`}>⌄</span>
                </div>
              </button>

              {isOpen && (
                <>
                  <div className="cards">
                    <article className="card"><h3>Patrimonio</h3><p>{brl(summary.total_value)}</p></article>
                    <article className="card"><h3>Investido</h3><p>{brl(summary.invested_value)}</p></article>
                    <article className="card"><h3>Aberto (R$)</h3><p className={Number(summary.open_pnl_value || 0) >= 0 ? 'up' : 'down'}>{brl(summary.open_pnl_value)}</p></article>
                    <article className="card"><h3>Aberto (%)</h3><p className={Number(summary.open_pnl_pct || 0) >= 0 ? 'up' : 'down'}>{Number(summary.open_pnl_pct || 0).toFixed(2)}%</p></article>
                    <article className="card"><h3>Proventos mes atual</h3><p>{brl(summary.incomes_current_month)}</p></article>
                    <article className="card"><h3>Proventos 3 meses</h3><p>{brl(summary.incomes_3m)}</p></article>
                    <article className="card"><h3>Proventos 12 meses</h3><p>{brl(summary.incomes_12m)}</p></article>
                    <article className="card"><h3>Proventos total</h3><p>{brl(summary.total_incomes)}</p></article>
                  </div>

                  <div className="table-wrap">
                    <table className="asset-table">
                      <thead>
                        <tr>
                          <th className="sticky-col sticky-col-ticker"><button type="button" className="th-sort-btn" onClick={() => toggleSort('ticker')}>{sortLabel('Ticker', 'ticker')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('name')}>{sortLabel('Nome', 'name')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('shares')}>{sortLabel('Qtd', 'shares')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('price')}>{sortLabel('Preco', 'price')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('avg_price')}>{sortLabel('Preco medio', 'avg_price')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('invested_value')}>{sortLabel('Investido', 'invested_value')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('value')}>{sortLabel('Total', 'value')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('total_incomes')}>{sortLabel('Proventos', 'total_incomes')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('open_pnl_value')}>{sortLabel('Aberto (R$)', 'open_pnl_value')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('open_pnl_pct')}>{sortLabel('Aberto (%)', 'open_pnl_pct')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('weight')}>{sortLabel('Peso', 'weight')}</button></th>
                        </tr>
                      </thead>
                      <tbody>
                        {items.map((item) => (
                          <tr key={`${meta.key}-${item.ticker}`}>
                            <td className="sticky-col sticky-col-ticker">
                              <div className="market-data-cell">
                                <Link to={`/ativo/${item.ticker}`}>{item.ticker}</Link>
                                <small className={item?.market_data?.is_stale ? 'market-data-badge stale' : 'market-data-badge live'}>
                                  {marketStatusLabel(item)}
                                </small>
                              </div>
                            </td>
                            <td>{item.name}</td>
                            <td>{Number(item.shares || 0).toFixed(4)}</td>
                            <td>{brl(item.price)}</td>
                            <td>{brl(item.avg_price)}</td>
                            <td>{brl(item.invested_value)}</td>
                            <td>{brl(item.value)}</td>
                            <td>{brl(item.total_incomes)}</td>
                            <td className={Number(item.open_pnl_value || 0) >= 0 ? 'up' : 'down'}>{brl(item.open_pnl_value)}</td>
                            <td className={Number(item.open_pnl_pct || 0) >= 0 ? 'up' : 'down'}>{Number(item.open_pnl_pct || 0).toFixed(2)}%</td>
                            <td>{Number(item.weight || 0).toFixed(2)}%</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </section>
          )
        })}
      </div>
    </section>
  )
}

export default PortfolioPage
