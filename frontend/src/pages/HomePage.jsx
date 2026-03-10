import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { apiGetCached } from '../api'

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`
const pct = (value) => `${Number(value || 0).toFixed(2)}%`
const money = (value, currency = 'BRL') => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  const code = String(currency || 'BRL').trim().toUpperCase() || 'BRL'
  try {
    return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: code }).format(num)
  } catch (_) {
    return `${code} ${num.toFixed(6)}`
  }
}
const dateBr = (value) => {
  const text = String(value || '').trim()
  if (!text) return '-'
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const [y, m, d] = text.split('-')
    return `${d}/${m}/${y}`
  }
  return text
}
const dateTimeBr = (value) => {
  const text = String(value || '').trim()
  if (!text) return '-'
  const iso = text.endsWith('Z') ? text : `${text}Z`
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return text
  return dt.toLocaleString('pt-BR', { hour12: false })
}
const secondsLabel = (value) => {
  const total = Math.max(Number(value || 0), 0)
  if (!Number.isFinite(total) || total <= 0) return '0s'
  const minutes = Math.floor(total / 60)
  const seconds = Math.floor(total % 60)
  if (minutes <= 0) return `${seconds}s`
  return `${minutes}m ${seconds}s`
}
const formatSyncLabel = (asset) => {
  const marketData = asset?.market_data || {}
  if (marketData.is_stale) {
    return 'Desatualizado'
  }
  if (marketData.updated_at) {
    return `Atualizado via ${(marketData.source || 'provider').toUpperCase()}`
  }
  return 'Sem sincronizacao'
}

function HomePage({ selectedPortfolioIds }) {
  const [assets, setAssets] = useState([])
  const [sectors, setSectors] = useState([])
  const [incomesByTicker, setIncomesByTicker] = useState({})
  const [incomesTotal, setIncomesTotal] = useState(0)
  const [upcomingIncomes, setUpcomingIncomes] = useState({ items: [], summary: { estimated_totals: {} } })
  const [syncHealth, setSyncHealth] = useState(null)
  const [syncHealthError, setSyncHealthError] = useState('')
  const [showHealthModal, setShowHealthModal] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [sortBy, setSortBy] = useState('name')
  const [sortDir, setSortDir] = useState('asc')

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

  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    setSyncHealth(null)
    setSyncHealthError('')
    setUpcomingIncomes({ items: [], summary: { estimated_totals: {} } })
    ;(async () => {
      try {
        const [assetsData, sectorsData, incomesData] = await Promise.all([
          apiGetCached('/api/assets', {}, { ttlMs: 15000, staleWhileRevalidate: true }),
          apiGetCached('/api/sectors', {}, { ttlMs: 20000, staleWhileRevalidate: true }),
          apiGetCached('/api/incomes', { portfolio_id: selectedPortfolioIds }, { ttlMs: 12000, staleWhileRevalidate: true }),
        ])
        if (!active) return
        const byTicker = incomesData.reduce((acc, income) => {
          const ticker = String(income.ticker || '').toUpperCase()
          if (!ticker) return acc
          acc[ticker] = (acc[ticker] || 0) + Number(income.amount || 0)
          return acc
        }, {})
        setAssets(assetsData)
        setSectors(sectorsData)
        setIncomesByTicker(byTicker)
        setIncomesTotal(Object.values(byTicker).reduce((acc, value) => acc + Number(value || 0), 0))
        setLoading(false)

        try {
          const upcomingData = await apiGetCached(
            '/api/incomes/upcoming',
            { portfolio_id: selectedPortfolioIds, limit: 24 },
            { ttlMs: 30000, staleWhileRevalidate: true },
          )
          if (!active) return
          setUpcomingIncomes(upcomingData || { items: [], summary: { estimated_totals: {} } })
        } catch (_) {
          if (!active) return
          setUpcomingIncomes({ items: [], summary: { estimated_totals: {} } })
        }

        try {
          const response = await fetch('/api/health', { credentials: 'same-origin' })
          const payload = await response.json().catch(() => ({}))
          if (!active) return
          if (payload?.ok && payload?.data) {
            setSyncHealth(payload.data)
            setSyncHealthError('')
          } else {
            setSyncHealth(null)
            setSyncHealthError('Saude indisponivel')
          }
        } catch (_) {
          if (!active) return
          setSyncHealth(null)
          setSyncHealthError('Saude indisponivel')
        }
      } catch (err) {
        if (!active) return
        setError(err.message)
        setLoading(false)
      }
    })()
    return () => {
      active = false
    }
  }, [selectedPortfolioIds])

  const highlights = assets.length > 0
    ? {
      highestDy: assets.reduce((best, asset) => (Number(asset.dy || 0) > Number(best.dy || 0) ? asset : best), assets[0]),
      highestGain: assets.reduce((best, asset) => (Number(asset.variation_day || 0) > Number(best.variation_day || 0) ? asset : best), assets[0]),
      largestCap: assets.reduce((best, asset) => (Number(asset.market_cap_bi || 0) > Number(best.market_cap_bi || 0) ? asset : best), assets[0]),
    }
    : null

  const sortedAssets = useMemo(() => {
    const toNumber = (value) => {
      const num = Number(value)
      return Number.isFinite(num) ? num : 0
    }
    const sorted = [...assets]
    sorted.sort((a, b) => {
      let left = null
      let right = null

      if (sortBy === 'incomes') {
        left = toNumber(incomesByTicker[a.ticker] || 0)
        right = toNumber(incomesByTicker[b.ticker] || 0)
      } else if (['ticker', 'name', 'sector'].includes(sortBy)) {
        left = String(a[sortBy] || '').toUpperCase()
        right = String(b[sortBy] || '').toUpperCase()
      } else {
        left = toNumber(a[sortBy])
        right = toNumber(b[sortBy])
      }

      if (left < right) return sortDir === 'asc' ? -1 : 1
      if (left > right) return sortDir === 'asc' ? 1 : -1
      return 0
    })
    return sorted
  }, [assets, incomesByTicker, sortBy, sortDir])

  const staleAssetsCount = useMemo(
    () => assets.filter((asset) => asset?.market_data?.is_stale).length,
    [assets],
  )
  const upcomingItems = Array.isArray(upcomingIncomes?.items) ? upcomingIncomes.items : []
  const upcomingSummary = upcomingIncomes?.summary || {}
  const upcomingEstimatedBrl = Number(upcomingSummary?.estimated_totals?.BRL || 0)
  const syncJobs = Array.isArray(syncHealth?.jobs) ? syncHealth.jobs : []
  const marketSyncJob = syncJobs.find((item) => item?.name === 'market_sync') || null
  const failedJobsCount = syncJobs.filter((item) => Number(item?.consecutive_failures || 0) > 0).length
  const providerCircuits = Array.isArray(syncHealth?.provider_circuits) ? syncHealth.provider_circuits : []
  const providerUsage = Array.isArray(syncHealth?.provider_usage) ? syncHealth.provider_usage : []
  const activeCircuits = providerCircuits.filter((item) => item?.active)
  const circuitLabel = activeCircuits.length > 0
    ? activeCircuits
      .map((item) => `${String(item.provider || '').toUpperCase()} (${secondsLabel(item.remaining_seconds)})`)
      .join(', ')
    : 'nenhum'
  const healthStatusLabel = syncHealth?.status === 'ok' ? 'OK' : 'ATENCAO'
  const healthTimeLabel = syncHealth?.time ? dateTimeBr(syncHealth.time) : '-'

  if (loading) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>

  return (
    <section>
      <h1>Acoes</h1>
      {staleAssetsCount > 0 && (
        <p className="notice-warn">
          {staleAssetsCount} ativo(s) exibem cotacao possivelmente antiga. Veja a coluna de preco para identificar quais.
        </p>
      )}

      {highlights && (
        <div className="cards">
          <article className="card">
            <h3>Maior dividend yield</h3>
            <p>{highlights.highestDy.ticker}</p>
            <small>{pct(highlights.highestDy.dy)} a.a.</small>
          </article>
          <article className="card">
            <h3>Maior alta do dia</h3>
            <p>{highlights.highestGain.ticker}</p>
            <small>{pct(highlights.highestGain.variation_day)}</small>
          </article>
          <article className="card">
            <h3>Maior valor de mercado</h3>
            <p>{highlights.largestCap.ticker}</p>
            <small>R$ {Number(highlights.largestCap.market_cap_bi || 0).toFixed(2)} bi</small>
          </article>
          <article className="card">
            <h3>Proventos totais</h3>
            <p>{brl(incomesTotal)}</p>
            <small>Carteiras selecionadas</small>
          </article>
          <article className="card">
            <h3>Proventos futuros</h3>
            <p>{upcomingItems.length}</p>
            <small>
              {upcomingEstimatedBrl > 0
                ? `Estimado BRL: ${brl(upcomingEstimatedBrl)}`
                : 'Sem valor estimado no momento'}
            </small>
          </article>
          <article className="card">
            <h3>Saude de sync</h3>
            <p>{healthStatusLabel}</p>
            <div className="card-health-lines">
              <small>Stale: {staleAssetsCount} | Falhas de jobs: {failedJobsCount}</small>
              <small>
                Ultimo sync mercado: {marketSyncJob?.last_success_at ? dateTimeBr(marketSyncJob.last_success_at) : '-'}
              </small>
              <small>Cooldown APIs: {circuitLabel}</small>
              {!!syncHealthError && <small>{syncHealthError}</small>}
              <button type="button" className="btn-primary health-details-trigger" onClick={() => setShowHealthModal(true)}>
                Ver detalhes
              </button>
            </div>
          </article>
        </div>
      )}

      {showHealthModal && (
        <div className="health-modal-backdrop" role="presentation" onClick={() => setShowHealthModal(false)}>
          <div
            className="health-modal"
            role="dialog"
            aria-modal="true"
            aria-label="Detalhes da saude de sync"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="health-modal-header">
              <h3>Detalhes da saude de sync</h3>
              <button type="button" className="btn-danger" onClick={() => setShowHealthModal(false)}>
                Fechar
              </button>
            </div>
            <div className="health-modal-summary">
              <div><strong>Status:</strong> {healthStatusLabel}</div>
              <div><strong>Horario:</strong> {healthTimeLabel}</div>
              <div><strong>Ativos stale:</strong> {staleAssetsCount}</div>
              <div><strong>Jobs com falha:</strong> {failedJobsCount}</div>
              <div><strong>Cooldowns ativos:</strong> {activeCircuits.length}</div>
            </div>
            {!!syncHealthError && <p className="notice-warn">{syncHealthError}</p>}

            <h4>Jobs</h4>
            <div className="table-wrap health-modal-table">
              <table>
                <thead>
                  <tr>
                    <th>Job</th>
                    <th>Enabled</th>
                    <th>Running</th>
                    <th>Stale</th>
                    <th>Falhas</th>
                    <th>Ultimo sucesso</th>
                    <th>Ultimo erro</th>
                    <th>Duracao (ms)</th>
                    <th>Intervalo (s)</th>
                    <th>Max age (s)</th>
                  </tr>
                </thead>
                <tbody>
                  {syncJobs.map((job) => (
                    <tr key={`job-${job.name}`}>
                      <td>{job.name}</td>
                      <td>{job.enabled ? 'sim' : 'nao'}</td>
                      <td>{job.running ? 'sim' : 'nao'}</td>
                      <td>{job.stale ? 'sim' : 'nao'}</td>
                      <td>{Number(job.consecutive_failures || 0)}</td>
                      <td>{job.last_success_at ? dateTimeBr(job.last_success_at) : '-'}</td>
                      <td>{job.last_error_at ? dateTimeBr(job.last_error_at) : '-'}</td>
                      <td>{job.last_duration_ms != null ? Number(job.last_duration_ms).toFixed(2) : '-'}</td>
                      <td>{Number(job.interval_seconds || 0)}</td>
                      <td>{Number(job.max_age_seconds || 0)}</td>
                    </tr>
                  ))}
                  {syncJobs.length === 0 && (
                    <tr>
                      <td colSpan={10}>Nenhum job reportado pelo health.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            <h4>Circuitos de API</h4>
            <div className="table-wrap health-modal-table">
              <table>
                <thead>
                  <tr>
                    <th>Provider</th>
                    <th>Ativo</th>
                    <th>Cooldown</th>
                    <th>Status code</th>
                    <th>Updated at</th>
                  </tr>
                </thead>
                <tbody>
                  {providerCircuits.map((item) => (
                    <tr key={`circuit-${item.provider}`}>
                      <td>{String(item.provider || '').toUpperCase()}</td>
                      <td>{item.active ? 'sim' : 'nao'}</td>
                      <td>{secondsLabel(item.remaining_seconds)}</td>
                      <td>{item.status_code ?? '-'}</td>
                      <td>{item.updated_at ? dateTimeBr(item.updated_at) : '-'}</td>
                    </tr>
                  ))}
                  {providerCircuits.length === 0 && (
                    <tr>
                      <td colSpan={5}>Nenhum circuito registrado.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            <h4>Orcamento de APIs</h4>
            <div className="table-wrap health-modal-table">
              <table>
                <thead>
                  <tr>
                    <th>Provider</th>
                    <th>Janela</th>
                    <th>Uso</th>
                    <th>Limite</th>
                    <th>Restante</th>
                    <th>Uso %</th>
                    <th>HTTP 429</th>
                  </tr>
                </thead>
                <tbody>
                  {providerUsage.flatMap((provider) => {
                    const windows = provider?.windows || {}
                    return Object.keys(windows).map((windowKey) => {
                      const win = windows[windowKey] || {}
                      return (
                        <tr key={`provider-${provider.provider}-${windowKey}`}>
                          <td>{String(provider.provider || '').toUpperCase()}</td>
                          <td>{windowKey}</td>
                          <td>{Number(win.request_count || 0)}</td>
                          <td>{Number(win.limit || 0)}</td>
                          <td>{win.remaining == null ? 'sem limite' : Number(win.remaining || 0)}</td>
                          <td>{win.usage_pct == null ? '-' : `${Number(win.usage_pct).toFixed(2)}%`}</td>
                          <td>{Number(win.status_429_count || 0)}</td>
                        </tr>
                      )
                    })
                  })}
                  {providerUsage.length === 0 && (
                    <tr>
                      <td colSpan={7}>Sem dados de orcamento de provider.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      <div className="table-wrap">
        <table className="asset-table">
          <thead>
            <tr>
              <th className="sticky-col sticky-col-ticker"><button type="button" className="th-sort-btn" onClick={() => toggleSort('ticker')}>{sortLabel('Ticker', 'ticker')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('name')}>{sortLabel('Nome', 'name')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('sector')}>{sortLabel('Setor', 'sector')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('price')}>{sortLabel('Preco', 'price')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('dy')}>{sortLabel('DY', 'dy')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('pl')}>{sortLabel('P/L', 'pl')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('pvp')}>{sortLabel('P/VP', 'pvp')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('incomes')}>{sortLabel('Proventos', 'incomes')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('variation_day')}>{sortLabel('Dia', 'variation_day')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('variation_7d')}>{sortLabel('7 dias', 'variation_7d')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('variation_30d')}>{sortLabel('30 dias', 'variation_30d')}</button></th>
            </tr>
          </thead>
          <tbody>
            {sortedAssets.map((asset) => (
              <tr key={asset.ticker}>
                <td className="sticky-col sticky-col-ticker"><Link to={`/ativo/${asset.ticker}`}>{asset.ticker}</Link></td>
                <td>{asset.name}</td>
                <td>{asset.sector}</td>
                <td>
                  <div className="market-data-cell">
                    <span>{brl(asset.price)}</span>
                    <small className={asset?.market_data?.is_stale ? 'market-data-badge stale' : 'market-data-badge live'}>
                      {formatSyncLabel(asset)}
                    </small>
                  </div>
                </td>
                <td>{pct(asset.dy)}</td>
                <td>{Number(asset.pl || 0).toFixed(2)}</td>
                <td>{Number(asset.pvp || 0).toFixed(2)}</td>
                <td>{brl(incomesByTicker[asset.ticker] || 0)}</td>
                <td className={Number(asset.variation_day || 0) >= 0 ? 'up' : 'down'}>
                  {pct(asset.variation_day)}
                </td>
                <td className={Number(asset.variation_7d || 0) >= 0 ? 'up' : 'down'}>
                  {pct(asset.variation_7d)}
                </td>
                <td className={Number(asset.variation_30d || 0) >= 0 ? 'up' : 'down'}>
                  {pct(asset.variation_30d)}
                </td>
              </tr>
            ))}
            {assets.length === 0 && (
              <tr>
                <td colSpan={11}>Nenhum ativo cadastrado ainda.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <h2 style={{ marginTop: 24 }}>Mapa de setores</h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Setor</th>
              <th>Ativos</th>
              <th>DY medio</th>
              <th>Valor de mercado</th>
            </tr>
          </thead>
          <tbody>
            {sectors.map((sector) => (
              <tr key={sector.sector}>
                <td>{sector.sector}</td>
                <td>{sector.assets_count}</td>
                <td>{pct(sector.avg_dy)}</td>
                <td>R$ {Number(sector.market_cap_bi || 0).toFixed(2)} bi</td>
              </tr>
            ))}
            {sectors.length === 0 && (
              <tr>
                <td colSpan={4}>Sem dados de setores disponiveis.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <h2 style={{ marginTop: 24 }}>Agenda de proventos futuros</h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Data com (ex)</th>
              <th>Pagamento</th>
              <th>Valor por cota</th>
              <th>Qtd em carteira</th>
              <th>Estimado</th>
              <th>Fonte</th>
            </tr>
          </thead>
          <tbody>
            {upcomingItems.map((item, idx) => (
              <tr key={`upcoming-home-${item.ticker}-${item.ex_date}-${idx}`}>
                <td><Link to={`/ativo/${item.ticker}`}>{item.ticker}</Link></td>
                <td>{dateBr(item.ex_date)}</td>
                <td>{dateBr(item.payment_date)}</td>
                <td>{money(item.amount_per_share, item.currency)}</td>
                <td>{Number(item.shares || 0).toFixed(4)}</td>
                <td>{money(item.estimated_total, item.currency)}</td>
                <td>{String(item.source || '').trim() || '-'}</td>
              </tr>
            ))}
            {upcomingItems.length === 0 && (
              <tr>
                <td colSpan={7}>Sem eventos futuros de proventos encontrados para os ativos em carteira.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default HomePage
