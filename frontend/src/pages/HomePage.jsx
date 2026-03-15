import { useEffect, useMemo, useState } from 'react'
import { Skeleton } from '@mui/material'
import { formatCompactBrl, formatCurrencyBRL, formatPercent } from '../formatters'
import DashboardCustomizerPanel from '../components/DashboardCustomizerPanel'
import HighlightsCards from '../components/HighlightsCards'
import AssetsTable from '../components/AssetsTable'
import SectorsTable from '../components/SectorsTable'
import UpcomingIncomesTable from '../components/UpcomingIncomesTable'
import StatePanel from '../components/StatePanel'
import { dateTimeBr } from '../datetime'
import { useHomeDashboardData } from '../hooks/useHomeDashboardData'
import { usePersistedState } from '../persistedState'
import { emitAppToast } from '../toast'

const brl = (value) => formatCurrencyBRL(value, 'R$ 0,00')
const pct = (value, signed = false) => formatPercent(value, 2, { signed, fallback: '0.00%' })
const DEFAULT_CARD_ORDER = ['highestDy', 'highestGain', 'largestCap', 'incomesTotal', 'upcomingIncomes', 'syncHealth']
const DEFAULT_SECTION_VISIBILITY = {
  assets: true,
  sectors: true,
  upcoming: true,
}

function createDefaultDashboardPrefs() {
  return {
    cardOrder: [...DEFAULT_CARD_ORDER],
    hiddenCards: [],
    sections: { ...DEFAULT_SECTION_VISIBILITY },
  }
}

function normalizeDashboardPrefs(value) {
  const raw = value && typeof value === 'object' ? value : {}
  const rawOrder = Array.isArray(raw.cardOrder) ? raw.cardOrder.filter((key) => DEFAULT_CARD_ORDER.includes(key)) : []
  const cardOrder = [...rawOrder, ...DEFAULT_CARD_ORDER.filter((key) => !rawOrder.includes(key))]
  const hiddenCards = Array.isArray(raw.hiddenCards)
    ? raw.hiddenCards.filter((key) => DEFAULT_CARD_ORDER.includes(key))
    : []
  const rawSections = raw.sections && typeof raw.sections === 'object' ? raw.sections : {}

  return {
    cardOrder,
    hiddenCards,
    sections: {
      ...DEFAULT_SECTION_VISIBILITY,
      ...rawSections,
    },
  }
}

const secondsLabel = (value) => {
  const total = Math.max(Number(value || 0), 0)
  if (!Number.isFinite(total) || total <= 0) return '0s'
  const minutes = Math.floor(total / 60)
  const seconds = Math.floor(total % 60)
  if (minutes <= 0) return `${seconds}s`
  return `${minutes}m ${seconds}s`
}

function HomePage({ selectedPortfolioIds }) {
  const {
    assets,
    sectors,
    incomesByTicker,
    incomesTotal,
    upcomingIncomes,
    syncHealth,
    syncHealthError,
    loading,
    error,
    refreshingStaleAssets,
    refreshOnlyStaleAssets,
  } = useHomeDashboardData(selectedPortfolioIds)
  const [showHealthModal, setShowHealthModal] = useState(false)
  const [showCustomizer, setShowCustomizer] = useState(false)
  const [sortState, setSortState] = usePersistedState('home.assets.sort.v1', { by: 'name', dir: 'asc' })
  const [dashboardPrefs, setDashboardPrefs] = usePersistedState(
    'home.dashboard.customization.v1',
    createDefaultDashboardPrefs(),
  )
  const sortBy = String(sortState?.by || 'name')
  const sortDir = String(sortState?.dir || 'asc')
  const normalizedDashboardPrefs = useMemo(() => normalizeDashboardPrefs(dashboardPrefs), [dashboardPrefs])

  const toggleSort = (field) => {
    if (sortBy === field) {
      setSortState((current) => ({
        by: String(current?.by || 'name'),
        dir: String(current?.dir || 'asc') === 'asc' ? 'desc' : 'asc',
      }))
      return
    }
    setSortState({ by: field, dir: 'desc' })
  }

  const sortLabel = (label, field) => {
    if (sortBy !== field) return label
    return `${label} ${sortDir === 'asc' ? '↑' : '↓'}`
  }

  useEffect(() => {
    if (!error) return
    emitAppToast({ severity: 'error', message: error })
  }, [error])

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
  const staleTickers = useMemo(
    () => assets
      .filter((asset) => asset?.market_data?.is_stale)
      .map((asset) => String(asset?.ticker || '').trim().toUpperCase())
      .filter(Boolean),
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
  const healthTimeLabel = syncHealth?.time ? dateTimeBr(syncHealth.time) : '-'

  const highlightCardItems = useMemo(() => ([
    {
      key: 'highestDy',
      title: 'Maior dividend yield',
      value: highlights?.highestDy?.ticker || '-',
      caption: highlights?.highestDy ? `${pct(highlights.highestDy.dy)} a.a.` : 'Sem ativos suficientes',
    },
    {
      key: 'highestGain',
      title: 'Maior alta do dia',
      value: highlights?.highestGain?.ticker || '-',
      caption: highlights?.highestGain ? pct(highlights.highestGain.variation_day, true) : 'Sem variacao capturada',
    },
    {
      key: 'largestCap',
      title: 'Maior valor de mercado',
      value: highlights?.largestCap?.ticker || '-',
      caption: highlights?.largestCap
        ? formatCompactBrl(Number(highlights.largestCap.market_cap_bi || 0) * 1_000_000_000, '-')
        : 'Sem base suficiente',
    },
    {
      key: 'incomesTotal',
      title: 'Proventos totais',
      value: brl(incomesTotal),
      caption: 'Carteiras selecionadas',
    },
    {
      key: 'upcomingIncomes',
      title: 'Proventos futuros',
      value: String(upcomingItems.length),
      caption: upcomingEstimatedBrl > 0
        ? `Estimado BRL: ${brl(upcomingEstimatedBrl)}`
        : 'Sem valor estimado no momento',
    },
    {
      key: 'syncHealth',
      title: 'Saude de sync',
      value: syncHealth?.status === 'ok' ? 'OK' : 'ATENCAO',
      valueClassName: syncHealth?.status === 'ok' ? 'up' : 'down',
      metaLines: [
        `Stale: ${staleAssetsCount} | Falhas de jobs: ${failedJobsCount}`,
        `Ultimo sync mercado: ${marketSyncJob?.last_success_at ? dateTimeBr(marketSyncJob.last_success_at) : '-'}`,
        `Cooldown APIs: ${circuitLabel}`,
        syncHealthError || '',
      ].filter(Boolean),
      actionLabel: 'Ver detalhes',
      onAction: () => setShowHealthModal(true),
    },
  ]), [
    highlights,
    incomesTotal,
    upcomingItems.length,
    upcomingEstimatedBrl,
    syncHealth,
    staleAssetsCount,
    failedJobsCount,
    marketSyncJob,
    circuitLabel,
    syncHealthError,
  ])

  const highlightCardMap = useMemo(
    () => Object.fromEntries(highlightCardItems.map((item) => [item.key, item])),
    [highlightCardItems],
  )
  const orderedHighlightCards = normalizedDashboardPrefs.cardOrder
    .map((key) => highlightCardMap[key])
    .filter(Boolean)
  const visibleHighlightCards = orderedHighlightCards.filter(
    (item) => !normalizedDashboardPrefs.hiddenCards.includes(item.key),
  )
  const sectionItems = [
    { key: 'assets', label: 'Tabela de ativos', enabled: normalizedDashboardPrefs.sections.assets !== false },
    { key: 'sectors', label: 'Mapa de setores', enabled: normalizedDashboardPrefs.sections.sectors !== false },
    { key: 'upcoming', label: 'Agenda de proventos futuros', enabled: normalizedDashboardPrefs.sections.upcoming !== false },
  ]
  const visibleSectionsCount = sectionItems.filter((item) => item.enabled).length

  const moveDashboardCard = (cardKey, direction) => {
    setDashboardPrefs((current) => {
      const next = normalizeDashboardPrefs(current)
      const currentIndex = next.cardOrder.indexOf(cardKey)
      const targetIndex = currentIndex + direction
      if (currentIndex < 0 || targetIndex < 0 || targetIndex >= next.cardOrder.length) return next
      const cardOrder = [...next.cardOrder]
      const [moved] = cardOrder.splice(currentIndex, 1)
      cardOrder.splice(targetIndex, 0, moved)
      return { ...next, cardOrder }
    })
  }

  const toggleDashboardCardVisibility = (cardKey) => {
    setDashboardPrefs((current) => {
      const next = normalizeDashboardPrefs(current)
      const hiddenCards = next.hiddenCards.includes(cardKey)
        ? next.hiddenCards.filter((key) => key !== cardKey)
        : [...next.hiddenCards, cardKey]
      return { ...next, hiddenCards }
    })
  }

  const toggleDashboardSection = (sectionKey) => {
    setDashboardPrefs((current) => {
      const next = normalizeDashboardPrefs(current)
      return {
        ...next,
        sections: {
          ...next.sections,
          [sectionKey]: !next.sections[sectionKey],
        },
      }
    })
  }

  const resetDashboardPrefs = () => {
    setDashboardPrefs(createDefaultDashboardPrefs())
  }

  const onRefreshOnlyStaleAssets = async () => {
    if (refreshingStaleAssets || staleTickers.length === 0) return
    try {
      const result = await refreshOnlyStaleAssets()
      if (result.failedCount > 0) {
        emitAppToast({
          severity: 'warning',
          message: `Atualizacao concluida com ${result.failedCount} falha(s). ${result.updatedCount} ativo(s) atualizado(s).`,
        })
      } else {
        emitAppToast({
          severity: 'success',
          message: `${result.updatedCount} ativo(s) desatualizado(s) atualizado(s).`,
        })
      }
    } catch (err) {
      emitAppToast({
        severity: 'error',
        message: err?.message || 'Falha ao atualizar ativos desatualizados.',
      })
    }
  }

  if (loading) {
    return (
      <section>
        <div className="dashboard-hero dashboard-animate">
          <div>
            <small className="dashboard-hero-eyebrow">Visao geral</small>
            <h1>Acoes</h1>
            <p>Resumo rapido dos ativos, saude de sync e agenda de proventos.</p>
          </div>
        </div>
        <div className="cards">
          {Array.from({ length: 6 }).map((_, idx) => (
            <article className="card" key={`home-skeleton-${idx}`}>
              <Skeleton variant="text" width={160} height={24} />
              <Skeleton variant="text" width={110} height={44} />
              <Skeleton variant="text" width={200} height={20} />
            </article>
          ))}
        </div>
        <div className="table-wrap">
          <table className="asset-table">
            <thead>
              <tr>
                <th>Ticker</th>
                <th>Nome</th>
                <th>Setor</th>
                <th>Preco</th>
              </tr>
            </thead>
            <tbody>
              {Array.from({ length: 8 }).map((_, idx) => (
                <tr key={`home-skeleton-row-${idx}`}>
                  <td><Skeleton variant="text" width={90} /></td>
                  <td><Skeleton variant="text" width={200} /></td>
                  <td><Skeleton variant="text" width={130} /></td>
                  <td><Skeleton variant="text" width={120} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    )
  }

  if (error) return <p className="error">{error}</p>

  return (
    <section>
      <div className="dashboard-hero dashboard-animate">
        <div>
          <small className="dashboard-hero-eyebrow">Visao geral</small>
          <h1>Acoes</h1>
          <p>Leia performance, qualidade de sincronizacao e proventos sem sair da pagina inicial.</p>
        </div>
        <div className="dashboard-hero-actions">
          <button
            type="button"
            className="btn-secondary"
            onClick={() => setShowCustomizer((current) => !current)}
          >
            {showCustomizer ? 'Fechar personalizacao' : 'Personalizar dashboard'}
          </button>
          <small>
            {visibleHighlightCards.length} card(s) visiveis · {visibleSectionsCount} bloco(s) ativos
          </small>
        </div>
      </div>

      {staleAssetsCount > 0 && (
        <div className="notice-warn notice-warn-action dashboard-animate">
          <span>
            {staleAssetsCount} ativo(s) exibem cotacao possivelmente antiga. Veja a coluna de preco para identificar quais.
          </span>
          <button
            type="button"
            className="btn-primary"
            onClick={onRefreshOnlyStaleAssets}
            disabled={refreshingStaleAssets || staleTickers.length === 0}
          >
            {refreshingStaleAssets ? 'Atualizando...' : 'Atualizar somente desatualizados'}
          </button>
        </div>
      )}

      {showCustomizer && (
        <DashboardCustomizerPanel
          cardItems={orderedHighlightCards}
          hiddenCardKeys={normalizedDashboardPrefs.hiddenCards}
          onMoveCard={moveDashboardCard}
          onToggleCardVisibility={toggleDashboardCardVisibility}
          sections={sectionItems}
          onToggleSection={toggleDashboardSection}
          onReset={resetDashboardPrefs}
        />
      )}

      <HighlightsCards
        cards={visibleHighlightCards}
        onResetHiddenCards={resetDashboardPrefs}
      />

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
              <div><strong>Status:</strong> {syncHealth?.status === 'ok' ? 'OK' : 'ATENCAO'}</div>
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

      {visibleSectionsCount > 0 ? (
        <div className="dashboard-sections">
          {normalizedDashboardPrefs.sections.assets !== false && (
            <div className="dashboard-animate">
              <AssetsTable
                sortedAssets={sortedAssets}
                incomesByTicker={incomesByTicker}
                toggleSort={toggleSort}
                sortLabel={sortLabel}
              />
            </div>
          )}

          {normalizedDashboardPrefs.sections.sectors !== false && (
            <div className="dashboard-animate">
              <SectorsTable sectors={sectors} />
            </div>
          )}

          {normalizedDashboardPrefs.sections.upcoming !== false && (
            <div className="dashboard-animate">
              <UpcomingIncomesTable upcomingItems={upcomingItems} />
            </div>
          )}
        </div>
      ) : (
        <StatePanel
          eyebrow="Dashboard personalizada"
          title="Todos os blocos da pagina foram ocultados"
          description="Reabra a personalizacao para voltar com tabelas, setores e agenda de proventos."
          actionLabel="Restaurar layout"
          onAction={resetDashboardPrefs}
        />
      )}
    </section>
  )
}

export default HomePage
