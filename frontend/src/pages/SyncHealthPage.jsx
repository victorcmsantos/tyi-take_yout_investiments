import { useEffect, useMemo, useState } from 'react'
import { Button, Paper, Typography } from '@mui/material'
import { apiGet, apiGetCached } from '../api'
import { formatDateTimeLocal } from '../datetime'

function toNumber(value, fallback = 0) {
  const num = Number(value)
  return Number.isFinite(num) ? num : fallback
}

function formatPct(value) {
  const num = Number(value)
  return Number.isFinite(num) ? `${num.toFixed(2)}%` : '-'
}

function windowLabel(windowKey) {
  if (windowKey === 'minute') return 'Minuto'
  if (windowKey === 'hour') return 'Hora'
  if (windowKey === 'day') return 'Dia'
  return String(windowKey || '')
}

function SyncHealthPage() {
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')
  const [statusPayload, setStatusPayload] = useState(null)
  const [queuePayload, setQueuePayload] = useState(null)
  const [auditPayload, setAuditPayload] = useState(null)

  const load = async (background = false) => {
    if (background) setRefreshing(true)
    else setLoading(true)
    setError('')
    try {
      const getFn = background ? apiGet : apiGetCached
      const [statusData, queueData, auditData] = await Promise.all([
        getFn('/api/sync/status', {}, { ttlMs: 5000, staleWhileRevalidate: true }),
        getFn('/api/sync/queue', { limit: 30 }, { ttlMs: 5000, staleWhileRevalidate: true }),
        getFn('/api/sync/audit', { limit: 40 }, { ttlMs: 8000, staleWhileRevalidate: true }),
      ])
      setStatusPayload(statusData || null)
      setQueuePayload(queueData || null)
      setAuditPayload(auditData || null)
    } catch (err) {
      setError(err.message)
    } finally {
      if (background) setRefreshing(false)
      else setLoading(false)
    }
  }

  useEffect(() => {
    load(false)
    const timer = window.setInterval(() => {
      load(true)
    }, 10000)
    return () => window.clearInterval(timer)
  }, [])

  const health = statusPayload?.health || {}
  const jobs = Array.isArray(health?.jobs) ? health.jobs : []
  const circuits = Array.isArray(health?.provider_circuits) ? health.provider_circuits : []
  const providerUsage = Array.isArray(health?.provider_usage) ? health.provider_usage : []
  const telegram = health?.telegram || {}
  const queueItems = Array.isArray(queuePayload?.items) ? queuePayload.items : []
  const auditItems = Array.isArray(auditPayload?.items) ? auditPayload.items : []

  const failedJobs = useMemo(
    () => jobs.filter((job) => toNumber(job?.consecutive_failures) > 0).length,
    [jobs],
  )
  const activeCooldowns = useMemo(
    () => circuits.filter((item) => Boolean(item?.active)).length,
    [circuits],
  )

  if (loading) return <p>Carregando saúde de sync...</p>

  return (
    <section>
      <div className="hero-line">
        <div>
          <h1>Saúde de sync</h1>
          <p className="subtitle">Visibilidade operacional de jobs, fila e orçamento de APIs.</p>
        </div>
        <div className="hero-actions">
          <Button variant="contained" onClick={() => load(true)} disabled={refreshing}>
            {refreshing ? 'Atualizando...' : 'Atualizar'}
          </Button>
        </div>
      </div>

      {!!error && <p className="notice-warn">{error}</p>}

      <div className="cards">
        <article className="card">
          <h3>Status geral</h3>
          <p>{String(health?.status || 'unknown').toUpperCase()}</p>
          <small>Atualizado em {formatDateTimeLocal(statusPayload?.generated_at || health?.time, '-')}</small>
        </article>
        <article className="card">
          <h3>Ativos stale</h3>
          <p>{toNumber(statusPayload?.stale_assets_total)}</p>
          <small>market_data_status stale/failed/unknown</small>
        </article>
        <article className="card">
          <h3>Falhas de jobs</h3>
          <p>{failedJobs}</p>
          <small>Jobs com consecutive_failures {'>'} 0</small>
        </article>
        <article className="card">
          <h3>Cooldown APIs</h3>
          <p>{activeCooldowns}</p>
          <small>Providers temporariamente pausados</small>
        </article>
        <article className="card">
          <h3>Fila scanner (running)</h3>
          <p>{toNumber(queuePayload?.running_count)}</p>
          <small>Execuções locais em andamento</small>
        </article>
        <article className="card">
          <h3>Telegram</h3>
          <p>{telegram?.enabled ? (telegram?.configured ? 'OK' : 'PARCIAL') : 'OFF'}</p>
          <small>{telegram?.enabled ? `${telegram?.notify_profile || 'prod'} · ${telegram?.notify_events?.length || 0} evento(s)` : 'Integracao desativada'}</small>
        </article>
      </div>

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Orçamento por provider</Typography>
        <div className="table-wrap">
          <table className="asset-table">
            <thead>
              <tr>
                <th>Provider</th>
                <th>Janela</th>
                <th>Uso</th>
                <th>Limite</th>
                <th>Restante</th>
                <th>Uso %</th>
                <th>429</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {providerUsage.flatMap((provider) => {
                const providerName = String(provider?.provider || '').toUpperCase()
                const windows = provider?.windows || {}
                return Object.keys(windows).map((windowKey) => {
                  const win = windows[windowKey] || {}
                  return (
                    <tr key={`${providerName}-${windowKey}`}>
                      <td>{providerName}</td>
                      <td>{windowLabel(windowKey)}</td>
                      <td>{toNumber(win.request_count)}</td>
                      <td>{toNumber(win.limit)}</td>
                      <td>{win.remaining == null ? 'sem limite' : toNumber(win.remaining)}</td>
                      <td>{win.usage_pct == null ? '-' : formatPct(win.usage_pct)}</td>
                      <td>{toNumber(win.status_429_count)}</td>
                      <td>{formatDateTimeLocal(win.updated_at, '-')}</td>
                    </tr>
                  )
                })
              })}
              {providerUsage.length === 0 && (
                <tr>
                  <td colSpan={8}>Sem dados de orçamento de provider.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Paper>

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Fila de scans</Typography>
        <div className="table-wrap">
          <table className="asset-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Status</th>
                <th>Solicitante</th>
                <th>Início</th>
                <th>Fim</th>
                <th>Total</th>
                <th>Processados</th>
                <th>Sinais</th>
                <th>HTTP</th>
                <th>Erro</th>
              </tr>
            </thead>
            <tbody>
              {queueItems.map((item) => (
                <tr key={`queue-${item.id}`}>
                  <td>{toNumber(item.id)}</td>
                  <td>{String(item.status || '').toUpperCase()}</td>
                  <td>{item.requested_by_username || '-'}</td>
                  <td>{formatDateTimeLocal(item.started_at, '-')}</td>
                  <td>{formatDateTimeLocal(item.finished_at, '-')}</td>
                  <td>{toNumber(item.total_tickers)}</td>
                  <td>{toNumber(item.processed_tickers)}</td>
                  <td>{toNumber(item.triggered_signals)}</td>
                  <td>{item.upstream_status ?? '-'}</td>
                  <td>{item.error_message || '-'}</td>
                </tr>
              ))}
              {queueItems.length === 0 && (
                <tr>
                  <td colSpan={10}>Sem execuções registradas.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Paper>

      <Paper className="admin-panel" sx={{ p: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Auditoria de sync por ticker</Typography>
        <div className="table-wrap">
          <table className="asset-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Ticker</th>
                <th>Sucesso</th>
                <th>When</th>
                <th>Providers</th>
                <th>Métrica</th>
                <th>Fallback</th>
                <th>Status</th>
                <th>Preço</th>
                <th>Erro</th>
              </tr>
            </thead>
            <tbody>
              {auditItems.map((item) => (
                <tr key={`audit-${item.id}`}>
                  <td>{toNumber(item.id)}</td>
                  <td>{item.ticker}</td>
                  <td>{item.success ? 'sim' : 'não'}</td>
                  <td>{formatDateTimeLocal(item.attempted_at, '-')}</td>
                  <td>{Array.isArray(item.providers_tried) ? item.providers_tried.join(', ') : '-'}</td>
                  <td>{item.metrics_source || '-'}</td>
                  <td>{item.fallback_used ? 'sim' : 'não'}</td>
                  <td>{item.market_data_status || '-'}</td>
                  <td>{item.price == null ? '-' : toNumber(item.price).toFixed(4)}</td>
                  <td>{item.error_message || '-'}</td>
                </tr>
              ))}
              {auditItems.length === 0 && (
                <tr>
                  <td colSpan={10}>Sem auditoria de sync registrada.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Paper>
    </section>
  )
}

export default SyncHealthPage
