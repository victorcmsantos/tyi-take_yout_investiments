import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { Skeleton } from '@mui/material'
import 'chart.js/auto'
import { Line } from 'react-chartjs-2'
import { apiGet, apiPost } from '../api'
import { formatAgeFromNow, formatDateTimeLocal, parseApiDate } from '../datetime'
import { formatCurrencyBRL, formatPercent, formatQuantity, toFiniteNumber } from '../formatters'
import StatePanel from '../components/StatePanel'
import Trend from '../components/Trend'
import { buildPositionDecision, normalizeStructuredMarketMood, structuredActionLabel } from '../lib/positionDecision'

const CHART_RANGES = [
  { key: '1d', label: '1 DIA' },
  { key: '7d', label: '7 DIAS' },
  { key: '30d', label: '30 DIAS' },
  { key: '6m', label: '6 MESES' },
  { key: '1y', label: '1 ANO' },
  { key: '5y', label: '5 ANOS' },
]
const rawEnrichmentStaleDays = Number(import.meta.env.VITE_OPENCLAW_ENRICHMENT_STALE_DAYS)
const enrichmentStaleDays = Number.isFinite(rawEnrichmentStaleDays) && rawEnrichmentStaleDays > 0
  ? rawEnrichmentStaleDays
  : 3
const ENRICHMENT_STALE_AFTER_MS = enrichmentStaleDays * 24 * 60 * 60 * 1000

const brl = (value) => formatCurrencyBRL(value, '-')
const pct = (value) => formatPercent(value, 2, { fallback: '-' })
const signedPct = (value) => formatPercent(value, 2, { signed: true, fallback: '0,00%' })
const ratio = (value) => formatQuantity(value, { minDigits: 2, maxDigits: 2, trim: false })
const qty = (value) => formatQuantity(value, { maxDigits: 4 })
const marketCapBi = (value) => {
  const num = toFiniteNumber(value, null)
  if (num == null || num <= 0) return '-'
  return `R$ ${formatQuantity(num, { minDigits: 2, maxDigits: 2, trim: false })} bi`
}
const money = (value, currency = 'BRL') => {
  const code = String(currency || 'BRL').trim().toUpperCase() || 'BRL'
  if (code === 'BRL') return formatCurrencyBRL(value, '-')
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  try {
    return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: code }).format(num)
  } catch (_) {
    return `${code} ${num.toFixed(6)}`
  }
}
const shortText = (value, limit = 140) => {
  const text = String(value || '').trim()
  if (!text) return ''
  if (text.length <= limit) return text
  return `${text.slice(0, limit - 3).trim()}...`
}
const marketDataSummary = (marketData) => {
  if (!marketData) return ''
  const source = marketData.source ? String(marketData.source).toUpperCase() : 'provider'
  const updatedAtLabel = formatDateTimeLocal(marketData.updated_at)
  const ageLabel = formatAgeFromNow(marketData.updated_at, '-')
  if (marketData.is_stale) {
    if (updatedAtLabel) {
      return `Cotação possivelmente antiga. Último sucesso via ${source}. Candle: ${updatedAtLabel}. Idade: ${ageLabel}.`
    }
    return 'Cotação sem sincronização recente confirmada.'
  }
  if (updatedAtLabel) {
    return `Última atualização confirmada via ${source}. Candle: ${updatedAtLabel}. Idade: ${ageLabel}.`
  }
  return ''
}

function dateBr(value) {
  if (!value) return ''
  const text = String(value)
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const [y, m, d] = text.split('-')
    return `${d}/${m}/${y}`
  }
  return text
}

function dateTimeLabel(value) {
  return formatDateTimeLocal(value)
}

function isTransientOpenClawReply(value) {
  const text = String(value || '').trim().toLowerCase()
  if (!text) return false
  return [
    'aguarde',
    'um momento',
    'enquanto busco',
    'estou buscando',
    'buscando essas informacoes',
    'busco essas informacoes',
    'buscando essas informações',
    'busco essas informações',
    'ja volto',
    'processando',
  ].some((marker) => text.includes(marker))
}

function AssetKpiSkeleton({ count = 4 }) {
  return (
    <div className="cards asset-kpi-grid">
      {Array.from({ length: count }).map((_, idx) => (
        <article className="card" key={`asset-kpi-skeleton-${idx}`}>
          <Skeleton variant="text" width={120} height={20} />
          <Skeleton variant="text" width={90} height={34} />
        </article>
      ))}
    </div>
  )
}

function AssetPageSkeleton() {
  return (
    <section className="asset-terminal-page">
      <div className="hero-actions asset-terminal-back">
        <Link to="/carteira" className="btn-link asset-back-link">Voltar para Renda Variável</Link>
      </div>
      <header className="card asset-terminal-hero">
        <small className="asset-terminal-eyebrow">Asset terminal</small>
        <Skeleton variant="text" width={260} height={40} />
        <Skeleton variant="text" width={160} height={20} />
        <div className="asset-terminal-meta-strip">
          {Array.from({ length: 4 }).map((_, idx) => (
            <div className="asset-terminal-meta-item" key={`asset-hero-skeleton-${idx}`}>
              <Skeleton variant="text" width={60} height={16} />
              <Skeleton variant="text" width={90} height={22} />
            </div>
          ))}
        </div>
      </header>
      <article className="card detail-card asset-price-card">
        <Skeleton variant="text" width={180} height={26} />
        <Skeleton variant="rectangular" height={300} style={{ borderRadius: 12, marginTop: 12 }} />
      </article>
      <AssetKpiSkeleton count={4} />
      <AssetKpiSkeleton count={4} />
    </section>
  )
}

function AssetPage({ selectedPortfolioIds }) {
  const { ticker } = useParams()
  const autoEnrichedTickersRef = useRef(new Set())
  const [rangeKey, setRangeKey] = useState('1y')
  const [payload, setPayload] = useState(null)
  const [priceHistory, setPriceHistory] = useState({ labels: [], prices: [], change_pct: null })
  const [priceHistoryLoading, setPriceHistoryLoading] = useState(true)
  const [priceHistoryError, setPriceHistoryError] = useState('')
  const [priceHistoryRefreshKey, setPriceHistoryRefreshKey] = useState(0)
  const [enrichment, setEnrichment] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [assetRefreshKey, setAssetRefreshKey] = useState(0)
  const [syncMessage, setSyncMessage] = useState('')
  const [syncStatus, setSyncStatus] = useState('warn')
  const [syncing, setSyncing] = useState(false)
  const [enriching, setEnriching] = useState(false)
  const [enrichMessage, setEnrichMessage] = useState('')
  const [enrichStatus, setEnrichStatus] = useState('warn')

  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    ;(async () => {
      try {
        const data = await apiGet(`/api/assets/${ticker}`, {
          portfolio_id: selectedPortfolioIds,
        })
        if (!active) return
        setPayload(data)
        setEnrichment(data?.enrichment || null)
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
  }, [ticker, selectedPortfolioIds, assetRefreshKey])

  useEffect(() => {
    let active = true
    setPriceHistoryLoading(true)
    setPriceHistoryError('')
    ;(async () => {
      try {
        const data = await apiGet(`/api/assets/${ticker}/price-history`, {
          range: rangeKey,
        })
        if (!active) return
        setPriceHistory(data || { labels: [], prices: [], change_pct: null })
      } catch (err) {
        if (!active) return
        setPriceHistory({ labels: [], prices: [], change_pct: null })
        setPriceHistoryError(err.message)
      } finally {
        if (active) setPriceHistoryLoading(false)
      }
    })()

    return () => {
      active = false
    }
  }, [ticker, rangeKey, priceHistoryRefreshKey])

  const onSyncTicker = async () => {
    if (!ticker) return
    setSyncing(true)
    setSyncMessage('')
    setSyncStatus('warn')
    try {
      const response = await apiPost(`/api/sync/market-data/${ticker}`, {})
      if (response?.success === false) {
        setSyncStatus('warn')
        setSyncMessage(String(response?.message || 'Sincronização adiada: existe outro scan em andamento.'))
        return
      }
      const sourceLabel = String(response?.source || '').trim().toUpperCase()
      const updatedLabel = formatDateTimeLocal(response?.updated_at)
      setSyncStatus('ok')
      if (sourceLabel && updatedLabel) {
        setSyncMessage(`Atualização concluída via ${sourceLabel} em ${updatedLabel}.`)
      } else if (sourceLabel) {
        setSyncMessage(`Atualização concluída via ${sourceLabel}.`)
      } else {
        setSyncMessage('Atualização concluída via scanner.')
      }
      const data = await apiGet(`/api/assets/${ticker}`, {
        portfolio_id: selectedPortfolioIds,
      })
      setPayload(data)
      setPriceHistoryRefreshKey((current) => current + 1)
    } catch (err) {
      setSyncStatus('warn')
      setSyncMessage(err.message)
    } finally {
      setSyncing(false)
    }
  }

  const asset = payload?.asset || {}
  const position = payload?.position || {}
  const transactions = payload?.transactions || []
  const incomes = payload?.incomes || []
  const upcomingIncomes = Array.isArray(payload?.upcoming_incomes) ? payload.upcoming_incomes : []
  const enrichmentHistory = Array.isArray(payload?.enrichment_history) ? payload.enrichment_history : []
  const marketData = asset.market_data || {}
  const enrichmentPayload = enrichment?.payload && typeof enrichment.payload === 'object' ? enrichment.payload : null
  const rawEnrichmentReply = String(enrichment?.raw_reply || '').trim()
  const hasTransientRawReply = isTransientOpenClawReply(rawEnrichmentReply)
  const enrichmentUpdatedAtMs = parseApiDate(enrichment?.updated_at)
  const hasAnyEnrichment = Boolean((rawEnrichmentReply && !hasTransientRawReply) || enrichmentPayload)
  const isEnrichmentStale = hasTransientRawReply || !enrichmentUpdatedAtMs || (Date.now() - enrichmentUpdatedAtMs) >= ENRICHMENT_STALE_AFTER_MS
  const hasStructuredEnrichment = Boolean(
    enrichmentPayload && (
      String(enrichmentPayload.resumo || '').trim() ||
      String(enrichmentPayload.modelo_de_negocio || '').trim() ||
      String(enrichmentPayload.dividendos || '').trim() ||
      String(enrichmentPayload.visao_do_mercado || '').trim() ||
      String(enrichmentPayload.humor_do_mercado || '').trim() ||
      String(enrichmentPayload.acao_sugerida || '').trim() ||
      String(enrichmentPayload.justificativa_da_acao || '').trim() ||
      String(enrichmentPayload.observacoes || '').trim() ||
      (Array.isArray(enrichmentPayload.tese) && enrichmentPayload.tese.length > 0) ||
      (Array.isArray(enrichmentPayload.riscos) && enrichmentPayload.riscos.length > 0)
    )
  )
  const hasSavedEnrichment = hasAnyEnrichment
  const hasPosition = Number(position.shares || 0) > 0
  const positionDecision = useMemo(
    () => buildPositionDecision({ asset, position, enrichmentPayload }),
    [asset, position, enrichmentPayload]
  )
  const decisionMoodClass = positionDecision.mood.tone === 'positive' ? 'up' : (positionDecision.mood.tone === 'negative' ? 'down' : '')
  const decisionActionClass = positionDecision.actionTone || 'neutral'
  const decisionPriceGapClass = positionDecision.priceGapPct === null
    ? ''
    : (positionDecision.priceGapPct >= 0 ? 'up' : 'down')
  const decisionPriceGapLabel = positionDecision.priceGapPct === null
    ? 'Sem preço médio'
    : `${signedPct(positionDecision.priceGapPct)} vs preço médio`
  const selectedRangeLabel = (CHART_RANGES.find((opt) => opt.key === rangeKey)?.label || String(rangeKey || '').toUpperCase())
  const openClawMoodLabel = String(enrichmentPayload?.humor_do_mercado || '').trim()
  const openClawMoodClass = openClawMoodLabel
    ? (normalizeStructuredMarketMood(openClawMoodLabel) === 'positive'
      ? 'up'
      : (normalizeStructuredMarketMood(openClawMoodLabel) === 'negative' ? 'down' : 'neutral'))
    : 'neutral'
  const openClawActionLabel = structuredActionLabel(enrichmentPayload?.acao_sugerida) || String(enrichmentPayload?.acao_sugerida || '').trim() || 'Sem sinal'
  const openClawCacheLabel = isEnrichmentStale ? 'Cache vencido' : 'Cache em dia'
  const openClawCacheClass = isEnrichmentStale ? 'down' : 'up'

  useEffect(() => {
    const tickerKey = String(ticker || '').trim().toUpperCase()
    if (!tickerKey || loading || error || !payload) return
    if (hasAnyEnrichment && !isEnrichmentStale) return
    if (enriching) return
    if (autoEnrichedTickersRef.current.has(tickerKey)) return

    autoEnrichedTickersRef.current.add(tickerKey)
    setEnriching(true)
    setEnrichStatus('warn')
    setEnrichMessage(hasAnyEnrichment ? 'Atualizando resumo automático via OpenClaw...' : 'Gerando resumo automático via OpenClaw...')

    ;(async () => {
      try {
        const data = await apiPost(`/api/assets/${tickerKey}/enrich/openclaw`)
        setEnrichStatus('ok')
        setEnrichMessage(data?.message || 'Resumo atualizado.')
        setEnrichment(data?.enrichment || null)
        setPayload((current) => (current ? { ...current, enrichment_history: data?.enrichment_history || [] } : current))
      } catch (err) {
        setEnrichStatus('warn')
        setEnrichMessage(err.message)
      } finally {
        setEnriching(false)
      }
    })()
  }, [ticker, loading, error, payload, hasAnyEnrichment, isEnrichmentStale, enriching])

  const onEnrichOpenClaw = async () => {
    if (!ticker) return
    autoEnrichedTickersRef.current.add(String(ticker || '').trim().toUpperCase())
    setEnriching(true)
    setEnrichStatus('warn')
    setEnrichMessage('')
    try {
      const data = await apiPost(`/api/assets/${ticker}/enrich/openclaw`)
      setEnrichStatus('ok')
      setEnrichMessage(data?.message || 'Resumo atualizado.')
      setEnrichment(data?.enrichment || null)
      setPayload((current) => (current ? { ...current, enrichment_history: data?.enrichment_history || [] } : current))
    } catch (err) {
      setEnrichStatus('warn')
      setEnrichMessage(err.message)
    } finally {
      setEnriching(false)
    }
  }

  const chartData = useMemo(() => ({
    labels: priceHistory.labels || [],
    datasets: [
      {
        label: 'Cotação',
        data: priceHistory.prices || [],
        borderColor: '#a57f39',
        backgroundColor: 'rgba(165, 127, 57, 0.12)',
        fill: true,
        tension: 0.2,
        pointRadius: 0,
      },
    ],
  }), [priceHistory.labels, priceHistory.prices])

  if (loading) return <AssetPageSkeleton />
  if (error) {
    return (
      <section className="asset-terminal-page">
        <div className="hero-actions asset-terminal-back">
          <Link to="/carteira" className="btn-link asset-back-link">Voltar para Renda Variável</Link>
        </div>
        <StatePanel
          eyebrow="Asset terminal"
          title={`Não foi possível carregar ${String(ticker || '').toUpperCase()}`}
          description={error}
          actionLabel="Tentar novamente"
          onAction={() => setAssetRefreshKey((current) => current + 1)}
        />
      </section>
    )
  }
  if (!payload) {
    return (
      <section className="asset-terminal-page">
        <StatePanel title="Sem dados para este ativo." />
      </section>
    )
  }

  return (
    <section className="asset-terminal-page">
      <div className="hero-actions asset-terminal-back">
        <Link to="/carteira" className="btn-link asset-back-link">
          Voltar para Renda Variável
        </Link>
      </div>

      <header className="card asset-terminal-hero">
        <div className="asset-terminal-hero-top">
          <div>
            <small className="asset-terminal-eyebrow">Asset terminal</small>
            <div className="hero-line asset-terminal-title-row">
              <h1>{asset.ticker} - {asset.name}</h1>
              <span className={`market-data-badge ${marketData.is_stale ? 'stale' : 'live'}`}>
                {marketData.is_stale ? 'STALE' : 'LIVE'}
              </span>
            </div>
            <p className="subtitle">Setor: {asset.sector}</p>
          </div>
          <button type="button" className="btn-secondary" onClick={onSyncTicker} disabled={syncing}>
            {syncing ? 'Atualizando...' : 'Atualizar market data'}
          </button>
        </div>

        <div className="asset-terminal-meta-strip">
          <div className="asset-terminal-meta-item">
            <span>Preço</span>
            <strong>{brl(asset.price)}</strong>
          </div>
          <div className="asset-terminal-meta-item">
            <span>Dia</span>
            <strong><Trend value={asset.variation_day}>{signedPct(asset.variation_day)}</Trend></strong>
          </div>
          <div className="asset-terminal-meta-item">
            <span>Provider</span>
            <strong>{(String(marketData?.source || '').trim().toUpperCase() || 'MARKET_SCANNER')}</strong>
          </div>
          <div className="asset-terminal-meta-item">
            <span>Candle</span>
            <strong>{formatDateTimeLocal(marketData?.updated_at, '-')}</strong>
          </div>
        </div>

        {!!syncMessage && <p className={syncStatus === 'ok' ? 'notice-ok' : 'notice-warn'}>{syncMessage}</p>}
        {!!marketDataSummary(marketData) && (
          <p className={marketData.is_stale ? 'notice-warn' : 'notice-ok'}>
            {marketDataSummary(marketData)}
            {marketData.last_error ? ` Último erro: ${marketData.last_error}.` : ''}
          </p>
        )}
      </header>

      <article className="card detail-card asset-price-card">
        <div className="hero-line">
          <h2>Cotação {asset.ticker}</h2>
          <div className="range-tabs" role="group" aria-label="Período do gráfico de cotação">
            {CHART_RANGES.map((opt) => (
              <button
                key={opt.key}
                type="button"
                className={`range-tab ${rangeKey === opt.key ? 'active' : ''}`}
                aria-pressed={rangeKey === opt.key}
                onClick={() => setRangeKey(opt.key)}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>
        <p className="asset-chart-price">
          <strong className="asset-chart-price-value">{brl(asset.price)}</strong>
          {priceHistory.change_pct !== null && priceHistory.change_pct !== undefined && (
            <Trend value={priceHistory.change_pct}>
              {signedPct(priceHistory.change_pct)} (período {selectedRangeLabel})
            </Trend>
          )}
        </p>
        {priceHistoryLoading ? (
          <Skeleton variant="rectangular" height={300} style={{ borderRadius: 12 }} />
        ) : (priceHistory.labels || []).length > 0 ? (
          <div
            className="chart-canvas-wrap"
            role="img"
            aria-label={`Gráfico de cotação de ${asset.ticker} no período ${selectedRangeLabel}. Preço atual ${brl(asset.price)}, variação de ${signedPct(priceHistory.change_pct)} no período.`}
          >
            <Line
              data={chartData}
              options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }}
            />
          </div>
        ) : (
          <p className="notice-warn">
            {priceHistoryError || 'Não foi possível carregar histórico para este período.'}
          </p>
        )}
      </article>

      {hasPosition && (
        <article className="card asset-position-hero" aria-label={`Resumo da posição em ${asset.ticker}`}>
          <div className="hero-primary">
            <span className="section-kicker">Valor de mercado da posição</span>
            <span className="hero-value">{brl(position.market_value)}</span>
            <span className="subtitle">Investido: {brl(position.total_value)}</span>
          </div>
          <div className="hero-secondary">
            <span className="section-kicker">Resultado em aberto</span>
            <span className="hero-value">
              <Trend value={position.open_pnl_value}>{brl(position.open_pnl_value)}</Trend>
            </span>
            <Trend value={position.open_pnl_pct}>{signedPct(position.open_pnl_pct)}</Trend>
            <span className={`analysis-pill asset-position-decision ${decisionActionClass}`}>
              Leitura: {positionDecision.action}
            </span>
          </div>
          <div className="hero-meta">
            <div>
              <span className="section-kicker">Quantidade</span>
              <strong>{qty(position.shares)}</strong>
            </div>
            <div>
              <span className="section-kicker">Preço médio</span>
              <strong>{brl(position.avg_price)}</strong>
            </div>
            <div>
              <span className="section-kicker">Proventos 12m</span>
              <strong>{brl(position.incomes_12m)}</strong>
            </div>
            <div>
              <span className="section-kicker">Proventos total</span>
              <strong>{brl(position.total_incomes)}</strong>
            </div>
          </div>
        </article>
      )}

      <article className="card detail-card tactical-card asset-terminal-panel">
        <div className="analysis-head">
          <div>
            <h2>Leitura tática</h2>
            <p className="subtitle">Ponderação entre a leitura do OpenClaw, preço atual e seu custo médio.</p>
          </div>
          <span className={`analysis-pill ${positionDecision.actionTone || 'neutral'}`}>
            {positionDecision.action}
          </span>
        </div>

        <div className="analysis-strip">
          <div className="analysis-strip-item">
            <span className="analysis-label">Preço atual</span>
            <strong>{brl(asset.price)}</strong>
          </div>
          <div className="analysis-strip-item">
            <span className="analysis-label">Preço médio</span>
            <strong>{Number(position.avg_price || 0) > 0 ? brl(position.avg_price) : 'Não calculado'}</strong>
          </div>
          <div className="analysis-strip-item">
            <span className="analysis-label">Resultado aberto</span>
            <strong>
              <Trend value={position.open_pnl_pct}>{signedPct(position.open_pnl_pct)}</Trend>
            </strong>
          </div>
        </div>

        <div className="analysis-metrics">
          <div className="analysis-metric">
            <span className="analysis-label">Humor do mercado</span>
            <strong className={decisionMoodClass}>{positionDecision.mood.label}</strong>
          </div>
          <div className="analysis-metric">
            <span className="analysis-label">Sinal do OpenClaw</span>
            <strong>{positionDecision.openClawActionLabel || 'Sem sinal estruturado'}</strong>
          </div>
          <div className="analysis-metric">
            <span className="analysis-label">Gap vs preço médio</span>
            <strong className={decisionPriceGapClass}>{decisionPriceGapLabel}</strong>
          </div>
        </div>

        <div className="analysis-summary">
          <p>{positionDecision.summary}</p>
          {!!positionDecision.openClawActionWhy && (
            <p className="subtitle">
              {positionDecision.openClawActionWhy}
            </p>
          )}
        </div>

        {positionDecision.reasons.length > 0 && (
          <ul className="analysis-list">
            {positionDecision.reasons.map((reason, idx) => (
              <li key={`decision-reason-${idx}`}>{reason}</li>
            ))}
          </ul>
        )}

        <p className="subtitle analysis-note">
          Heurística de apoio, não decisão automática.
        </p>
      </article>

      <section className="asset-kpi-section">
        <h2 className="asset-kpi-section-title">Fundamentos</h2>
        <div className="cards asset-kpi-grid">
          <article className="card"><h3>Dividend Yield</h3><p>{pct(asset.dy)}</p></article>
          <article className="card"><h3>P/L</h3><p>{ratio(asset.pl)}</p></article>
          <article className="card"><h3>P/VP</h3><p>{ratio(asset.pvp)}</p></article>
          <article className="card"><h3>Valor de mercado</h3><p>{marketCapBi(asset.market_cap_bi)}</p></article>
        </div>
      </section>

      <section className="asset-kpi-section">
        <h2 className="asset-kpi-section-title">Proventos</h2>
        {Number(position.total_incomes || 0) > 0 ? (
          <div className="cards asset-kpi-grid">
            <article className="card"><h3>Mês atual</h3><p>{brl(position.incomes_current_month)}</p></article>
            <article className="card"><h3>3 meses</h3><p>{brl(position.incomes_3m)}</p></article>
            <article className="card"><h3>12 meses</h3><p>{brl(position.incomes_12m)}</p></article>
            <article className="card"><h3>Total</h3><p>{brl(position.total_incomes)}</p></article>
          </div>
        ) : (
          <StatePanel compact title="Sem proventos registrados para este ativo." />
        )}
      </section>

      <article className="card detail-card openclaw-card asset-terminal-panel">
        <div className="hero-line">
          <div>
            <h2>OpenClaw</h2>
            <p className="subtitle">Atualização automática a cada {enrichmentStaleDays} dia(s). O botão ignora o cache.</p>
          </div>
          <button type="button" className="btn-secondary" onClick={onEnrichOpenClaw} disabled={enriching}>
            {enriching ? 'Atualizando...' : (hasSavedEnrichment ? 'Forçar atualização' : 'Gerar com OpenClaw')}
          </button>
        </div>

        {!!enrichMessage && (
          <p className={enrichStatus === 'ok' ? 'notice-ok' : 'notice-warn'}>
            {enrichMessage}
          </p>
        )}

        {hasStructuredEnrichment ? (
          <>
            <div className="openclaw-meta">
              <span className={`analysis-pill ${openClawCacheClass}`}>{openClawCacheLabel}</span>
              {!!enrichment.updated_at && (
                <span className="meta-chip">
                  Atualizado em {dateTimeLabel(enrichment.updated_at)}
                </span>
              )}
            </div>

            <div className="openclaw-overview">
              <section className="openclaw-overview-card">
                <span className="section-kicker">Humor do mercado</span>
                <strong className={openClawMoodClass}>
                  {openClawMoodLabel || 'Sem leitura estruturada'}
                </strong>
              </section>

              <section className="openclaw-overview-card">
                <span className="section-kicker">Ação sugerida</span>
                <strong className={decisionActionClass}>{openClawActionLabel}</strong>
              </section>

              <section className="openclaw-overview-card">
                <span className="section-kicker">Leitura da posição</span>
                <strong className={decisionActionClass}>{positionDecision.action}</strong>
              </section>
            </div>

            <div className="openclaw-grid">
              {!!enrichmentPayload.resumo && (
                <section className="openclaw-section openclaw-section-wide openclaw-section-lead">
                  <span className="section-kicker">Resumo</span>
                  <p>{enrichmentPayload.resumo}</p>
                </section>
              )}

              {!!enrichmentPayload.modelo_de_negocio && (
                <section className="openclaw-section">
                  <span className="section-kicker">Modelo de negócio</span>
                  <p>{enrichmentPayload.modelo_de_negocio}</p>
                </section>
              )}

              {!!enrichmentPayload.visao_do_mercado && (
                <section className="openclaw-section">
                  <span className="section-kicker">Como o mercado está vendo</span>
                  <p>{enrichmentPayload.visao_do_mercado}</p>
                </section>
              )}

              {!!enrichmentPayload.justificativa_da_acao && (
                <section className="openclaw-section">
                  <span className="section-kicker">Justificativa da ação</span>
                  <p>{enrichmentPayload.justificativa_da_acao}</p>
                </section>
              )}

              {!!enrichmentPayload.dividendos && (
                <section className="openclaw-section">
                  <span className="section-kicker">Dividendos</span>
                  <p>{enrichmentPayload.dividendos}</p>
                </section>
              )}

              {!!enrichmentPayload.observacoes && (
                <section className="openclaw-section">
                  <span className="section-kicker">Observações</span>
                  <p>{enrichmentPayload.observacoes}</p>
                </section>
              )}

              {Array.isArray(enrichmentPayload.tese) && enrichmentPayload.tese.length > 0 && (
                <section className="openclaw-section openclaw-section-list">
                  <span className="section-kicker">Tese</span>
                  <ul className="analysis-list">
                    {enrichmentPayload.tese.map((item, idx) => (
                      <li key={`tese-${idx}`}>{item}</li>
                    ))}
                  </ul>
                </section>
              )}

              {Array.isArray(enrichmentPayload.riscos) && enrichmentPayload.riscos.length > 0 && (
                <section className="openclaw-section openclaw-section-list">
                  <span className="section-kicker">Riscos</span>
                  <ul className="analysis-list">
                    {enrichmentPayload.riscos.map((item, idx) => (
                      <li key={`risco-${idx}`}>{item}</li>
                    ))}
                  </ul>
                </section>
              )}
            </div>
          </>
        ) : !!rawEnrichmentReply && !hasTransientRawReply ? (
          <>
            {!!enrichment.updated_at && <p className="subtitle">Atualizado em: {dateTimeLabel(enrichment.updated_at)}</p>}
            <p><strong>Resposta bruta do OpenClaw:</strong></p>
            <pre style={{ whiteSpace: 'pre-wrap', margin: 0 }}>{rawEnrichmentReply}</pre>
          </>
        ) : (
          <p className="subtitle">Sem enriquecimento ainda.</p>
        )}
      </article>

      <article className="card detail-card asset-terminal-panel">
        <div className="hero-line">
          <div>
            <h2>Histórico de leitura</h2>
            <p className="subtitle">Evolução das leituras do OpenClaw para este ativo.</p>
          </div>
        </div>

        {enrichmentHistory.length > 0 ? (
          <div className="table-wrap">
            <table className="asset-table history-table">
              <thead>
                <tr>
                  <th scope="col">Data</th>
                  <th scope="col">Preço</th>
                  <th scope="col">Humor</th>
                  <th scope="col">Ação</th>
                  <th scope="col">Resumo</th>
                </tr>
              </thead>
              <tbody>
                {enrichmentHistory.map((entry) => {
                  const entryPayload = entry?.payload && typeof entry.payload === 'object' ? entry.payload : {}
                  const entryMood = String(entry.mood || entryPayload.humor_do_mercado || '').trim() || 'Sem leitura'
                  const entryAction = structuredActionLabel(entry.suggested_action || entryPayload.acao_sugerida) || String(entry.suggested_action || entryPayload.acao_sugerida || '').trim() || 'Sem sinal'
                  const entrySummary = shortText(entryPayload.resumo || entryPayload.visao_do_mercado || entry.raw_reply || 'Sem resumo salvo.')
                  return (
                    <tr key={entry.id || `${entry.created_at}-${entryMood}-${entryAction}`}>
                      <td>{dateTimeLabel(entry.created_at)}</td>
                      <td>{Number(entry.price_at_update || 0) > 0 ? brl(entry.price_at_update) : '-'}</td>
                      <td>{entryMood}</td>
                      <td>{entryAction}</td>
                      <td className="history-summary-cell">{entrySummary}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="subtitle">Sem histórico salvo ainda. Ele passa a ser criado nas próximas atualizações do OpenClaw.</p>
        )}
      </article>

      <article className="card detail-card asset-terminal-panel">
        <h2>Proventos futuros (estimativa)</h2>
        <p className="subtitle">Fonte: Yahoo Finance (yfinance). Datas e valores podem mudar até a data com.</p>
        {upcomingIncomes.length > 0 ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th scope="col">Data com (ex)</th>
                  <th scope="col">Pagamento</th>
                  <th scope="col">Valor por cota</th>
                  <th scope="col">Moeda</th>
                  <th scope="col">Fonte</th>
                </tr>
              </thead>
              <tbody>
                {upcomingIncomes.map((item, idx) => (
                  <tr key={`upcoming-income-${item.ex_date || 'na'}-${item.payment_date || 'na'}-${idx}`}>
                    <td>{dateBr(item.ex_date)}</td>
                    <td>{dateBr(item.payment_date)}</td>
                    <td>{money(item.amount, item.currency)}</td>
                    <td>{String(item.currency || '').trim().toUpperCase() || '-'}</td>
                    <td>{String(item.source || '').trim() || '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="subtitle">Nenhum provento futuro encontrado para este ativo neste momento.</p>
        )}
      </article>

      <section className="asset-kpi-section">
        <h2 className="asset-kpi-section-title">Proventos recebidos</h2>
        <div className="table-wrap asset-ledger-table">
          <table>
            <thead>
              <tr>
                <th scope="col">Carteira</th>
                <th scope="col">Data</th>
                <th scope="col">Tipo</th>
                <th scope="col">Valor</th>
              </tr>
            </thead>
            <tbody>
              {incomes.map((item, idx) => (
                <tr key={`${item.date}-${item.income_type}-${idx}`}>
                  <td>{item.portfolio_name}</td>
                  <td>{dateBr(item.date)}</td>
                  <td>{String(item.income_type || '').toUpperCase()}</td>
                  <td>{brl(item.amount)}</td>
                </tr>
              ))}
              {incomes.length === 0 && (
                <tr>
                  <td colSpan={4}>Esse ativo ainda não possui proventos.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="asset-kpi-section">
        <h2 className="asset-kpi-section-title">Transações</h2>
        <div className="table-wrap asset-ledger-table">
          <table>
            <thead>
              <tr>
                <th scope="col">Carteira</th>
                <th scope="col">Data</th>
                <th scope="col">Tipo</th>
                <th scope="col">Qtd</th>
                <th scope="col">Preço</th>
                <th scope="col">Total</th>
              </tr>
            </thead>
            <tbody>
              {transactions.map((tx, idx) => (
                <tr key={`${tx.date}-${tx.tx_type}-${idx}`}>
                  <td>{tx.portfolio_name}</td>
                  <td>{dateBr(tx.date)}</td>
                  <td>{tx.tx_type === 'buy' ? 'Compra' : 'Venda'}</td>
                  <td>{qty(tx.shares)}</td>
                  <td>{brl(tx.price)}</td>
                  <td>{brl(tx.total_value)}</td>
                </tr>
              ))}
              {transactions.length === 0 && (
                <tr>
                  <td colSpan={6}>Esse ativo ainda não possui transações.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  )
}

export default AssetPage
