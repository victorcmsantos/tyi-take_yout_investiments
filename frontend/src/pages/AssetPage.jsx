import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import 'chart.js/auto'
import { Line } from 'react-chartjs-2'
import { apiGet, apiPost } from '../api'
import { formatAgeFromNow, formatDateTimeLocal, parseApiDate } from '../datetime'

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

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`
const pct = (value) => `${Number(value || 0).toFixed(2)}%`
const signedPct = (value) => `${Number(value || 0) >= 0 ? '+' : ''}${Number(value || 0).toFixed(2)}%`
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
      return `Cotacao possivelmente antiga. Ultimo sucesso via ${source}. Candle: ${updatedAtLabel}. Idade: ${ageLabel}.`
    }
    return 'Cotacao sem sincronizacao recente confirmada.'
  }
  if (updatedAtLabel) {
    return `Ultima atualizacao confirmada via ${source}. Candle: ${updatedAtLabel}. Idade: ${ageLabel}.`
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

function normalizeSearchText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
}

function inferMarketMoodLabel(value) {
  const text = normalizeSearchText(value)
  if (!text) {
    return { label: 'Sem leitura', tone: 'neutral', score: 0 }
  }

  let score = 0
  const positiveMarkers = [
    'positivo',
    'otimista',
    'confianca',
    'resiliente',
    'solido',
    'barato',
    'atrativo',
    'desconto',
    'bom momento',
    'favoravel',
    'crescimento',
    'melhora',
    'forte',
  ]
  const negativeMarkers = [
    'negativo',
    'cautela',
    'pressionado',
    'caro',
    'esticado',
    'risco',
    'incerteza',
    'fraco',
    'desaceleracao',
    'desaceleração',
    'volatil',
    'volatilidade',
    'piora',
    'desafiador',
  ]

  positiveMarkers.forEach((marker) => {
    if (text.includes(marker)) score += 1
  })
  negativeMarkers.forEach((marker) => {
    if (text.includes(marker)) score -= 1
  })

  if (score >= 2) {
    return { label: 'Mercado com viés positivo', tone: 'positive', score }
  }
  if (score <= -2) {
    return { label: 'Mercado com viés cauteloso', tone: 'negative', score }
  }
  return { label: 'Mercado sem direção forte', tone: 'neutral', score }
}

function normalizeStructuredMarketMood(value) {
  const text = normalizeSearchText(value)
  if (!text) return ''
  if (text.includes('positivo') || text.includes('favoravel') || text.includes('otimista')) return 'positive'
  if (text.includes('cauteloso') || text.includes('negativo') || text.includes('pessimista')) return 'negative'
  if (text.includes('neutro')) return 'neutral'
  return ''
}

function marketMoodPresentation(structuredMood, marketView) {
  const normalized = normalizeStructuredMarketMood(structuredMood)
  if (normalized === 'positive') {
    return { label: 'Mercado com viés positivo', tone: 'positive', score: 2 }
  }
  if (normalized === 'negative') {
    return { label: 'Mercado com viés cauteloso', tone: 'negative', score: -2 }
  }
  if (normalized === 'neutral') {
    return { label: 'Mercado sem direção forte', tone: 'neutral', score: 0 }
  }
  return inferMarketMoodLabel(marketView)
}

function normalizeStructuredAction(value) {
  const text = normalizeSearchText(value)
  if (!text) return ''
  if (text.includes('comprar_mais') || text.includes('comprar mais') || text === 'compra') return 'buy_more'
  if (text.includes('segurar') || text.includes('manter')) return 'hold'
  if (text.includes('reduzir') || text.includes('vender')) return 'reduce'
  if (text.includes('observar') || text.includes('aguardar') || text.includes('monitorar')) return 'watch'
  return ''
}

function structuredActionLabel(value) {
  const normalized = normalizeStructuredAction(value)
  if (normalized === 'buy_more') return 'Comprar mais'
  if (normalized === 'hold') return 'Segurar'
  if (normalized === 'reduce') return 'Vender ou reduzir'
  if (normalized === 'watch') return 'Observar'
  return ''
}

function buildPositionDecision({ asset, position, enrichmentPayload }) {
  const currentPrice = Number(asset?.price || 0)
  const avgPrice = Number(position?.avg_price || 0)
  const shares = Number(position?.shares || 0)
  const openPnlPct = Number(position?.open_pnl_pct || 0)
  const marketView = String(enrichmentPayload?.visao_do_mercado || '').trim()
  const structuredMood = String(enrichmentPayload?.humor_do_mercado || '').trim()
  const openClawAction = String(enrichmentPayload?.acao_sugerida || '').trim()
  const openClawActionWhy = String(enrichmentPayload?.justificativa_da_acao || '').trim()
  const mood = marketMoodPresentation(structuredMood, marketView)
  const priceGapPct = avgPrice > 0 ? ((currentPrice - avgPrice) / avgPrice) * 100 : null
  const reasons = []

  if (structuredMood) {
    reasons.push(`OpenClaw classificou o humor como ${structuredMood}`)
  } else if (marketView) {
    reasons.push(mood.label)
  } else {
    reasons.push('OpenClaw ainda nao trouxe leitura de mercado')
  }

  if (openClawAction) {
    reasons.push(`OpenClaw sugeriu ${structuredActionLabel(openClawAction) || openClawAction}`)
  }

  if (avgPrice > 0 && priceGapPct !== null) {
    const direction = priceGapPct >= 0 ? 'acima' : 'abaixo'
    reasons.push(`Preco atual ${Math.abs(priceGapPct).toFixed(2)}% ${direction} do seu preco medio`)
  }

  if (shares > 0) {
    const pnlDirection = openPnlPct >= 0 ? 'acima' : 'abaixo'
    reasons.push(`Posicao ${Math.abs(openPnlPct).toFixed(2)}% ${pnlDirection} do zero`)
  }

  if (shares <= 0 || avgPrice <= 0) {
    if (mood.tone === 'positive') {
      return {
        mood,
        action: 'Observar compra',
        actionTone: 'up',
        openClawActionLabel: structuredActionLabel(openClawAction),
        openClawActionWhy,
        reasons,
        priceGapPct,
        summary: 'O mercado parece construtivo, mas voce ainda nao tem preco medio relevante nessa posicao.',
      }
    }
    if (mood.tone === 'negative') {
      return {
        mood,
        action: 'Aguardar',
        actionTone: 'down',
        openClawActionLabel: structuredActionLabel(openClawAction),
        openClawActionWhy,
        reasons,
        priceGapPct,
        summary: 'A leitura atual nao sugere pressa para montar ou aumentar posicao.',
      }
    }
    return {
      mood,
      action: 'Monitorar',
      actionTone: '',
      openClawActionLabel: structuredActionLabel(openClawAction),
      openClawActionWhy,
      reasons,
      priceGapPct,
      summary: 'Sem uma posicao formada, faz mais sentido monitorar antes de agir.',
    }
  }

  if (mood.tone === 'positive') {
    if (priceGapPct <= -7) {
      return {
        mood,
        action: 'Comprar mais',
        actionTone: 'up',
        openClawActionLabel: structuredActionLabel(openClawAction),
        openClawActionWhy,
        reasons,
        priceGapPct,
        summary: 'O mercado segue favoravel e o preco esta abaixo do seu custo medio.',
      }
    }
    return {
      mood,
      action: 'Segurar',
      actionTone: '',
      openClawActionLabel: structuredActionLabel(openClawAction),
      openClawActionWhy,
      reasons,
      priceGapPct,
      summary: 'A leitura segue boa, mas o preco ja nao oferece desconto claro contra o seu custo medio.',
    }
  }

  if (mood.tone === 'negative') {
    if (priceGapPct >= 8 || openPnlPct >= 10) {
      return {
        mood,
        action: 'Vender ou reduzir',
        actionTone: 'down',
        openClawActionLabel: structuredActionLabel(openClawAction),
        openClawActionWhy,
        reasons,
        priceGapPct,
        summary: 'O humor do mercado piorou e a posicao ainda tem gordura para realizar ou reduzir risco.',
      }
    }
    return {
      mood,
      action: 'Segurar',
      actionTone: '',
      openClawActionLabel: structuredActionLabel(openClawAction),
      openClawActionWhy,
      reasons,
      priceGapPct,
      summary: 'A leitura esta mais cautelosa, mas o preco nao abre uma saida tao confortavel agora.',
    }
  }

  if (priceGapPct <= -10) {
    return {
      mood,
      action: 'Segurar',
      actionTone: '',
      openClawActionLabel: structuredActionLabel(openClawAction),
      openClawActionWhy,
      reasons,
      priceGapPct,
      summary: 'O preco caiu abaixo do seu medio, mas sem melhora clara no humor do mercado ainda faz sentido evitar aumentar no escuro.',
    }
  }

  if (priceGapPct >= 12 && openPnlPct > 0) {
    return {
      mood,
      action: 'Segurar',
      actionTone: '',
      openClawActionLabel: structuredActionLabel(openClawAction),
      openClawActionWhy,
      reasons,
      priceGapPct,
      summary: 'A posicao esta andando bem, mas sem sinal forte do mercado a leitura segue de manutencao.',
    }
  }

  return {
    mood,
    action: 'Segurar',
    actionTone: '',
    openClawActionLabel: structuredActionLabel(openClawAction),
    openClawActionWhy,
    reasons,
    priceGapPct,
    summary: 'Nao ha sinal forte o bastante para aumentar ou reduzir agora.',
  }
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
  const [syncMessage, setSyncMessage] = useState('')
  const [syncing, setSyncing] = useState(false)
  const [enriching, setEnriching] = useState(false)
  const [enrichMessage, setEnrichMessage] = useState('')

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
  }, [ticker, selectedPortfolioIds])

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
    try {
      const response = await apiPost(`/api/sync/market-data/${ticker}`, {})
      if (response?.success === false) {
        setSyncMessage(String(response?.message || 'Sincronizacao adiada: existe outro scan em andamento.'))
        return
      }
      const sourceLabel = String(response?.source || '').trim().toUpperCase()
      const updatedLabel = formatDateTimeLocal(response?.updated_at)
      if (sourceLabel && updatedLabel) {
        setSyncMessage(`Atualizacao concluida via ${sourceLabel} em ${updatedLabel}.`)
      } else if (sourceLabel) {
        setSyncMessage(`Atualizacao concluida via ${sourceLabel}.`)
      } else {
        setSyncMessage('Atualizacao concluida via scanner.')
      }
      const data = await apiGet(`/api/assets/${ticker}`, {
        portfolio_id: selectedPortfolioIds,
      })
      setPayload(data)
      setPriceHistoryRefreshKey((current) => current + 1)
    } catch (err) {
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
    ? 'Sem preco medio'
    : `${signedPct(positionDecision.priceGapPct)} vs preco medio`
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
    setEnrichMessage(hasAnyEnrichment ? 'Atualizando resumo automatico via OpenClaw...' : 'Gerando resumo automatico via OpenClaw...')

    ;(async () => {
      try {
        const data = await apiPost(`/api/assets/${tickerKey}/enrich/openclaw`)
        setEnrichMessage(data?.message || 'OK')
        setEnrichment(data?.enrichment || null)
        setPayload((current) => (current ? { ...current, enrichment_history: data?.enrichment_history || [] } : current))
      } catch (err) {
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
    setEnrichMessage('')
    try {
      const data = await apiPost(`/api/assets/${ticker}/enrich/openclaw`)
      setEnrichMessage(data?.message || 'OK')
      setEnrichment(data?.enrichment || null)
      setPayload((current) => (current ? { ...current, enrichment_history: data?.enrichment_history || [] } : current))
    } catch (err) {
      setEnrichMessage(err.message)
    } finally {
      setEnriching(false)
    }
  }

  const chartData = useMemo(() => ({
    labels: priceHistory.labels || [],
    datasets: [
      {
        label: 'Cotacao',
        data: priceHistory.prices || [],
        borderColor: '#a57f39',
        backgroundColor: 'rgba(165, 127, 57, 0.12)',
        fill: true,
        tension: 0.2,
        pointRadius: 0,
      },
    ],
  }), [priceHistory.labels, priceHistory.prices])

  if (loading) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>
  if (!payload) return <p>Sem dados.</p>

  return (
    <section>
      <div className="hero-actions">
        <Link to="/carteira" className="btn-primary btn-link">
          Voltar para Renda Variavel
        </Link>
      </div>

      <div className="hero-line">
        <h1>{asset.ticker} - {asset.name}</h1>
        <button type="button" className="btn-primary" onClick={onSyncTicker} disabled={syncing}>
          {syncing ? 'Atualizando...' : 'Atualizar market data'}
        </button>
      </div>
      <p className="subtitle">Setor: {asset.sector}</p>
      {!!syncMessage && <p className="notice-warn">{syncMessage}</p>}
      {!!marketDataSummary(marketData) && (
        <p className={marketData.is_stale ? 'notice-warn' : 'notice-ok'}>
          {marketDataSummary(marketData)}
          {marketData.last_error ? ` Ultimo erro: ${marketData.last_error}.` : ''}
        </p>
      )}

      <article className="card detail-card">
        <div className="hero-line">
          <h3>Cotacao {asset.ticker}</h3>
          <div className="range-tabs">
            {CHART_RANGES.map((opt) => (
              <button
                key={opt.key}
                type="button"
                className={`range-tab ${rangeKey === opt.key ? 'active' : ''}`}
                onClick={() => setRangeKey(opt.key)}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>
        <p>
          <strong>{brl(asset.price)}</strong>{' '}
          {priceHistory.change_pct !== null && priceHistory.change_pct !== undefined && (
            <span className={Number(priceHistory.change_pct || 0) >= 0 ? 'up' : 'down'}>
              {pct(priceHistory.change_pct)} (PERIODO {selectedRangeLabel})
            </span>
          )}
        </p>
        {priceHistoryLoading ? (
          <p className="subtitle">Carregando historico...</p>
        ) : (priceHistory.labels || []).length > 0 ? (
          <div className="chart-canvas-wrap">
            <Line data={chartData} options={{ responsive: true, maintainAspectRatio: false }} />
          </div>
        ) : (
          <p className="notice-warn">
            {priceHistoryError || 'Nao foi possivel carregar historico para este periodo.'}
          </p>
        )}
      </article>

      <div className="cards">
        <article className="card"><h3>Preco</h3><p>{brl(asset.price)}</p></article>
        <article className="card"><h3>Dividend Yield</h3><p>{pct(asset.dy)}</p></article>
        <article className="card"><h3>P/L</h3><p>{Number(asset.pl || 0).toFixed(2)}</p></article>
        <article className="card"><h3>P/VP</h3><p>{Number(asset.pvp || 0).toFixed(2)}</p></article>
      </div>

      <div className="cards">
        <article className="card"><h3>Quantidade em carteira</h3><p>{Number(position.shares || 0).toFixed(4)}</p></article>
        <article className="card"><h3>Preco medio</h3><p>{brl(position.avg_price)}</p></article>
        <article className="card"><h3>Valor total investido</h3><p>{brl(position.total_value)}</p></article>
        <article className="card"><h3>Valor de mercado da posicao</h3><p>{brl(position.market_value)}</p></article>
      </div>

      <div className="cards">
        <article className="card"><h3>Resultado em aberto (R$)</h3><p className={Number(position.open_pnl_value || 0) >= 0 ? 'up' : 'down'}>{brl(position.open_pnl_value)}</p></article>
        <article className="card"><h3>Resultado em aberto (%)</h3><p className={Number(position.open_pnl_pct || 0) >= 0 ? 'up' : 'down'}>{pct(position.open_pnl_pct)}</p></article>
        <article className="card"><h3>Proventos mes atual</h3><p>{brl(position.incomes_current_month)}</p></article>
        <article className="card"><h3>Proventos 3 meses</h3><p>{brl(position.incomes_3m)}</p></article>
        <article className="card"><h3>Proventos 12 meses</h3><p>{brl(position.incomes_12m)}</p></article>
        <article className="card"><h3>Proventos total</h3><p>{brl(position.total_incomes)}</p></article>
      </div>

      <article className="card detail-card">
        <h3>Resumo</h3>
        <p>Variacao no dia (provider): <strong className={Number(asset.variation_day || 0) >= 0 ? 'up' : 'down'}>{pct(asset.variation_day)}</strong></p>
        <p>Valor de mercado: R$ {Number(asset.market_cap_bi || 0).toFixed(2)} bi</p>
        <p>
          Fonte/candle: {' '}
          <strong>
            {(String(marketData?.source || '').trim().toUpperCase() || 'MARKET_SCANNER')}
            {' | '}
            {formatDateTimeLocal(marketData?.updated_at, '-')}
            {' | '}
            {formatAgeFromNow(marketData?.updated_at, '-')}
          </strong>
        </p>
      </article>

      <article className="card detail-card tactical-card">
        <div className="analysis-head">
          <div>
            <h3>Leitura tática</h3>
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
            <strong>{Number(position.avg_price || 0) > 0 ? brl(position.avg_price) : 'Nao calculado'}</strong>
          </div>
          <div className="analysis-strip-item">
            <span className="analysis-label">Resultado aberto</span>
            <strong className={Number(position.open_pnl_pct || 0) >= 0 ? 'up' : 'down'}>
              {pct(position.open_pnl_pct)}
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

      <article className="card detail-card openclaw-card">
        <div className="hero-line">
          <div>
            <h3>OpenClaw</h3>
            <p className="subtitle">Atualização automática a cada {enrichmentStaleDays} dia(s). O botão ignora o cache.</p>
          </div>
          <button type="button" className="btn-primary" onClick={onEnrichOpenClaw} disabled={enriching}>
            {enriching ? 'Atualizando...' : (hasSavedEnrichment ? 'Forcar atualizacao' : 'Gerar com OpenClaw')}
          </button>
        </div>

        {!!enrichMessage && (
          <p className={enrichMessage === 'OK' || enrichMessage.includes('OK') ? 'notice-ok' : 'notice-warn'}>
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

      <article className="card detail-card">
        <div className="hero-line">
          <div>
            <h3>Historico de leitura</h3>
            <p className="subtitle">Evolucao das leituras do OpenClaw para este ativo.</p>
          </div>
        </div>

        {enrichmentHistory.length > 0 ? (
          <div className="table-wrap">
            <table className="asset-table history-table">
              <thead>
                <tr>
                  <th>Data</th>
                  <th>Preco</th>
                  <th>Humor</th>
                  <th>Acao</th>
                  <th>Resumo</th>
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
          <p className="subtitle">Sem historico salvo ainda. Ele passa a ser criado nas proximas atualizacoes do OpenClaw.</p>
        )}
      </article>

      <article className="card detail-card">
        <h3>Proventos futuros (estimativa)</h3>
        <p className="subtitle">Fonte: Yahoo Finance (yfinance). Datas e valores podem mudar até a data com.</p>
        {upcomingIncomes.length > 0 ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Data com (ex)</th>
                  <th>Pagamento</th>
                  <th>Valor por cota</th>
                  <th>Moeda</th>
                  <th>Fonte</th>
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

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Carteira</th>
              <th>Data</th>
              <th>Tipo</th>
              <th>Valor</th>
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
                <td colSpan={4}>Esse ativo ainda nao possui proventos.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Carteira</th>
              <th>Data</th>
              <th>Tipo</th>
              <th>Qtd</th>
              <th>Preco</th>
              <th>Total</th>
            </tr>
          </thead>
          <tbody>
            {transactions.map((tx, idx) => (
              <tr key={`${tx.date}-${tx.tx_type}-${idx}`}>
                <td>{tx.portfolio_name}</td>
                <td>{dateBr(tx.date)}</td>
                <td>{tx.tx_type === 'buy' ? 'Compra' : 'Venda'}</td>
                <td>{Number(tx.shares || 0).toFixed(4)}</td>
                <td>{brl(tx.price)}</td>
                <td>{brl(tx.total_value)}</td>
              </tr>
            ))}
            {transactions.length === 0 && (
              <tr>
                <td colSpan={6}>Esse ativo ainda nao possui transacoes.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default AssetPage
