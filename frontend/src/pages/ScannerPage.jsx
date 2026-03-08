import { useEffect, useMemo, useState } from 'react'
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Paper,
  Typography,
} from '@mui/material'
import { apiGet, apiPost } from '../api'
import { currentBrowserTimeZone, formatDateTimeLocal } from '../datetime'

function toNumberOrNull(value) {
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function formatFloat(value, digits = 2) {
  const num = Number(value)
  return Number.isFinite(num) ? num.toFixed(digits) : '-'
}

function formatPercent(value, digits = 2) {
  const num = Number(value)
  return Number.isFinite(num) ? `${num.toFixed(digits)}%` : '-'
}

function formatDecimal(value, digits = 2) {
  const num = Number(value)
  return Number.isFinite(num) ? num.toFixed(digits) : ''
}

function getRiskRewardRatio(signal) {
  const value = signal?.trade_levels?.risk_reward_ratio
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function getPotentialGainPct(signal) {
  const value = signal?.trade_levels?.potential_gain_pct
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function hasMetricKeyword(signal, keyword) {
  const needle = String(keyword || '').trim().toLowerCase()
  if (!needle) return false
  return (signal?.metrics_triggered || []).some((metric) => String(metric || '').toLowerCase().includes(needle))
}

function matchesSetup(signal, setupKey) {
  if (!setupKey || setupKey === 'all') return true
  const hasBreakout = hasMetricKeyword(signal, 'breakout')
  const hasVolume = hasMetricKeyword(signal, 'volume')
  const hasTrend = hasMetricKeyword(signal, 'trend')
  const hasMomentum = hasMetricKeyword(signal, 'momentum')
  const hasRelativeStrength = hasMetricKeyword(signal, 'relative strength') || hasMetricKeyword(signal, 'ibov')
  const hasVolCompression = hasMetricKeyword(signal, 'volatility compression')

  if (setupKey === 'setup_1') return hasBreakout && hasVolume && hasTrend
  if (setupKey === 'setup_2') return hasMomentum && hasRelativeStrength && hasVolume
  if (setupKey === 'setup_3') return hasVolCompression && hasBreakout
  return true
}

function sortSignals(signals, sortKey) {
  const sorted = [...(signals || [])]
  sorted.sort((left, right) => {
    const leftTicker = String(left?.ticker || '')
    const rightTicker = String(right?.ticker || '')
    const leftScore = Number(left?.score || 0)
    const rightScore = Number(right?.score || 0)
    const leftCount = Array.isArray(left?.metrics_triggered) ? left.metrics_triggered.length : 0
    const rightCount = Array.isArray(right?.metrics_triggered) ? right.metrics_triggered.length : 0

    if (sortKey === 'score_desc') {
      return rightScore - leftScore || leftTicker.localeCompare(rightTicker)
    }
    if (sortKey === 'gain_pct_desc') {
      return (getPotentialGainPct(right) || -1) - (getPotentialGainPct(left) || -1)
        || rightScore - leftScore
        || leftTicker.localeCompare(rightTicker)
    }
    if (sortKey === 'rr_desc') {
      return (getRiskRewardRatio(right) || -1) - (getRiskRewardRatio(left) || -1)
        || rightScore - leftScore
        || leftTicker.localeCompare(rightTicker)
    }
    if (sortKey === 'ticker_asc') {
      return leftTicker.localeCompare(rightTicker)
    }
    return rightCount - leftCount
      || rightScore - leftScore
      || ((getRiskRewardRatio(right) || -1) - (getRiskRewardRatio(left) || -1))
      || leftTicker.localeCompare(rightTicker)
  })
  return sorted
}

function buildMatrixFromSignals(signals) {
  const metricFrequency = {}
  ;(signals || []).forEach((signal) => {
    ;(signal?.metrics_triggered || []).forEach((metric) => {
      const key = String(metric || '').trim()
      if (!key) return
      metricFrequency[key] = (metricFrequency[key] || 0) + 1
    })
  })
  const columns = Object.keys(metricFrequency).sort((left, right) => (
    metricFrequency[right] - metricFrequency[left] || left.localeCompare(right)
  ))
  const rows = (signals || []).map((signal) => ({
    ticker: signal?.ticker,
    triggered_count: Array.isArray(signal?.metrics_triggered) ? signal.metrics_triggered.length : 0,
    score: signal?.score,
    cells: Object.fromEntries(columns.map((column) => [column, (signal?.metrics_triggered || []).includes(column)])),
  }))
  return { columns, rows }
}

function ScannerPage() {
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const [signals, setSignals] = useState([])
  const [signalMatrix, setSignalMatrix] = useState({ columns: [], rows: [] })
  const [trades, setTrades] = useState({ active: [], history: [], summary: {} })
  const [tickerLookup, setTickerLookup] = useState('')
  const [tickerDetails, setTickerDetails] = useState(null)
  const [lookupLoading, setLookupLoading] = useState(false)
  const [createForm, setCreateForm] = useState({
    ticker: '',
    quantity: '1',
    investedAmount: '',
    notes: '',
  })
  const [filters, setFilters] = useState({
    ticker: '',
    rr: 'all',
    sort: 'count_desc',
    setup: 'all',
  })
  const [tradeModalOpen, setTradeModalOpen] = useState(false)
  const [tradeModalSubmitting, setTradeModalSubmitting] = useState(false)
  const [tradeModalPricingMode, setTradeModalPricingMode] = useState('price')
  const [tradeModalForm, setTradeModalForm] = useState({
    ticker: '',
    quantity: '1',
    executionPrice: '',
    investedAmount: '',
    notes: '',
    signalPrice: null,
    signalScore: null,
  })
  const [matrixExpanded, setMatrixExpanded] = useState(false)
  const browserTimeZone = currentBrowserTimeZone()

  const loadScanner = async (background = false) => {
    if (background) setRefreshing(true)
    else setLoading(true)
    setError('')
    try {
      const [signalsPayload, matrixPayload, tradesPayload] = await Promise.all([
        apiGet('/api/scanner/signals'),
        apiGet('/api/scanner/signal-matrix'),
        apiGet('/api/scanner/trades'),
      ])
      setSignals(Array.isArray(signalsPayload) ? signalsPayload : [])
      setSignalMatrix(matrixPayload || { columns: [], rows: [] })
      setTrades(tradesPayload || { active: [], history: [], summary: {} })
    } catch (err) {
      setError(err.message)
    } finally {
      if (background) setRefreshing(false)
      else setLoading(false)
    }
  }

  useEffect(() => {
    loadScanner(false)
  }, [])

  const summary = useMemo(() => ({
    activeSignals: Array.isArray(signals) ? signals.length : 0,
    matrixTickers: Array.isArray(signalMatrix?.rows) ? signalMatrix.rows.length : 0,
    openTrades: Number(trades?.summary?.open || 0),
    successTrades: Number(trades?.summary?.success || 0),
    failureTrades: Number(trades?.summary?.failure || 0),
    manualClosedTrades:
      Number(trades?.summary?.closed_profit || 0) + Number(trades?.summary?.closed_loss || 0),
  }), [signals, signalMatrix, trades])

  const trackedTickerSet = useMemo(() => (
    new Set((Array.isArray(trades?.tracked_tickers) ? trades.tracked_tickers : []).map((item) => String(item || '').toUpperCase()))
  ), [trades?.tracked_tickers])

  const highlightScore = useMemo(() => {
    const scoreValues = (signals || [])
      .map((item) => Number(item?.score))
      .filter((value) => Number.isFinite(value))
    if (!scoreValues.length) return 60
    const localMax = Math.max(...scoreValues)
    return Math.max(55, Math.min(80, localMax - 3))
  }, [signals])

  const openTradeModal = (signal) => {
    const value = String(signal?.ticker || '').trim().toUpperCase()
    if (!value) return
    if (trackedTickerSet.has(value)) return
    const signalPrice = toNumberOrNull(signal?.price)
    setTradeModalForm({
      ticker: value,
      quantity: '1',
      executionPrice: signalPrice != null ? formatDecimal(signalPrice, 4) : '',
      investedAmount: signalPrice != null ? formatDecimal(signalPrice, 2) : '',
      notes: '',
      signalPrice,
      signalScore: toNumberOrNull(signal?.score),
    })
    setTradeModalPricingMode('price')
    setTradeModalOpen(true)
    setError('')
  }

  const closeTradeModal = () => {
    if (tradeModalSubmitting) return
    setTradeModalOpen(false)
    setTradeModalPricingMode('price')
    setTradeModalForm({
      ticker: '',
      quantity: '1',
      executionPrice: '',
      investedAmount: '',
      notes: '',
      signalPrice: null,
      signalScore: null,
    })
  }

  const onCreateTradeFromModal = async () => {
    const ticker = String(tradeModalForm.ticker || '').trim().toUpperCase()
    const quantity = toNumberOrNull(tradeModalForm.quantity)
    const investedAmount = toNumberOrNull(tradeModalForm.investedAmount)
    if (!ticker) {
      setError('Ticker inválido para abrir trade.')
      return
    }
    if (!quantity || quantity <= 0) {
      setError('Quantidade inválida.')
      return
    }
    if (!investedAmount || investedAmount <= 0) {
      setError('Valor investido inválido.')
      return
    }
    setTradeModalSubmitting(true)
    setError('')
    setMessage('')
    try {
      const payload = {
        ticker,
        quantity,
        invested_amount: investedAmount,
        notes: String(tradeModalForm.notes || '').trim(),
      }
      await apiPost('/api/scanner/trades', payload)
      setTradeModalOpen(false)
      setTradeModalPricingMode('price')
      setCreateForm((current) => ({ ...current, ticker }))
      setMessage(`Trade criado para ${ticker}.`)
      await loadScanner(true)
    } catch (err) {
      setError(err.message)
    } finally {
      setTradeModalSubmitting(false)
    }
  }

  const updateTradeModalValues = (changedField, rawValue, modeOverride = null) => {
    setTradeModalForm((current) => {
      const next = { ...current }
      if (changedField === 'quantity') next.quantity = rawValue
      if (changedField === 'execution_price') next.executionPrice = rawValue
      if (changedField === 'invested_amount') next.investedAmount = rawValue

      const pricingMode = modeOverride || tradeModalPricingMode
      const quantity = toNumberOrNull(next.quantity)
      const price = toNumberOrNull(next.executionPrice)
      const investedAmount = toNumberOrNull(next.investedAmount)

      if (changedField === 'invested_amount') {
        if (quantity && quantity > 0 && investedAmount && investedAmount > 0) {
          next.executionPrice = formatDecimal(investedAmount / quantity, 4)
        }
        return next
      }

      if (changedField === 'execution_price') {
        if (quantity && quantity > 0 && price && price > 0) {
          next.investedAmount = formatDecimal(quantity * price, 2)
        }
        return next
      }

      if (changedField === 'quantity' && quantity && quantity > 0) {
        if (pricingMode === 'invested_amount' && investedAmount && investedAmount > 0) {
          next.executionPrice = formatDecimal(investedAmount / quantity, 4)
        } else if (price && price > 0) {
          next.investedAmount = formatDecimal(quantity * price, 2)
        }
      }

      return next
    })
  }

  const filteredSignals = useMemo(() => {
    const query = String(filters.ticker || '').trim().toUpperCase()
    const rrRaw = String(filters.rr || 'all')
    const rrMin = Number(rrRaw)
    const setupKey = String(filters.setup || 'all')
    const next = (signals || []).filter((signal) => {
      const ticker = String(signal?.ticker || '').toUpperCase()
      const ratio = getRiskRewardRatio(signal)
      const matchesTicker = !query || ticker.includes(query)
      const matchesRr = Number.isNaN(rrMin) || !rrMin || (ratio != null && ratio > rrMin)
      return matchesTicker && matchesRr && matchesSetup(signal, setupKey)
    })
    return sortSignals(next, String(filters.sort || 'count_desc'))
  }, [signals, filters])

  const visibleMatrix = useMemo(() => buildMatrixFromSignals(filteredSignals), [filteredSignals])

  const onLookupTicker = async (event) => {
    event.preventDefault()
    const ticker = String(tickerLookup || '').trim().toUpperCase()
    if (!ticker) return
    setLookupLoading(true)
    setError('')
    setMessage('')
    try {
      const payload = await apiGet(`/api/scanner/ticker/${encodeURIComponent(ticker)}`)
      setTickerDetails(payload || null)
    } catch (err) {
      setError(err.message)
      setTickerDetails(null)
    } finally {
      setLookupLoading(false)
    }
  }

  const onCreateTrade = async (event) => {
    event.preventDefault()
    const ticker = String(createForm.ticker || '').trim().toUpperCase()
    if (!ticker) {
      setError('Informe o ticker para abrir trade.')
      return
    }
    setError('')
    setMessage('')
    try {
      const payload = {
        ticker,
        quantity: toNumberOrNull(createForm.quantity) || 1,
        notes: String(createForm.notes || '').trim(),
      }
      const investedAmount = toNumberOrNull(createForm.investedAmount)
      if (investedAmount && investedAmount > 0) payload.invested_amount = investedAmount

      await apiPost('/api/scanner/trades', payload)
      setMessage(`Trade criado para ${ticker}.`)
      setCreateForm((current) => ({ ...current, notes: '' }))
      await loadScanner(true)
    } catch (err) {
      setError(err.message)
    }
  }

  if (loading) {
    return <p>Carregando scanner...</p>
  }

  return (
    <section className="scanner-page">
      <div className="hero-line">
        <div>
          <h1>Scanner</h1>
          <p className="subtitle">Leitura do Market Scanner integrada ao portal principal.</p>
        </div>
        <div className="hero-actions">
          <Button variant="contained" onClick={() => loadScanner(true)} disabled={refreshing}>
            {refreshing ? 'Atualizando...' : 'Atualizar'}
          </Button>
        </div>
      </div>

      {!!message && <p className="notice-ok">{message}</p>}
      {!!error && <p className="notice-warn">{error}</p>}

      <Typography variant="caption" sx={{ mb: 1.5, display: 'block', opacity: 0.7 }}>
        Horários exibidos no seu fuso: {browserTimeZone}
      </Typography>

      <div className="cards">
        <article className="card">
          <h3>Sinais ativos</h3>
          <p>{summary.activeSignals}</p>
          <small>Tickers com sinal recente.</small>
        </article>
        <article className="card">
          <h3>Trades abertos</h3>
          <p>{summary.openTrades}</p>
          <small>Posições em acompanhamento.</small>
        </article>
        <article className="card">
          <h3>Fechadas (gain/loss)</h3>
          <p>{summary.manualClosedTrades}</p>
          <small>Encerradas manualmente.</small>
        </article>
        <article className="card">
          <h3>Matriz</h3>
          <p>{summary.matrixTickers}</p>
          <small>Tickers na matriz de métricas.</small>
        </article>
      </div>

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Pesquisas e filtros</Typography>
        <div className="scanner-filter-grid">
          <label className="auth-field">
            <span>Filtrar ticker</span>
            <input
              value={filters.ticker}
              onChange={(event) => setFilters((current) => ({ ...current, ticker: event.target.value }))}
              placeholder="Ex: PETR, VALE, WEGE"
            />
          </label>
          <label className="auth-field">
            <span>Faixa RR</span>
            <select value={filters.rr} onChange={(event) => setFilters((current) => ({ ...current, rr: event.target.value }))}>
              <option value="all">Todos</option>
              <option value="2">RR &gt; 2.0</option>
              <option value="3">RR &gt; 3.0</option>
              <option value="4">RR &gt; 4.0</option>
            </select>
          </label>
          <label className="auth-field">
            <span>Ordenar por</span>
            <select value={filters.sort} onChange={(event) => setFilters((current) => ({ ...current, sort: event.target.value }))}>
              <option value="count_desc">Mais sinais</option>
              <option value="score_desc">Maior score</option>
              <option value="gain_pct_desc">Maior ganho %</option>
              <option value="rr_desc">Maior RR</option>
              <option value="ticker_asc">Ticker A-Z</option>
            </select>
          </label>
          <label className="auth-field">
            <span>Setup</span>
            <select value={filters.setup} onChange={(event) => setFilters((current) => ({ ...current, setup: event.target.value }))}>
              <option value="all">Todos</option>
              <option value="setup_1">Setup 1: Breakout + Volume + Trend</option>
              <option value="setup_2">Setup 2: Momentum + Relative + Volume</option>
              <option value="setup_3">Setup 3: Volatility Compression + Breakout</option>
            </select>
          </label>
        </div>
      </Paper>

      <Accordion
        expanded={matrixExpanded}
        onChange={(_, expanded) => setMatrixExpanded(expanded)}
        className="admin-panel scanner-matrix-panel"
        sx={{ mb: 2 }}
      >
        <AccordionSummary expandIcon={<span>{matrixExpanded ? '▴' : '▾'}</span>}>
          <div>
            <Typography variant="h6">Signal Matrix ({visibleMatrix?.rows?.length || 0})</Typography>
            <Typography variant="body2" sx={{ opacity: 0.75 }}>
              Ticker x métricas ativadas (após filtros).
            </Typography>
          </div>
        </AccordionSummary>
        <AccordionDetails>
          {!Array.isArray(visibleMatrix?.rows) || visibleMatrix.rows.length === 0 ? (
            <p>Sem dados de matriz no momento.</p>
          ) : (
            <div className="table-wrap scanner-matrix-wrap">
              <table className="asset-table scanner-matrix-table">
                <thead>
                  <tr>
                    <th>Ticker</th>
                    <th>Count</th>
                    <th>Score</th>
                    {(visibleMatrix.columns || []).map((column) => (
                      <th key={column}>{column}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {visibleMatrix.rows.map((row) => (
                    <tr key={row.ticker}>
                      <td>{row.ticker}</td>
                      <td>{row.triggered_count}</td>
                      <td>{formatFloat(row.score)}</td>
                      {(visibleMatrix.columns || []).map((column) => (
                        <td key={`${row.ticker}-${column}`} className={row?.cells?.[column] ? 'scanner-matrix-hit' : 'scanner-matrix-miss'}>
                          {row?.cells?.[column] ? '●' : '·'}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </AccordionDetails>
      </Accordion>

      <section className="scanner-signal-grid">
        {!filteredSignals.length ? (
          <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
            <Typography variant="h6" sx={{ mb: 1 }}>Sinais</Typography>
            <p>Nenhum card corresponde aos filtros atuais.</p>
          </Paper>
        ) : (
          filteredSignals.map((signal) => {
            const tradeLevels = signal?.trade_levels || {}
            const entryLow = tradeLevels?.entry_region?.low
            const entryHigh = tradeLevels?.entry_region?.high
            const rr = tradeLevels?.risk_reward_ratio
            const signalScore = Number(signal?.score)
            return (
              <article
                key={`${signal.ticker}-${signal.created_at || signal.timestamp}`}
                className={`scanner-signal-tile ${signalScore >= highlightScore ? 'scanner-signal-tile--highlight' : ''}`}
              >
                <header className="scanner-signal-header">
                  <strong>{signal.ticker}</strong>
                  <span className="scanner-buy-badge">BUY</span>
                </header>
                <div className="scanner-signal-stats">
                  <div>
                    <small>Preço</small>
                    <strong>{formatFloat(signal.price)}</strong>
                  </div>
                  <div>
                    <small>Score</small>
                    <strong>{formatFloat(signal.score)}</strong>
                  </div>
                </div>
                <div className="scanner-trade-plan">
                  <p>
                    <span>Entrada:</span> {formatFloat(entryLow)} - {formatFloat(entryHigh)}
                  </p>
                  <p>
                    <span>Objetivo:</span> {formatFloat(tradeLevels?.objective_price)} ({formatPercent(tradeLevels?.potential_gain_pct)})
                  </p>
                  <p>
                    <span>Stop:</span> {formatFloat(tradeLevels?.stop_price)} ({formatPercent(tradeLevels?.risk_pct)})
                  </p>
                  <p>
                    <span>Risco/Retorno:</span> {rr != null ? `1:${formatFloat(rr)}` : 'N/A'}
                  </p>
                </div>
                <div className="scanner-metrics">
                  {(Array.isArray(signal.metrics_triggered) ? signal.metrics_triggered : []).slice(0, 8).map((metric) => (
                    <span key={`${signal.ticker}-${metric}`} className="scanner-metric-chip">{metric}</span>
                  ))}
                  {Array.isArray(signal.metrics_triggered) && signal.metrics_triggered.length > 8 && (
                    <span className="scanner-metric-chip">+{signal.metrics_triggered.length - 8}</span>
                  )}
                </div>
                <div className="scanner-signal-actions">
                  <Button
                    size="small"
                    variant="contained"
                    onClick={() => openTradeModal(signal)}
                    disabled={trackedTickerSet.has(String(signal?.ticker || '').toUpperCase())}
                  >
                    {trackedTickerSet.has(String(signal?.ticker || '').toUpperCase()) ? 'Acompanhando' : 'Comprei'}
                  </Button>
                  <small>{formatDateTimeLocal(signal.created_at || signal.timestamp)}</small>
                </div>
              </article>
            )
          })
        )}
      </section>

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Abrir trade</Typography>
        <form onSubmit={onCreateTrade} className="form-grid">
          <label className="auth-field">
            <span>Ticker</span>
            <input
              value={createForm.ticker}
              onChange={(event) => setCreateForm((current) => ({ ...current, ticker: event.target.value }))}
              placeholder="PETR4"
            />
          </label>
          <label className="auth-field">
            <span>Quantidade</span>
            <input
              value={createForm.quantity}
              onChange={(event) => setCreateForm((current) => ({ ...current, quantity: event.target.value }))}
              inputMode="decimal"
            />
          </label>
          <label className="auth-field">
            <span>Investido (opcional)</span>
            <input
              value={createForm.investedAmount}
              onChange={(event) => setCreateForm((current) => ({ ...current, investedAmount: event.target.value }))}
              inputMode="decimal"
              placeholder="1000.00"
            />
          </label>
          <label className="auth-field">
            <span>Notas</span>
            <input
              value={createForm.notes}
              onChange={(event) => setCreateForm((current) => ({ ...current, notes: event.target.value }))}
              placeholder="Entrada por sinal forte"
            />
          </label>
          <div>
            <Button type="submit" variant="contained">Criar trade</Button>
          </div>
        </form>
      </Paper>

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Consulta por ticker</Typography>
        <form onSubmit={onLookupTicker} className="hero-actions">
          <input
            className="app-v2-search"
            style={{ maxWidth: 240, color: 'inherit', borderColor: 'rgba(0,0,0,0.2)' }}
            value={tickerLookup}
            onChange={(event) => setTickerLookup(event.target.value)}
            placeholder="Ticker (ex: VALE3)"
          />
          <Button type="submit" variant="outlined" disabled={lookupLoading}>
            {lookupLoading ? 'Consultando...' : 'Consultar'}
          </Button>
        </form>
        {tickerDetails && (
          <div className="table-wrap" style={{ marginTop: 12 }}>
            <table className="asset-table">
              <tbody>
                <tr>
                  <td>Ticker</td>
                  <td>{tickerDetails.ticker || '-'}</td>
                </tr>
                <tr>
                  <td>Preço atual</td>
                  <td>{tickerDetails.latest_price ?? '-'}</td>
                </tr>
                <tr>
                  <td>Último sinal</td>
                  <td>{tickerDetails.latest_signal ? `${tickerDetails.latest_signal.score} pts` : 'Sem sinal recente'}</td>
                </tr>
                <tr>
                  <td>Atualizado em</td>
                  <td>{formatDateTimeLocal(tickerDetails.latest_price_timestamp)}</td>
                </tr>
              </tbody>
            </table>
          </div>
        )}
      </Paper>

      <Dialog open={tradeModalOpen} onClose={closeTradeModal} fullWidth maxWidth="sm">
        <DialogTitle>Registrar entrada</DialogTitle>
        <DialogContent dividers>
          <div className="form-grid">
            <label className="auth-field">
              <span>Ticker</span>
              <input value={tradeModalForm.ticker} disabled />
            </label>
            <label className="auth-field">
              <span>Quantidade</span>
              <input
                value={tradeModalForm.quantity}
                onChange={(event) => {
                  updateTradeModalValues('quantity', event.target.value)
                }}
                inputMode="decimal"
              />
            </label>
            <label className="auth-field">
              <span>Preço de execução</span>
              <input
                value={tradeModalForm.executionPrice}
                onChange={(event) => {
                  setTradeModalPricingMode('price')
                  updateTradeModalValues('execution_price', event.target.value, 'price')
                }}
                inputMode="decimal"
              />
            </label>
            <label className="auth-field">
              <span>Valor investido</span>
              <input
                value={tradeModalForm.investedAmount}
                onChange={(event) => {
                  setTradeModalPricingMode('invested_amount')
                  updateTradeModalValues('invested_amount', event.target.value, 'invested_amount')
                }}
                inputMode="decimal"
                placeholder="Ex: 1000.00"
              />
            </label>
            <label className="auth-field" style={{ gridColumn: '1 / -1' }}>
              <span>Observações</span>
              <input
                value={tradeModalForm.notes}
                onChange={(event) => setTradeModalForm((current) => ({ ...current, notes: event.target.value }))}
                placeholder="Motivo, contexto da compra..."
              />
            </label>
          </div>
          <p className="subtitle" style={{ marginTop: 10 }}>
            Sinal atual: preço {formatFloat(tradeModalForm.signalPrice)} | score {formatFloat(tradeModalForm.signalScore)}
          </p>
        </DialogContent>
        <DialogActions>
          <Button onClick={closeTradeModal} disabled={tradeModalSubmitting}>Cancelar</Button>
          <Button onClick={onCreateTradeFromModal} variant="contained" disabled={tradeModalSubmitting}>
            {tradeModalSubmitting ? 'Criando...' : 'Confirmar compra'}
          </Button>
        </DialogActions>
      </Dialog>
    </section>
  )
}

export default ScannerPage
