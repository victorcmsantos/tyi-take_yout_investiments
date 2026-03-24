import { useEffect, useMemo, useState } from 'react'
import 'chart.js/auto'
import { Button, Paper, Typography } from '@mui/material'
import { Bar, Doughnut, Line } from 'react-chartjs-2'
import { apiGet, apiPatch, apiPost } from '../api'
import { currentBrowserTimeZone, formatAgeFromNow, formatDateTimeLocal } from '../datetime'
import { formatCurrencyBRL, formatDecimal, formatPercent, formatQuantity, toFiniteNumber } from '../formatters'
import { emitAppToast } from '../toast'

const CHART_TEXT = '#8096ad'
const CHART_GRID = 'rgba(128, 150, 173, 0.16)'
const CHART_MONO = 'IBM Plex Mono, SFMono-Regular, monospace'
const POSITIVE_COLOR = '#18b88a'
const NEGATIVE_COLOR = '#d85f6f'
const NEUTRAL_COLOR = '#3fb4d8'
const WARNING_COLOR = '#c48b2d'

function toNumberOrNull(value) {
  return toFiniteNumber(value, null)
}

function formatFloat(value, digits = 2) {
  return formatDecimal(value, digits)
}

function marketDataStampLabel(marketData) {
  const source = (
    String(marketData?.source || '')
      .trim()
      .toUpperCase()
      .replaceAll('_', ' ')
      || 'MARKET SCANNER'
  )
  const candle = formatDateTimeLocal(marketData?.updated_at, '-')
  const age = formatAgeFromNow(marketData?.updated_at, '-')
  return `${source} | ${candle} | ${age}`
}

function formatCompactDateTime(value) {
  if (!value) return '-'
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return '-'
  return new Intl.DateTimeFormat('pt-BR', {
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(parsed)
}

function chartLegend(display = true) {
  return {
    display,
    labels: {
      color: CHART_TEXT,
      usePointStyle: true,
      pointStyle: 'circle',
      boxWidth: 10,
      boxHeight: 10,
      padding: 14,
      font: { size: 11, weight: '700' },
    },
  }
}

function makeScale(tickCallback) {
  return {
    ticks: {
      color: CHART_TEXT,
      font: { size: 11, family: CHART_MONO },
      callback: tickCallback,
    },
    grid: {
      color: CHART_GRID,
      drawBorder: false,
    },
    border: {
      display: false,
    },
  }
}

function buildCartesianOptions({ yTick, legend = false } = {}) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { intersect: false, mode: 'index' },
    scales: {
      x: {
        ticks: {
          color: CHART_TEXT,
          maxRotation: 0,
          autoSkipPadding: 14,
          font: { size: 11, family: CHART_MONO },
        },
        grid: {
          display: false,
          drawBorder: false,
        },
        border: {
          display: false,
        },
      },
      y: makeScale(yTick),
    },
    plugins: {
      legend: chartLegend(legend),
      tooltip: {
        backgroundColor: 'rgba(7, 18, 31, 0.96)',
        titleColor: '#f4fbff',
        bodyColor: '#dbe8f5',
        borderColor: 'rgba(43, 176, 201, 0.32)',
        borderWidth: 1,
        padding: 12,
        displayColors: true,
        titleFont: { family: CHART_MONO, size: 11, weight: '700' },
        bodyFont: { family: CHART_MONO, size: 11 },
      },
    },
  }
}

function buildCircularOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: chartLegend(true),
      tooltip: {
        backgroundColor: 'rgba(7, 18, 31, 0.96)',
        titleColor: '#f4fbff',
        bodyColor: '#dbe8f5',
        borderColor: 'rgba(43, 176, 201, 0.32)',
        borderWidth: 1,
        padding: 12,
        displayColors: true,
        titleFont: { family: CHART_MONO, size: 11, weight: '700' },
        bodyFont: { family: CHART_MONO, size: 11 },
      },
    },
  }
}

function formatTradeHeadline(trade) {
  if (!trade?.ticker) return 'Sem histórico'
  return `${trade.ticker} · ${formatCurrencyBRL(trade.realized_pnl_amount)}`
}

function pnlClassName(value) {
  return Number(value || 0) >= 0 ? 'scanner-positive' : 'scanner-negative'
}

function SwingTradePage({ readOnly = false }) {
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const [trades, setTrades] = useState({ active: [], history: [], summary: {}, tracked_tickers: [] })
  const browserTimeZone = currentBrowserTimeZone()

  const loadTrades = async (background = false) => {
    if (background) setRefreshing(true)
    else setLoading(true)
    setError('')
    try {
      const payload = await apiGet('/api/scanner/trades')
      setTrades(payload || { active: [], history: [], summary: {}, tracked_tickers: [] })
    } catch (err) {
      setError(err.message)
    } finally {
      if (background) setRefreshing(false)
      else setLoading(false)
    }
  }

  useEffect(() => {
    loadTrades(false)
    const timer = window.setInterval(() => {
      loadTrades(true)
    }, 60000)
    return () => window.clearInterval(timer)
  }, [])

  useEffect(() => {
    if (!message) return
    emitAppToast({ severity: 'success', message })
  }, [message])

  useEffect(() => {
    if (!error) return
    emitAppToast({ severity: 'error', message: error })
  }, [error])

  const operationSummary = useMemo(() => ({
    trackedCount: Number(trades?.summary?.tracked_count || 0),
    historyCount: Number(trades?.summary?.history_count || 0),
    successCount: Number(trades?.summary?.success || 0) + Number(trades?.summary?.closed_profit || 0),
    failureCount: Number(trades?.summary?.failure || 0) + Number(trades?.summary?.closed_loss || 0),
    openInvestedAmount: Number(trades?.summary?.open_invested_amount || 0),
    openPnlAmount: Number(trades?.summary?.open_pnl_amount || 0),
  }), [trades?.summary])

  const historyAnalytics = useMemo(() => {
    const history = Array.isArray(trades?.history) ? trades.history : []
    const orderedHistory = [...history].sort((left, right) => {
      const leftTime = new Date(left?.closed_at || left?.opened_at || 0).getTime()
      const rightTime = new Date(right?.closed_at || right?.opened_at || 0).getTime()
      return leftTime - rightTime
    })

    const totals = {
      grossProfit: 0,
      grossLoss: 0,
      netPnl: 0,
      winCount: 0,
      lossCount: 0,
      flatCount: 0,
    }
    let bestTrade = null
    let worstTrade = null
    const statusMap = new Map()
    let runningPnl = 0

    const cumulativeSeries = orderedHistory.map((trade) => {
      const pnlAmount = Number(trade?.realized_pnl_amount || 0)
      totals.netPnl += pnlAmount
      if (pnlAmount > 0) {
        totals.grossProfit += pnlAmount
        totals.winCount += 1
      } else if (pnlAmount < 0) {
        totals.grossLoss += Math.abs(pnlAmount)
        totals.lossCount += 1
      } else {
        totals.flatCount += 1
      }

      if (!bestTrade || pnlAmount > Number(bestTrade.realized_pnl_amount || 0)) bestTrade = trade
      if (!worstTrade || pnlAmount < Number(worstTrade.realized_pnl_amount || 0)) worstTrade = trade

      const statusKey = String(trade?.status_label || trade?.status || 'Sem status').trim() || 'Sem status'
      const currentStatus = statusMap.get(statusKey) || {
        label: statusKey,
        count: 0,
        tone: trade?.status_tone || 'neutral',
      }
      currentStatus.count += 1
      statusMap.set(statusKey, currentStatus)

      runningPnl += pnlAmount
      return {
        label: `${trade?.ticker || 'Trade'} · ${formatCompactDateTime(trade?.closed_at || trade?.opened_at)}`,
        value: Number(runningPnl.toFixed(2)),
      }
    })

    const closedCount = orderedHistory.length
    const averageWin = totals.winCount > 0 ? totals.grossProfit / totals.winCount : 0
    const averageLoss = totals.lossCount > 0 ? totals.grossLoss / totals.lossCount : 0
    const winRate = closedCount > 0 ? (totals.winCount / closedCount) * 100 : 0
    const profitFactor = totals.grossLoss > 0 ? totals.grossProfit / totals.grossLoss : null
    const statusSeries = [...statusMap.values()].sort((left, right) => right.count - left.count)

    return {
      closedCount,
      grossProfit: Number(totals.grossProfit.toFixed(2)),
      grossLoss: Number(totals.grossLoss.toFixed(2)),
      netPnl: Number(totals.netPnl.toFixed(2)),
      winCount: totals.winCount,
      lossCount: totals.lossCount,
      flatCount: totals.flatCount,
      winRate,
      averageWin,
      averageLoss,
      profitFactor,
      bestTrade,
      worstTrade,
      statusSeries,
      cumulativeSeries,
    }
  }, [trades?.history])

  const historyStatusChart = useMemo(() => ({
    labels: historyAnalytics.statusSeries.map((item) => item.label),
    datasets: [
      {
        label: 'Operações',
        data: historyAnalytics.statusSeries.map((item) => item.count),
        backgroundColor: historyAnalytics.statusSeries.map((item) => {
          if (item.tone === 'positive') return POSITIVE_COLOR
          if (item.tone === 'negative') return NEGATIVE_COLOR
          return item.count > 0 ? NEUTRAL_COLOR : WARNING_COLOR
        }),
        borderColor: 'rgba(7, 18, 31, 0.85)',
        borderWidth: 2,
      },
    ],
  }), [historyAnalytics.statusSeries])

  const historyAmountChart = useMemo(() => ({
    labels: ['Ganho bruto', 'Perda bruta', 'PnL líquido'],
    datasets: [
      {
        label: 'Valor',
        data: [historyAnalytics.grossProfit, historyAnalytics.grossLoss, historyAnalytics.netPnl],
        backgroundColor: [POSITIVE_COLOR, NEGATIVE_COLOR, historyAnalytics.netPnl >= 0 ? NEUTRAL_COLOR : WARNING_COLOR],
        borderRadius: 10,
        maxBarThickness: 64,
      },
    ],
  }), [historyAnalytics.grossLoss, historyAnalytics.grossProfit, historyAnalytics.netPnl])

  const historyCurveChart = useMemo(() => ({
    labels: historyAnalytics.cumulativeSeries.map((item) => item.label),
    datasets: [
      {
        label: 'PnL acumulado',
        data: historyAnalytics.cumulativeSeries.map((item) => item.value),
        borderColor: historyAnalytics.netPnl >= 0 ? POSITIVE_COLOR : NEGATIVE_COLOR,
        backgroundColor: historyAnalytics.netPnl >= 0 ? 'rgba(24, 184, 138, 0.18)' : 'rgba(216, 95, 111, 0.18)',
        fill: true,
        tension: 0.28,
        pointRadius: 3,
        pointHoverRadius: 5,
      },
    ],
  }), [historyAnalytics.cumulativeSeries, historyAnalytics.netPnl])

  const onCloseTrade = async (tradeId) => {
    if (readOnly) {
      setError('Perfil viewer possui acesso somente leitura.')
      return
    }
    setError('')
    setMessage('')
    try {
      await apiPost(`/api/scanner/trades/${tradeId}/close`, {})
      setMessage('Operação encerrada com sucesso.')
      await loadTrades(true)
    } catch (err) {
      setError(err.message)
    }
  }

  const onUpdateTradeInline = async (event, tradeId) => {
    event.preventDefault()
    if (readOnly) {
      setError('Perfil viewer possui acesso somente leitura.')
      return
    }
    const formData = new FormData(event.currentTarget)
    const quantity = toNumberOrNull(String(formData.get('quantity') || '').replace(',', '.'))
    const investedAmount = toNumberOrNull(String(formData.get('invested_amount') || '').replace(',', '.'))
    const objectivePrice = toNumberOrNull(String(formData.get('objective_price') || '').replace(',', '.'))
    const stopPrice = toNumberOrNull(String(formData.get('stop_price') || '').replace(',', '.'))
    const notes = String(formData.get('notes') || '').trim()
    if (!quantity || quantity <= 0 || !investedAmount || investedAmount <= 0 || !objectivePrice || !stopPrice) {
      setError('Valores inválidos para atualizar o trade.')
      return
    }
    setError('')
    setMessage('')
    try {
      await apiPatch(`/api/scanner/trades/${tradeId}`, {
        quantity,
        invested_amount: investedAmount,
        objective_price: objectivePrice,
        stop_price: stopPrice,
        notes,
      })
      setMessage('Operação atualizada.')
      await loadTrades(true)
    } catch (err) {
      setError(err.message)
    }
  }

  if (loading) return <p>Carregando swing trade...</p>

  return (
    <section>
      <div className="hero-line">
        <div>
          <h1>Swing Trade</h1>
          <p className="subtitle">Operações acompanhadas e histórico de tentativas do scanner.</p>
        </div>
        <div className="hero-actions">
          <Button variant="contained" onClick={() => loadTrades(true)} disabled={refreshing}>
            {refreshing ? 'Atualizando...' : 'Atualizar'}
          </Button>
        </div>
      </div>

      {!!message && <p className="notice-ok">{message}</p>}
      {!!error && <p className="notice-warn">{error}</p>}

      <Typography variant="caption" sx={{ mb: 1.5, display: 'block', opacity: 0.7 }}>
        Horários exibidos no seu fuso: {browserTimeZone}
      </Typography>

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Resumo</Typography>
        <div className="scanner-ops-stats">
          <article className="card">
            <h3>Em andamento</h3>
            <p>{operationSummary.trackedCount}</p>
          </article>
          <article className="card">
            <h3>Histórico</h3>
            <p>{operationSummary.historyCount}</p>
          </article>
          <article className="card">
            <h3>Sucesso</h3>
            <p>{operationSummary.successCount}</p>
          </article>
          <article className="card">
            <h3>Fracasso</h3>
            <p>{operationSummary.failureCount}</p>
          </article>
          <article className="card">
            <h3>Capital aberto</h3>
            <p>{formatCurrencyBRL(operationSummary.openInvestedAmount)}</p>
          </article>
          <article className="card">
            <h3>PnL aberto</h3>
            <p className={operationSummary.openPnlAmount >= 0 ? 'scanner-positive' : 'scanner-negative'}>
              {formatCurrencyBRL(operationSummary.openPnlAmount)}
            </p>
          </article>
        </div>
      </Paper>

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Operações abertas</Typography>
        {!Array.isArray(trades?.active) || trades.active.length === 0 ? (
          <p>Nenhuma operação aberta no momento.</p>
        ) : (
          <div className="scanner-open-trades-grid">
            {trades.active.map((trade) => (
              <article key={trade.id} className={`scanner-open-trade-card scanner-open-trade-card--${trade.status_tone || 'neutral'}`}>
                <header className="scanner-open-trade-head">
                  <div>
                    <strong>{trade.ticker}</strong>
                    <span className={`scanner-status-badge scanner-status-badge--${trade.status_tone || 'neutral'}`}>
                      {trade.status_label || trade.status || 'N/A'}
                    </span>
                  </div>
                  <Button size="small" color="error" variant="outlined" onClick={() => onCloseTrade(trade.id)} disabled={readOnly}>
                    Encerrar agora
                  </Button>
                </header>

                <div className="scanner-open-trade-metrics">
                  <div><small>Entrada</small><strong>{formatFloat(trade.entry_price, 4)}</strong></div>
                  <div><small>Quantidade</small><strong>{formatQuantity(trade.quantity, { maxDigits: 4 })}</strong></div>
                  <div><small>Investido</small><strong>{formatCurrencyBRL(trade.invested_amount)}</strong></div>
                  <div>
                    <small>Último preço</small>
                    <strong>{trade.last_price == null ? 'N/A' : formatFloat(trade.last_price, 4)}</strong>
                    <small className="scanner-market-data-stamp">{marketDataStampLabel(trade?.market_data)}</small>
                  </div>
                  <div><small>Valor atual</small><strong>{formatCurrencyBRL(trade.current_market_value)}</strong></div>
                  <div><small>PnL %</small><strong className={Number(trade.current_pnl_pct || 0) >= 0 ? 'scanner-positive' : 'scanner-negative'}>{formatPercent(trade.current_pnl_pct, 2, { signed: true })}</strong></div>
                  <div><small>PnL R$</small><strong className={Number(trade.current_pnl_amount || 0) >= 0 ? 'scanner-positive' : 'scanner-negative'}>{formatCurrencyBRL(trade.current_pnl_amount)}</strong></div>
                  <div><small>Score</small><strong>{formatFloat(trade.score)}</strong></div>
                </div>

                <div className="scanner-trade-plan">
                  <p><span>Entrada:</span> {formatFloat(trade?.entry_region?.low, 4)} - {formatFloat(trade?.entry_region?.high, 4)}</p>
                  <p><span>Objetivo:</span> {formatFloat(trade.objective_price, 4)}</p>
                  <p><span>Stop:</span> {formatFloat(trade.stop_price, 4)}</p>
                  <p><span>Risco/Retorno:</span> {trade.risk_reward_ratio == null ? 'N/A' : `1:${formatFloat(trade.risk_reward_ratio)}`}</p>
                </div>

                <div className="scanner-trade-meta">
                  <span>Aberta em {formatDateTimeLocal(trade.opened_at)}</span>
                  <span>Última checagem {formatDateTimeLocal(trade.last_checked_at)}</span>
                </div>

                {readOnly ? (
                  <p className="subtitle">Perfil viewer possui acesso somente leitura.</p>
                ) : (
                  <form className="scanner-trade-edit-form" onSubmit={(event) => onUpdateTradeInline(event, trade.id)}>
                    <div className="form-grid">
                      <label className="auth-field">
                        <span>Quantidade</span>
                        <input name="quantity" defaultValue={trade.quantity} type="number" min="0.000001" step="0.000001" />
                      </label>
                      <label className="auth-field">
                        <span>Investido</span>
                        <input name="invested_amount" defaultValue={trade.invested_amount} type="number" min="0.01" step="0.01" />
                      </label>
                      <label className="auth-field">
                        <span>Objetivo</span>
                        <input name="objective_price" defaultValue={trade.objective_price} type="number" min="0.0001" step="0.0001" />
                      </label>
                      <label className="auth-field">
                        <span>Stop</span>
                        <input name="stop_price" defaultValue={trade.stop_price} type="number" min="0.0001" step="0.0001" />
                      </label>
                      <label className="auth-field" style={{ gridColumn: '1 / -1' }}>
                        <span>Observações</span>
                        <input name="notes" defaultValue={trade.notes || ''} />
                      </label>
                    </div>
                    <div className="hero-actions" style={{ marginTop: 10 }}>
                      <Button size="small" type="submit" variant="contained">Salvar ajuste</Button>
                    </div>
                  </form>
                )}

                <div className="scanner-metrics">
                  {(Array.isArray(trade.metrics_triggered) ? trade.metrics_triggered : []).map((metric) => (
                    <span key={`${trade.id}-${metric}`} className="scanner-metric-chip">{metric}</span>
                  ))}
                </div>
              </article>
            ))}
          </div>
        )}
      </Paper>

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <div className="chart-panel-head">
          <div>
            <Typography variant="h6" sx={{ mb: 0.5 }}>Painel do histórico</Typography>
            <p className="subtitle">
              Visão rápida das operações encerradas para acompanhar acerto, perdas e evolução do resultado.
            </p>
          </div>
        </div>

        {!Array.isArray(trades?.history) || trades.history.length === 0 ? (
          <p>Ainda não existe histórico encerrado para montar os gráficos.</p>
        ) : (
          <>
            <div className="scanner-ops-stats" style={{ marginBottom: 16 }}>
              <article className="card">
                <h3>PnL realizado</h3>
                <p className={pnlClassName(historyAnalytics.netPnl)}>
                  {formatCurrencyBRL(historyAnalytics.netPnl)}
                </p>
              </article>
              <article className="card">
                <h3>Ganho bruto</h3>
                <p className="scanner-positive">{formatCurrencyBRL(historyAnalytics.grossProfit)}</p>
              </article>
              <article className="card">
                <h3>Perda bruta</h3>
                <p className="scanner-negative">{formatCurrencyBRL(historyAnalytics.grossLoss)}</p>
              </article>
              <article className="card">
                <h3>Taxa de acerto</h3>
                <p>{formatPercent(historyAnalytics.winRate, 1)}</p>
              </article>
              <article className="card">
                <h3>Melhor trade</h3>
                <p className={pnlClassName(historyAnalytics?.bestTrade?.realized_pnl_amount)}>{formatTradeHeadline(historyAnalytics.bestTrade)}</p>
              </article>
              <article className="card">
                <h3>Pior trade</h3>
                <p className={pnlClassName(historyAnalytics?.worstTrade?.realized_pnl_amount)}>{formatTradeHeadline(historyAnalytics.worstTrade)}</p>
              </article>
            </div>

            <div className="scanner-ops-stats" style={{ marginBottom: 16 }}>
              <article className="card">
                <h3>Operações com lucro</h3>
                <p>{historyAnalytics.winCount}</p>
              </article>
              <article className="card">
                <h3>Operações com perda</h3>
                <p>{historyAnalytics.lossCount}</p>
              </article>
              <article className="card">
                <h3>Operações zeradas</h3>
                <p>{historyAnalytics.flatCount}</p>
              </article>
              <article className="card">
                <h3>Lucro médio</h3>
                <p className="scanner-positive">{formatCurrencyBRL(historyAnalytics.averageWin)}</p>
              </article>
              <article className="card">
                <h3>Perda média</h3>
                <p className="scanner-negative">{formatCurrencyBRL(historyAnalytics.averageLoss)}</p>
              </article>
              <article className="card">
                <h3>Profit factor</h3>
                <p>{historyAnalytics.profitFactor == null ? 'Sem perdas' : `${formatDecimal(historyAnalytics.profitFactor, 2)}x`}</p>
              </article>
            </div>

            <div className="charts-grid charts-grid-analytics">
              <article className="card chart-card">
                <div className="chart-panel-head">
                  <div>
                    <h3>Status do histórico</h3>
                    <p className="subtitle">Quantidade de trades encerrados por tipo de resultado.</p>
                  </div>
                </div>
                <div className="chart-canvas-wrap">
                  <Doughnut
                    data={historyStatusChart}
                    options={buildCircularOptions()}
                  />
                </div>
              </article>

              <article className="card chart-card">
                <div className="chart-panel-head">
                  <div>
                    <h3>Ganhou vs perdeu</h3>
                    <p className="subtitle">Comparativo entre ganho bruto, perda bruta e resultado líquido.</p>
                  </div>
                </div>
                <div className="chart-canvas-wrap">
                  <Bar
                    data={historyAmountChart}
                    options={buildCartesianOptions({
                      yTick: (value) => formatCurrencyBRL(value),
                    })}
                  />
                </div>
              </article>

              <article className="card chart-card">
                <div className="chart-panel-head">
                  <div>
                    <h3>Curva de resultado</h3>
                    <p className="subtitle">Evolução acumulada do PnL realizado ao longo dos fechamentos.</p>
                  </div>
                </div>
                <div className="chart-canvas-wrap">
                  <Line
                    data={historyCurveChart}
                    options={buildCartesianOptions({
                      yTick: (value) => formatCurrencyBRL(value),
                    })}
                  />
                </div>
              </article>
            </div>
          </>
        )}
      </Paper>

      <Paper className="admin-panel" sx={{ p: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Histórico de tentativas</Typography>
        {!Array.isArray(trades?.history) || trades.history.length === 0 ? (
          <p>Ainda não existe histórico encerrado.</p>
        ) : (
          <div className="table-wrap">
            <table className="asset-table history-table">
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Status</th>
                  <th>Qtd</th>
                  <th>Investido</th>
                  <th>Entrada</th>
                  <th>Saída</th>
                  <th>PnL %</th>
                  <th>PnL R$</th>
                  <th>Fonte/candle</th>
                  <th>Motivo</th>
                  <th>Abertura</th>
                  <th>Fechamento</th>
                </tr>
              </thead>
              <tbody>
                {trades.history.map((trade) => (
                  <tr key={trade.id}>
                    <td>{trade.ticker}</td>
                    <td>
                      <span className={`scanner-status-badge scanner-status-badge--${trade.status_tone || 'neutral'}`}>
                        {trade.status_label || trade.status || 'N/A'}
                      </span>
                    </td>
                    <td>{formatQuantity(trade.quantity, { maxDigits: 4 })}</td>
                    <td>{formatCurrencyBRL(trade.invested_amount)}</td>
                    <td>{formatFloat(trade.entry_price, 4)}</td>
                    <td>{trade.exit_price == null ? 'N/A' : formatFloat(trade.exit_price, 4)}</td>
                    <td className={Number(trade.realized_pnl_pct || 0) >= 0 ? 'scanner-positive' : 'scanner-negative'}>
                      {formatPercent(trade.realized_pnl_pct, 2, { signed: true })}
                    </td>
                    <td className={Number(trade.realized_pnl_amount || 0) >= 0 ? 'scanner-positive' : 'scanner-negative'}>
                      {formatCurrencyBRL(trade.realized_pnl_amount)}
                    </td>
                    <td>{marketDataStampLabel(trade?.market_data)}</td>
                    <td>{trade.exit_reason || 'N/A'}</td>
                    <td>{formatDateTimeLocal(trade.opened_at)}</td>
                    <td>{formatDateTimeLocal(trade.closed_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Paper>
    </section>
  )
}

export default SwingTradePage
