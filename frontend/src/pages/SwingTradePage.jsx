import { useEffect, useMemo, useState } from 'react'
import { Button, Paper, Typography } from '@mui/material'
import { apiGet, apiPatch, apiPost } from '../api'
import { currentBrowserTimeZone, formatDateTimeLocal } from '../datetime'

function toNumberOrNull(value) {
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function formatFloat(value, digits = 2) {
  const num = Number(value)
  return Number.isFinite(num) ? num.toFixed(digits) : '-'
}

function formatCurrency(value) {
  const num = Number(value || 0)
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
  }).format(Number.isFinite(num) ? num : 0)
}

function SwingTradePage() {
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

  const operationSummary = useMemo(() => ({
    trackedCount: Number(trades?.summary?.tracked_count || 0),
    historyCount: Number(trades?.summary?.history_count || 0),
    successCount: Number(trades?.summary?.success || 0) + Number(trades?.summary?.closed_profit || 0),
    failureCount: Number(trades?.summary?.failure || 0) + Number(trades?.summary?.closed_loss || 0),
    openInvestedAmount: Number(trades?.summary?.open_invested_amount || 0),
    openPnlAmount: Number(trades?.summary?.open_pnl_amount || 0),
  }), [trades?.summary])

  const onCloseTrade = async (tradeId) => {
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
            <p>{formatCurrency(operationSummary.openInvestedAmount)}</p>
          </article>
          <article className="card">
            <h3>PnL aberto</h3>
            <p className={operationSummary.openPnlAmount >= 0 ? 'scanner-positive' : 'scanner-negative'}>
              {formatCurrency(operationSummary.openPnlAmount)}
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
                  <Button size="small" color="error" variant="outlined" onClick={() => onCloseTrade(trade.id)}>
                    Encerrar agora
                  </Button>
                </header>

                <div className="scanner-open-trade-metrics">
                  <div><small>Entrada</small><strong>{formatFloat(trade.entry_price, 4)}</strong></div>
                  <div><small>Quantidade</small><strong>{formatFloat(trade.quantity, 6)}</strong></div>
                  <div><small>Investido</small><strong>{formatCurrency(trade.invested_amount)}</strong></div>
                  <div><small>Último preço</small><strong>{trade.last_price == null ? 'N/A' : formatFloat(trade.last_price, 4)}</strong></div>
                  <div><small>Valor atual</small><strong>{formatCurrency(trade.current_market_value)}</strong></div>
                  <div><small>PnL %</small><strong className={Number(trade.current_pnl_pct || 0) >= 0 ? 'scanner-positive' : 'scanner-negative'}>{`${Number(trade.current_pnl_pct || 0) >= 0 ? '+' : ''}${formatFloat(Number(trade.current_pnl_pct || 0))}%`}</strong></div>
                  <div><small>PnL R$</small><strong className={Number(trade.current_pnl_amount || 0) >= 0 ? 'scanner-positive' : 'scanner-negative'}>{formatCurrency(trade.current_pnl_amount)}</strong></div>
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
                    <td>{formatFloat(trade.quantity, 6)}</td>
                    <td>{formatCurrency(trade.invested_amount)}</td>
                    <td>{formatFloat(trade.entry_price, 4)}</td>
                    <td>{trade.exit_price == null ? 'N/A' : formatFloat(trade.exit_price, 4)}</td>
                    <td className={Number(trade.realized_pnl_pct || 0) >= 0 ? 'scanner-positive' : 'scanner-negative'}>
                      {`${Number(trade.realized_pnl_pct || 0) >= 0 ? '+' : ''}${formatFloat(Number(trade.realized_pnl_pct || 0))}%`}
                    </td>
                    <td className={Number(trade.realized_pnl_amount || 0) >= 0 ? 'scanner-positive' : 'scanner-negative'}>
                      {formatCurrency(trade.realized_pnl_amount)}
                    </td>
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
