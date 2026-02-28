import { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import 'chart.js/auto'
import { Line } from 'react-chartjs-2'
import { apiGet, apiPost } from '../api'

const CHART_RANGES = [
  { key: '1d', label: '1 DIA' },
  { key: '7d', label: '7 DIAS' },
  { key: '30d', label: '30 DIAS' },
  { key: '6m', label: '6 MESES' },
  { key: '1y', label: '1 ANO' },
  { key: '5y', label: '5 ANOS' },
]

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`
const pct = (value) => `${Number(value || 0).toFixed(2)}%`

function dateBr(value) {
  if (!value) return ''
  const text = String(value)
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const [y, m, d] = text.split('-')
    return `${d}/${m}/${y}`
  }
  return text
}

function AssetPage({ selectedPortfolioIds }) {
  const { ticker } = useParams()
  const [rangeKey, setRangeKey] = useState('1y')
  const [payload, setPayload] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [syncMessage, setSyncMessage] = useState('')
  const [syncing, setSyncing] = useState(false)

  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    ;(async () => {
      try {
        const data = await apiGet(`/api/assets/${ticker}`, {
          portfolio_id: selectedPortfolioIds,
          range: rangeKey,
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
  }, [ticker, selectedPortfolioIds, rangeKey])

  const onSyncTicker = async () => {
    if (!ticker) return
    setSyncing(true)
    setSyncMessage('')
    try {
      await apiPost(`/api/sync/yahoo/${ticker}`)
      setSyncMessage('Dados atualizados com sucesso via Yahoo Finance.')
      const data = await apiGet(`/api/assets/${ticker}`, {
        portfolio_id: selectedPortfolioIds,
        range: rangeKey,
      })
      setPayload(data)
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
  const priceHistory = payload?.price_history || {}

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
          {syncing ? 'Atualizando...' : 'Atualizar dados do Yahoo'}
        </button>
      </div>
      <p className="subtitle">Setor: {asset.sector}</p>
      {!!syncMessage && <p className="notice-warn">{syncMessage}</p>}

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
              {pct(priceHistory.change_pct)} ({String(priceHistory.range_label || '').toUpperCase()})
            </span>
          )}
        </p>
        {(priceHistory.labels || []).length > 0 ? (
          <div className="chart-canvas-wrap">
            <Line data={chartData} options={{ responsive: true, maintainAspectRatio: false }} />
          </div>
        ) : (
          <p className="notice-warn">Nao foi possivel carregar historico para este periodo.</p>
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
        <p>Variacao no dia: <strong className={Number(asset.variation_day || 0) >= 0 ? 'up' : 'down'}>{pct(asset.variation_day)}</strong></p>
        <p>Valor de mercado: R$ {Number(asset.market_cap_bi || 0).toFixed(2)} bi</p>
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
