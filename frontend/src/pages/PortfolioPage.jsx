import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { apiGet } from '../api'

const CATEGORY_META = [
  { key: 'br_stocks', label: 'Acoes BR' },
  { key: 'us_stocks', label: 'Acoes US' },
  { key: 'crypto', label: 'Cripto' },
  { key: 'fiis', label: 'FIIs' },
]

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`

function PortfolioPage({ selectedPortfolioIds }) {
  const [snapshot, setSnapshot] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [sortBy, setSortBy] = useState('value')
  const [sortDir, setSortDir] = useState('desc')
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

  if (loading) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>
  if (!snapshot) return <p>Sem dados.</p>

  return (
    <section>
      <h1>Renda Variavel</h1>
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
                    <table>
                      <thead>
                        <tr>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('ticker')}>{sortLabel('Ticker', 'ticker')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('name')}>{sortLabel('Nome', 'name')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('shares')}>{sortLabel('Qtd', 'shares')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('price')}>{sortLabel('Preco', 'price')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('avg_price')}>{sortLabel('Preco medio', 'avg_price')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('invested_value')}>{sortLabel('Investido', 'invested_value')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('value')}>{sortLabel('Total', 'value')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('total_incomes')}>{sortLabel('Proventos', 'total_incomes')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('open_pnl_value')}>{sortLabel('Aberto', 'open_pnl_value')}</button></th>
                          <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('weight')}>{sortLabel('Peso', 'weight')}</button></th>
                        </tr>
                      </thead>
                      <tbody>
                        {items.map((item) => (
                          <tr key={`${meta.key}-${item.ticker}`}>
                            <td><Link to={`/ativo/${item.ticker}`}>{item.ticker}</Link></td>
                            <td>{item.name}</td>
                            <td>{Number(item.shares || 0).toFixed(4)}</td>
                            <td>{brl(item.price)}</td>
                            <td>{brl(item.avg_price)}</td>
                            <td>{brl(item.invested_value)}</td>
                            <td>{brl(item.value)}</td>
                            <td>{brl(item.total_incomes)}</td>
                            <td className={Number(item.open_pnl_value || 0) >= 0 ? 'up' : 'down'}>
                              {brl(item.open_pnl_value)} ({Number(item.open_pnl_pct || 0).toFixed(2)}%)
                            </td>
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
