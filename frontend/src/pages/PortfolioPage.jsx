import { useEffect, useState } from 'react'
import { apiGet } from '../api'

function PortfolioPage({ selectedPortfolioIds }) {
  const [snapshot, setSnapshot] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    ;(async () => {
      try {
        const data = await apiGet('/api/portfolio/snapshot', {
          portfolio_id: selectedPortfolioIds,
          sort_by: 'value',
          sort_dir: 'desc',
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
  }, [selectedPortfolioIds])

  if (loading) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>
  if (!snapshot) return <p>Sem dados.</p>

  return (
    <section>
      <h1>Renda Variavel</h1>
      <div className="cards">
        <article className="card"><h3>Patrimonio</h3><p>R$ {snapshot.total_value.toFixed(2)}</p></article>
        <article className="card"><h3>Investido</h3><p>R$ {snapshot.invested_value.toFixed(2)}</p></article>
        <article className="card"><h3>Aberto (R$)</h3><p className={snapshot.open_pnl_value >= 0 ? 'up' : 'down'}>R$ {snapshot.open_pnl_value.toFixed(2)}</p></article>
        <article className="card"><h3>Aberto (%)</h3><p className={snapshot.open_pnl_pct >= 0 ? 'up' : 'down'}>{snapshot.open_pnl_pct.toFixed(2)}%</p></article>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Nome</th>
              <th>Qtd</th>
              <th>Preco</th>
              <th>Investido</th>
              <th>Total</th>
              <th>Aberto</th>
              <th>Peso</th>
            </tr>
          </thead>
          <tbody>
            {snapshot.positions.map((item) => (
              <tr key={item.ticker}>
                <td>{item.ticker}</td>
                <td>{item.name}</td>
                <td>{Number(item.shares || 0).toFixed(4)}</td>
                <td>R$ {Number(item.price || 0).toFixed(2)}</td>
                <td>R$ {Number(item.invested_value || 0).toFixed(2)}</td>
                <td>R$ {Number(item.value || 0).toFixed(2)}</td>
                <td className={Number(item.open_pnl_value || 0) >= 0 ? 'up' : 'down'}>
                  R$ {Number(item.open_pnl_value || 0).toFixed(2)} ({Number(item.open_pnl_pct || 0).toFixed(2)}%)
                </td>
                <td>{Number(item.weight || 0).toFixed(2)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default PortfolioPage
