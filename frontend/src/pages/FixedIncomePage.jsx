import { useEffect, useState } from 'react'
import { apiGet } from '../api'

function FixedIncomePage({ selectedPortfolioIds }) {
  const [payload, setPayload] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    ;(async () => {
      try {
        const data = await apiGet('/api/fixed-incomes', {
          portfolio_id: selectedPortfolioIds,
          sort_by: 'date_aporte',
          sort_dir: 'desc',
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
  }, [selectedPortfolioIds])

  if (loading) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>
  if (!payload) return <p>Sem dados.</p>

  const summary = payload.summary || {}
  const items = payload.items || []

  return (
    <section>
      <h1>Renda Fixa</h1>
      <div className="cards">
        <article className="card"><h3>Total aplicado</h3><p>R$ {Number(summary.applied_total || 0).toFixed(2)}</p></article>
        <article className="card"><h3>Valor atual bruto</h3><p>R$ {Number(summary.current_total || 0).toFixed(2)}</p></article>
        <article className="card"><h3>Rendimento bruto</h3><p>R$ {Number(summary.income_total || 0).toFixed(2)}</p></article>
        <article className="card"><h3>Total recebido</h3><p>R$ {Number(summary.total_received || 0).toFixed(2)}</p></article>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Distribuidor</th>
              <th>Emissor</th>
              <th>Investimento</th>
              <th>Tipo</th>
              <th>Aporte</th>
              <th>Data final</th>
              <th>Atual bruto</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.id}>
                <td>{item.distributor}</td>
                <td>{item.issuer}</td>
                <td>{item.investment_type}</td>
                <td>{item.rate_type}</td>
                <td>R$ {Number(item.aporte || 0).toFixed(2)}</td>
                <td>{item.maturity_date}</td>
                <td>R$ {Number(item.current_gross_value || 0).toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default FixedIncomePage
