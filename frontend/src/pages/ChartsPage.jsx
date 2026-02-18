import { useEffect, useState } from 'react'
import { apiGet } from '../api'

function ChartsPage({ selectedPortfolioIds }) {
  const [benchmark, setBenchmark] = useState(null)
  const [monthly, setMonthly] = useState([])
  const [range, setRange] = useState('12m')
  const [scope, setScope] = useState('all')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    ;(async () => {
      try {
        const [benchmarkData, monthlyData] = await Promise.all([
          apiGet('/api/charts/benchmark', {
            portfolio_id: selectedPortfolioIds,
            range,
            scope,
          }),
          apiGet('/api/charts/monthly-class-summary', {
            portfolio_id: selectedPortfolioIds,
          }),
        ])
        if (!active) return
        setBenchmark(benchmarkData)
        setMonthly(monthlyData)
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
  }, [selectedPortfolioIds, range, scope])

  if (loading) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>

  return (
    <section>
      <h1>Graficos</h1>
      <div className="inline-filters">
        <label>
          Periodo
          <select value={range} onChange={(e) => setRange(e.target.value)}>
            <option value="6m">6M</option>
            <option value="12m">12M</option>
            <option value="24m">24M</option>
            <option value="60m">5A</option>
          </select>
        </label>
        <label>
          Tipo
          <select value={scope} onChange={(e) => setScope(e.target.value)}>
            <option value="all">Todos</option>
            <option value="br">BR</option>
            <option value="us">US</option>
            <option value="fiis">FIIs</option>
            <option value="crypto">Cripto</option>
          </select>
        </label>
      </div>

      <h2>Benchmark</h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Mes</th>
              {(benchmark?.datasets || []).map((ds) => (
                <th key={ds.label}>{ds.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(benchmark?.labels || []).map((label, idx) => (
              <tr key={label + idx}>
                <td>{label}</td>
                {(benchmark?.datasets || []).map((ds) => {
                  const value = ds.values?.[idx]
                  const up = Number(value || 0) >= 0
                  return (
                    <td key={ds.label + idx} className={up ? 'up' : 'down'}>
                      {value == null ? '-' : `${Number(value).toFixed(2)}%`}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h2 style={{ marginTop: 24 }}>Resumo mensal por classe</h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Data</th>
              <th>Total investido</th>
              <th>Total proventos</th>
            </tr>
          </thead>
          <tbody>
            {monthly.map((row) => (
              <tr key={row.label}>
                <td>{row.label}</td>
                <td>R$ {Number(row.total_invested || 0).toFixed(2)}</td>
                <td>R$ {Number(row.total_incomes || 0).toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default ChartsPage
