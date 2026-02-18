import { useEffect, useState } from 'react'
import { apiGet } from '../api'

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`

function pct(value) {
  return `${Number(value || 0).toFixed(2)}%`
}

function formatRateType(item) {
  const rateType = String(item.rate_type || '').toUpperCase()
  const fixed = Number(item.rate_fixed || 0)
  const ipca = Number(item.rate_ipca || 0)
  const cdi = Number(item.rate_cdi || 0)
  const annual = Number(item.annual_rate || 0)

  if (rateType === 'FIXO') return `${rateType} (${pct(fixed || annual)})`
  if (rateType === 'IPCA') return `${rateType} (${pct(ipca || annual)})`
  if (rateType === 'CDI') return `${rateType} (${pct(cdi || annual)})`
  if (rateType === 'FIXO+IPCA') return `${rateType} (${pct(fixed)} + ${pct(ipca)})`
  if (rateType === 'FIXO+CDI') return `${rateType} (${pct(fixed)} + ${pct(cdi)})`
  return `${rateType || 'N/A'} (${pct(annual)})`
}

function groupSummary(items) {
  return {
    count: items.length,
    applied: items.reduce((acc, item) => acc + Number(item.active_applied_value || 0), 0),
    current: items.reduce((acc, item) => acc + Number(item.current_gross_value || 0), 0),
    income: items.reduce((acc, item) => acc + Number(item.current_income || 0), 0),
    totalReceived: items.reduce((acc, item) => acc + Number(item.total_received || 0), 0),
  }
}

function FixedIncomePage({ selectedPortfolioIds }) {
  const [payload, setPayload] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [sortBy, setSortBy] = useState('date_aporte')
  const [sortDir, setSortDir] = useState('desc')
  const [openGroups, setOpenGroups] = useState({ prefixado: true, posfixado: false })

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
        const data = await apiGet('/api/fixed-incomes', {
          portfolio_id: selectedPortfolioIds,
          sort_by: sortBy,
          sort_dir: sortDir,
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
  }, [selectedPortfolioIds, sortBy, sortDir])

  if (loading) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>
  if (!payload) return <p>Sem dados.</p>

  const summary = payload.summary || {}
  const items = payload.items || []
  const prefixadoItems = items.filter((item) => String(item.rate_type || '').toUpperCase() === 'FIXO')
  const posfixadoItems = items.filter((item) => String(item.rate_type || '').toUpperCase() !== 'FIXO')
  const groups = [
    { key: 'prefixado', label: 'Juros Prefixado', items: prefixadoItems },
    { key: 'posfixado', label: 'Juros Pos-fixado', items: posfixadoItems },
  ]

  return (
    <section>
      <h1>Renda Fixa</h1>
      <div className="cards">
        <article className="card"><h3>Total aplicado</h3><p>{brl(summary.applied_total)}</p></article>
        <article className="card"><h3>Valor atual bruto</h3><p>{brl(summary.current_total)}</p></article>
        <article className="card"><h3>Rendimento bruto</h3><p>{brl(summary.income_total)}</p></article>
        <article className="card"><h3>Total recebido</h3><p>{brl(summary.total_received)}</p></article>
      </div>

      <div className="accordion-wrap">
        {groups.map((group) => {
          const sum = groupSummary(group.items)
          const isOpen = !!openGroups[group.key]
          const fixedTotal = Number(summary.current_total || 0)
          const groupWeight = fixedTotal > 0 ? (sum.current / fixedTotal) * 100 : 0
          return (
            <section key={group.key} className="asset-group">
              <button type="button" className="asset-group-summary-btn" onClick={() => toggleGroup(group.key)}>
                <div className="asset-group-summary">
                <div>
                  <strong>{group.label}</strong>
                  <small>{sum.count} registro(s)</small>
                </div>
                <div className="asset-group-metrics">
                  <div className="metric-item">
                    <span className="metric-label">Aplicado</span>
                    <strong>{brl(sum.applied)}</strong>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Valor atual bruto</span>
                    <strong>{brl(sum.current)}</strong>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Rendimento bruto</span>
                    <strong className={sum.income >= 0 ? 'up' : 'down'}>{brl(sum.income)}</strong>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">% na carteira</span>
                    <strong>{Number(groupWeight || 0).toFixed(2)}%</strong>
                  </div>
                </div>
                <span className={`asset-group-chevron ${isOpen ? 'open' : ''}`}>⌄</span>
                </div>
              </button>

              {isOpen && (
                <>
              <div className="cards">
                <article className="card"><h3>Total aplicado</h3><p>{brl(sum.applied)}</p></article>
                <article className="card"><h3>Valor atual bruto</h3><p>{brl(sum.current)}</p></article>
                <article className="card"><h3>Rendimento bruto</h3><p className={sum.income >= 0 ? 'up' : 'down'}>{brl(sum.income)}</p></article>
                <article className="card"><h3>Total recebido</h3><p>{brl(sum.totalReceived)}</p></article>
              </div>

              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('distributor')}>{sortLabel('Distribuidor', 'distributor')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('issuer')}>{sortLabel('Emissor', 'issuer')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('investment_type')}>{sortLabel('Investimento', 'investment_type')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('rate_type')}>{sortLabel('Tipo', 'rate_type')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('date_aporte')}>{sortLabel('Data aporte', 'date_aporte')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('active_applied_value')}>{sortLabel('Aporte', 'active_applied_value')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('maturity_date')}>{sortLabel('Data final', 'maturity_date')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('current_gross_value')}>{sortLabel('Atual bruto', 'current_gross_value')}</button></th>
                    </tr>
                  </thead>
                  <tbody>
                    {group.items.map((item) => (
                      <tr key={`${group.key}-${item.id}`}>
                        <td>{item.distributor}</td>
                        <td>{item.issuer}</td>
                        <td>{item.investment_type}</td>
                        <td>{formatRateType(item)}</td>
                        <td>{item.date_aporte}</td>
                        <td>{brl(item.aporte)}</td>
                        <td>{item.maturity_date}</td>
                        <td>{brl(item.current_gross_value)}</td>
                      </tr>
                    ))}
                    {group.items.length === 0 && (
                      <tr>
                        <td colSpan={8}>Sem registros nesse grupo.</td>
                      </tr>
                    )}
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

export default FixedIncomePage
