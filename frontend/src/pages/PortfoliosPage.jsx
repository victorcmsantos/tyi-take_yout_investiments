import { useState } from 'react'
import { apiDelete, apiPost } from '../api'

function PortfoliosPage({ portfolios, selectedPortfolioIds, refreshPortfolios }) {
  const [name, setName] = useState('')
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')

  const onCreate = async (event) => {
    event.preventDefault()
    setError('')
    setMessage('')
    try {
      await apiPost('/api/portfolios', { name })
      setName('')
      setMessage('Carteira criada com sucesso.')
      await refreshPortfolios()
    } catch (err) {
      setError(err.message)
    }
  }

  const onDelete = async (portfolioId, portfolioName) => {
    const confirmed = window.confirm(`Remover a carteira ${portfolioName}?`)
    if (!confirmed) return
    setError('')
    setMessage('')
    try {
      await apiDelete('/api/portfolios', { portfolio_id: portfolioId })
      setMessage(`Carteira '${portfolioName}' removida com sucesso.`)
      await refreshPortfolios()
    } catch (err) {
      setError(err.message)
    }
  }

  return (
    <section>
      <h1>Carteiras</h1>
      <p className="subtitle">Crie e remova carteiras para separar estrategias.</p>

      {!!error && <p className="notice-warn">{error}</p>}
      {!!message && <p className="notice-ok">{message}</p>}

      <article className="card form-card">
        <form onSubmit={onCreate} className="form-grid">
          <div>
            <label htmlFor="portfolio-name">Nome da carteira</label>
            <input
              id="portfolio-name"
              type="text"
              placeholder="Ex: Longo Prazo"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
            />
          </div>
          <div className="form-actions">
            <button type="submit" className="btn-primary">Criar carteira</button>
          </div>
        </form>
      </article>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Nome</th>
              <th>Status</th>
              <th>Remover</th>
            </tr>
          </thead>
          <tbody>
            {portfolios.map((item) => (
              <tr key={item.id}>
                <td>{item.id}</td>
                <td>{item.name}</td>
                <td>{selectedPortfolioIds.some((id) => Number(id) === Number(item.id)) ? 'Selecionada' : '-'}</td>
                <td>
                  <button type="button" className="btn-danger" onClick={() => onDelete(item.id, item.name)}>
                    Remover
                  </button>
                </td>
              </tr>
            ))}
            {portfolios.length === 0 && (
              <tr>
                <td colSpan={4}>Nenhuma carteira cadastrada.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default PortfoliosPage
