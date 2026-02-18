import { useEffect, useMemo, useState } from 'react'
import { Link, Navigate, Route, Routes } from 'react-router-dom'
import { apiGet } from './api'
import HomePage from './pages/HomePage'
import PortfolioPage from './pages/PortfolioPage'
import FixedIncomePage from './pages/FixedIncomePage'
import ChartsPage from './pages/ChartsPage'

function App() {
  const [portfolios, setPortfolios] = useState([])
  const [selectedPortfolioIds, setSelectedPortfolioIds] = useState([])
  const [loadingPortfolios, setLoadingPortfolios] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let active = true
    ;(async () => {
      try {
        const data = await apiGet('/api/portfolios')
        if (!active) return
        setPortfolios(data)
        const saved = localStorage.getItem('selectedPortfolioIds')
        const parsed = saved ? JSON.parse(saved) : []
        const validIds = parsed.filter((id) => data.some((p) => Number(p.id) === Number(id)))
        if (validIds.length > 0) {
          setSelectedPortfolioIds(validIds)
        } else if (data.length > 0) {
          setSelectedPortfolioIds([data[0].id])
        }
      } catch (err) {
        if (!active) return
        setError(err.message)
      } finally {
        if (active) setLoadingPortfolios(false)
      }
    })()

    return () => {
      active = false
    }
  }, [])

  useEffect(() => {
    localStorage.setItem('selectedPortfolioIds', JSON.stringify(selectedPortfolioIds))
  }, [selectedPortfolioIds])

  const activePortfolioName = useMemo(() => {
    const firstId = selectedPortfolioIds[0]
    return portfolios.find((item) => Number(item.id) === Number(firstId))?.name || 'Sem carteira'
  }, [portfolios, selectedPortfolioIds])

  const onTogglePortfolio = (portfolioId) => {
    setSelectedPortfolioIds((current) => {
      const exists = current.some((id) => Number(id) === Number(portfolioId))
      if (exists) {
        const next = current.filter((id) => Number(id) !== Number(portfolioId))
        return next.length > 0 ? next : current
      }
      return [...current, portfolioId]
    })
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="topbar-row">
          <Link to="/" className="brand">Invest Portal</Link>
          <nav className="nav">
            <Link to="/">Acoes</Link>
            <Link to="/carteira">Renda Variavel</Link>
            <Link to="/renda-fixa">Renda Fixa</Link>
            <Link to="/graficos">Graficos</Link>
          </nav>
        </div>
        <p className="active-tag">Carteira ativa: {activePortfolioName}</p>
      </header>

      <div className="layout">
        <aside className="sidebar">
          <h3>Carteiras</h3>
          {loadingPortfolios && <p>Carregando...</p>}
          {!!error && <p className="error">{error}</p>}
          {portfolios.map((portfolio) => (
            <label key={portfolio.id} className="check-row">
              <input
                type="checkbox"
                checked={selectedPortfolioIds.some((id) => Number(id) === Number(portfolio.id))}
                onChange={() => onTogglePortfolio(portfolio.id)}
              />
              {portfolio.name}
            </label>
          ))}
        </aside>

        <main className="content">
          <Routes>
            <Route path="/" element={<HomePage selectedPortfolioIds={selectedPortfolioIds} />} />
            <Route path="/carteira" element={<PortfolioPage selectedPortfolioIds={selectedPortfolioIds} />} />
            <Route path="/renda-fixa" element={<FixedIncomePage selectedPortfolioIds={selectedPortfolioIds} />} />
            <Route path="/graficos" element={<ChartsPage selectedPortfolioIds={selectedPortfolioIds} />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </main>
      </div>
    </div>
  )
}

export default App
