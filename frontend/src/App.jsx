import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, NavLink, Navigate, Route, Routes, useLocation, useNavigate } from 'react-router-dom'
import {
  AppBar,
  Box,
  Button,
  Checkbox,
  FormControlLabel,
  FormGroup,
  Paper,
  Stack,
  Toolbar,
  Typography,
} from '@mui/material'
import { apiGet } from './api'
import HomePage from './pages/HomePage'
import PortfolioPage from './pages/PortfolioPage'
import FixedIncomePage from './pages/FixedIncomePage'
import ChartsPage from './pages/ChartsPage'
import AssetPage from './pages/AssetPage'
import NewTransactionPage from './pages/NewTransactionPage'
import NewIncomePage from './pages/NewIncomePage'
import PortfoliosPage from './pages/PortfoliosPage'

function App({ themeMode, onToggleTheme }) {
  const navigate = useNavigate()
  const location = useLocation()
  const [portfolios, setPortfolios] = useState([])
  const [selectedPortfolioIds, setSelectedPortfolioIds] = useState([])
  const [loadingPortfolios, setLoadingPortfolios] = useState(true)
  const [error, setError] = useState('')
  const [assetSearch, setAssetSearch] = useState('')
  const [sidebarOpen, setSidebarOpen] = useState(false)

  const refreshPortfolios = useCallback(async () => {
    const data = await apiGet('/api/portfolios')
    setPortfolios(data)
    setSelectedPortfolioIds((current) => {
      const valid = current.filter((id) => data.some((p) => Number(p.id) === Number(id)))
      if (valid.length > 0) return valid
      if (data.length > 0) return data.map((item) => item.id)
      return []
    })
    return data
  }, [])

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
          setSelectedPortfolioIds(data.map((item) => item.id))
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

  useEffect(() => {
    setSidebarOpen(false)
  }, [location.pathname])

  useEffect(() => {
    if (!sidebarOpen) return undefined
    const onKeyDown = (event) => {
      if (event.key === 'Escape') setSidebarOpen(false)
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [sidebarOpen])

  const activePortfolioNames = useMemo(() => {
    const names = selectedPortfolioIds
      .map((selectedId) => portfolios.find((item) => Number(item.id) === Number(selectedId))?.name)
      .filter(Boolean)
    if (names.length === 0) return 'Sem carteira'
    if (names.length <= 3) return names.join(', ')
    return `${names.slice(0, 3).join(', ')} +${names.length - 3}`
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

  const onSearchAsset = (event) => {
    event.preventDefault()
    const ticker = String(assetSearch || '').trim().toUpperCase()
    if (!ticker) return
    navigate(`/ativo/${ticker}`)
    setAssetSearch('')
  }

  const breadcrumbLabelMap = {
    '/': 'Dashboard',
    '/carteira': 'Renda Variavel',
    '/renda-fixa': 'Renda Fixa',
    '/graficos': 'Graficos',
    '/nova': 'Nova transacao',
    '/novo': 'Novo provento',
    '/carteiras': 'Carteiras',
  }

  const breadcrumbs = useMemo(() => {
    const path = location.pathname || '/'
    if (path.startsWith('/ativo/')) {
      const ticker = decodeURIComponent(path.split('/').pop() || '').toUpperCase()
      return [
        { to: '/carteira', label: 'Renda Variavel' },
        { to: path, label: ticker || 'Ativo' },
      ]
    }
    const label = breadcrumbLabelMap[path]
    if (!label) return [{ to: '/', label: 'Dashboard' }]
    if (path === '/') return [{ to: '/', label }]
    return [
      { to: '/', label: 'Dashboard' },
      { to: path, label },
    ]
  }, [location.pathname])

  const menuSections = [
    {
      title: 'Visao Geral',
      items: [
        { to: '/', label: 'Dashboard' },
        { to: '/graficos', label: 'Graficos' },
      ],
    },
    {
      title: 'Carteira',
      items: [
        { to: '/carteira', label: 'Renda Variavel' },
        { to: '/renda-fixa', label: 'Renda Fixa' },
      ],
    },
    {
      title: 'Lancamentos',
      items: [
        { to: '/nova', label: 'Nova transacao' },
        { to: '/novo', label: 'Novo provento' },
      ],
    },
    {
      title: 'Configuracao',
      items: [
        { to: '/carteiras', label: 'Carteiras' },
      ],
    },
  ]

  return (
    <Box className="app-v2-shell" sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
      <AppBar position="static" color="secondary" elevation={0} className="app-v2-header-bar">
        <Toolbar className="app-v2-toolbar" sx={{ position: 'relative', display: 'flex', flexDirection: 'column', alignItems: 'stretch', gap: 1, py: 1.25 }}>
          <Box className="app-v2-header-row">
            <Box className="app-v2-header-left">
              <button
                type="button"
                aria-label={sidebarOpen ? 'Fechar menu' : 'Abrir menu'}
                className={`app-v2-mobile-toggle ${sidebarOpen ? 'open' : ''}`}
                onClick={() => setSidebarOpen((current) => !current)}
              >
                <span className="icon icon-menu">☰</span>
                <span className="icon icon-close">✕</span>
              </button>
              <Link to="/" className="app-v2-brand-link" aria-label="CasalInvest">
                <img
                  src="/logo-casalinvest.svg"
                  alt="CasalInvest"
                  className="app-v2-brand-logo"
                />
              </Link>
            </Box>
            <Box className="app-v2-header-right">
              <Typography variant="body2" className="app-v2-header-portfolios" title={activePortfolioNames}>
                Carteiras selecionadas: {activePortfolioNames}
              </Typography>
              <Button color="inherit" variant="outlined" onClick={onToggleTheme} sx={{ borderColor: 'rgba(255,255,255,0.35)' }}>
                {themeMode === 'dark' ? 'Modo claro' : 'Modo escuro'}
              </Button>
            </Box>
          </Box>
          <Box component="form" onSubmit={onSearchAsset} className="app-v2-header-search">
            <input
              className="app-v2-search"
              value={assetSearch}
              onChange={(event) => setAssetSearch(event.target.value)}
              placeholder="Buscar ticker (ex: ITUB4)"
            />
            <Button type="submit" variant="contained" color="primary">Buscar</Button>
          </Box>
        </Toolbar>
      </AppBar>

      <Box className="app-v2-layout" sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', lg: '280px 1fr' }, gap: 2, p: 2 }}>
        <div
          className={`app-v2-sidebar-backdrop${sidebarOpen ? ' open' : ''}`}
          onClick={() => setSidebarOpen(false)}
          aria-hidden="true"
        />
        <Paper className={`app-v2-sidebar ${sidebarOpen ? 'open' : ''}`} sx={{ p: 2, alignSelf: 'start' }}>
          <div className="app-v2-sidebar-head">
            <Typography variant="h6">Menu</Typography>
            <button
              type="button"
              aria-label="Fechar menu"
              className="app-v2-sidebar-close"
              onClick={() => setSidebarOpen(false)}
            >
              ✕
            </button>
          </div>
          <div className="app-v2-menu">
            {menuSections.map((section) => (
              <div key={section.title} className="app-v2-menu-section">
                <span className="app-v2-menu-title">{section.title}</span>
                {section.items.map((item) => (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    className={({ isActive }) => `app-v2-menu-link${isActive ? ' active' : ''}`}
                    onClick={() => setSidebarOpen(false)}
                  >
                    {item.label}
                  </NavLink>
                ))}
              </div>
            ))}
          </div>

          <Typography variant="h6" sx={{ mt: 2, mb: 1 }}>Carteiras</Typography>
          {loadingPortfolios && <Typography variant="body2">Carregando...</Typography>}
          {!!error && <Typography color="error" variant="body2">{error}</Typography>}
          <FormGroup>
            {portfolios.map((portfolio) => (
              <FormControlLabel
                key={portfolio.id}
                control={(
                  <Checkbox
                    checked={selectedPortfolioIds.some((id) => Number(id) === Number(portfolio.id))}
                    onChange={() => onTogglePortfolio(portfolio.id)}
                    size="small"
                  />
                )}
                label={portfolio.name}
              />
            ))}
          </FormGroup>
          <Button component={Link} to="/carteiras" variant="outlined" fullWidth sx={{ mt: 1 }}>
            Gerenciar
          </Button>
        </Paper>

        <Box className="app-v2-content">
          <div className="app-v2-breadcrumbs">
            {breadcrumbs.map((item, idx) => (
              <span key={`${item.to}-${idx}`}>
                {idx > 0 ? <span className="sep">/</span> : null}
                <Link to={item.to}>{item.label}</Link>
              </span>
            ))}
          </div>
          <Routes>
            <Route path="/" element={<HomePage selectedPortfolioIds={selectedPortfolioIds} />} />
            <Route path="/carteira" element={<PortfolioPage selectedPortfolioIds={selectedPortfolioIds} />} />
            <Route path="/renda-fixa" element={<FixedIncomePage selectedPortfolioIds={selectedPortfolioIds} />} />
            <Route path="/graficos" element={<ChartsPage selectedPortfolioIds={selectedPortfolioIds} />} />
            <Route path="/ativo/:ticker" element={<AssetPage selectedPortfolioIds={selectedPortfolioIds} />} />
            <Route
              path="/nova"
              element={<NewTransactionPage selectedPortfolioIds={selectedPortfolioIds} portfolios={portfolios} />}
            />
            <Route
              path="/novo"
              element={<NewIncomePage selectedPortfolioIds={selectedPortfolioIds} portfolios={portfolios} />}
            />
            <Route
              path="/carteiras"
              element={(
                <PortfoliosPage
                  portfolios={portfolios}
                  selectedPortfolioIds={selectedPortfolioIds}
                  refreshPortfolios={refreshPortfolios}
                />
              )}
            />
            <Route path="/transacoes/nova" element={<Navigate to="/nova" replace />} />
            <Route path="/proventos/novo" element={<Navigate to="/novo" replace />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Box>
      </Box>
    </Box>
  )
}

export default App
