import { Suspense, lazy, useCallback, useEffect, useMemo, useState } from 'react'
import { Link, NavLink, Navigate, Route, Routes, useLocation, useNavigate } from 'react-router-dom'
import {
  Alert,
  AppBar,
  Box,
  Button,
  Checkbox,
  FormControlLabel,
  FormGroup,
  Paper,
  Snackbar,
  Toolbar,
  Typography,
} from '@mui/material'
import { apiGet, apiGetCached, apiPost, clearApiCache } from './api'
import { APP_TOAST_EVENT } from './toast'
import StatePanel from './components/StatePanel'

const HomePage = lazy(() => import('./pages/HomePage'))
const PortfolioPage = lazy(() => import('./pages/PortfolioPage'))
const FixedIncomePage = lazy(() => import('./pages/FixedIncomePage'))
const ChartsPage = lazy(() => import('./pages/ChartsPage'))
const AssetPage = lazy(() => import('./pages/AssetPage'))
const NewTransactionPage = lazy(() => import('./pages/NewTransactionPage'))
const NewIncomePage = lazy(() => import('./pages/NewIncomePage'))
const PortfoliosPage = lazy(() => import('./pages/PortfoliosPage'))
const LoginPage = lazy(() => import('./pages/LoginPage'))
const AdminPage = lazy(() => import('./pages/AdminPage'))
const AllocationPage = lazy(() => import('./pages/AllocationPage'))
const MetricFormulasPage = lazy(() => import('./pages/MetricFormulasPage'))
const ScannerPage = lazy(() => import('./pages/ScannerPage'))
const ScannerMetricsLabPage = lazy(() => import('./pages/ScannerMetricsLabPage'))
const SwingTradePage = lazy(() => import('./pages/SwingTradePage'))
const SyncHealthPage = lazy(() => import('./pages/SyncHealthPage'))

function RouteFallback({ title = 'Carregando pagina...' }) {
  return (
    <StatePanel
      busy
      eyebrow="Navegacao"
      title={title}
      description="Preparando os dados e componentes desta tela."
      className="route-fallback"
    />
  )
}

function App({ themeMode, onToggleTheme }) {
  const navigate = useNavigate()
  const location = useLocation()
  const [currentUser, setCurrentUser] = useState(null)
  const [authLoading, setAuthLoading] = useState(true)
  const [portfolios, setPortfolios] = useState([])
  const [selectedPortfolioIds, setSelectedPortfolioIds] = useState([])
  const [loadingPortfolios, setLoadingPortfolios] = useState(true)
  const [error, setError] = useState('')
  const [assetSearch, setAssetSearch] = useState('')
  const [assetSuggestions, setAssetSuggestions] = useState([])
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [toastState, setToastState] = useState({
    open: false,
    message: '',
    severity: 'info',
    durationMs: 4200,
    stamp: '',
  })
  const currentUserRole = String(currentUser?.role || '').toLowerCase()
  const isAdminUser = Boolean(currentUser?.is_admin) || currentUserRole === 'admin'
  const isViewerUser = currentUserRole === 'viewer'

  const shouldShowAssetSuggestions = String(assetSearch || '').trim().length >= 1

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

  const refreshAuth = useCallback(async () => {
    const payload = await apiGet('/api/auth/me')
    const user = payload?.user || null
    setCurrentUser(user)
    return user
  }, [])

  useEffect(() => {
    let active = true
    ;(async () => {
      try {
        const user = await refreshAuth()
        if (!active) return
        if (!user) {
          setLoadingPortfolios(false)
          return
        }
        const data = await refreshPortfolios()
        if (!active) return
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
        if (active) {
          setAuthLoading(false)
          setLoadingPortfolios(false)
        }
      }
    })()

    return () => {
      active = false
    }
  }, [refreshAuth, refreshPortfolios])

  useEffect(() => {
    localStorage.setItem('selectedPortfolioIds', JSON.stringify(selectedPortfolioIds))
  }, [selectedPortfolioIds])

  useEffect(() => {
    setSidebarOpen(false)
  }, [location.pathname])

  useEffect(() => {
    window.scrollTo({ left: 0, top: window.scrollY, behavior: 'auto' })
  }, [location.pathname])

  useEffect(() => {
    if (!currentUser) {
      setAssetSuggestions([])
      return undefined
    }
    let active = true
    ;(async () => {
      try {
        const assets = await apiGetCached('/api/assets', {}, { ttlMs: 15000, staleWhileRevalidate: true })
        if (!active) return
        setAssetSuggestions(Array.isArray(assets) ? assets : [])
      } catch (err) {
        if (!active) return
        setAssetSuggestions([])
      }
    })()
    return () => {
      active = false
    }
  }, [currentUser])

  useEffect(() => {
    if (!sidebarOpen) return undefined
    const onKeyDown = (event) => {
      if (event.key === 'Escape') setSidebarOpen(false)
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [sidebarOpen])

  useEffect(() => {
    const handleToast = (event) => {
      const detail = event?.detail || {}
      const message = String(detail.message || '').trim()
      if (!message) return
      setToastState({
        open: true,
        message,
        severity: String(detail.severity || 'info'),
        durationMs: Number(detail.durationMs) || 4200,
        stamp: new Date().toLocaleTimeString('pt-BR', { hour12: false }),
      })
    }
    window.addEventListener(APP_TOAST_EVENT, handleToast)
    return () => window.removeEventListener(APP_TOAST_EVENT, handleToast)
  }, [])

  const onLoggedIn = async () => {
    setAuthLoading(false)
    setLoadingPortfolios(true)
    setError('')
    try {
      clearApiCache()
      const authenticatedUser = await refreshAuth()
      if (!authenticatedUser) {
        throw new Error('Nao foi possivel confirmar a sessao apos o login.')
      }
      const data = await refreshPortfolios()
      if (data.length > 0) {
        setSelectedPortfolioIds(data.map((item) => item.id))
      }
      navigate('/')
    } catch (err) {
      setCurrentUser(null)
      setError(err.message)
    } finally {
      setLoadingPortfolios(false)
    }
  }

  const onLogout = async () => {
    try {
      await apiPost('/api/auth/logout')
    } catch (err) {
      // ignora erro e limpa estado local mesmo assim
    }
    clearApiCache()
    setCurrentUser(null)
    setPortfolios([])
    setSelectedPortfolioIds([])
    setError('')
    setAssetSuggestions([])
    navigate('/login')
  }

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
    '/alocador': 'Alocador',
    '/scanner': 'Scanner',
    '/admin/sync-health': 'Saude de sync',
    '/swing-trade': 'Swing Trade',
    '/admin': 'Admin',
    '/admin/metricas': 'Metricas',
    '/admin/metrics-lab': 'Metrics Lab',
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

  const menuSections = useMemo(() => {
    if (isAdminUser) {
      return [
        {
          title: 'Administracao',
          items: [
            { to: '/admin', label: 'Usuarios' },
            { to: '/admin/sync-health', label: 'Saude de sync' },
            { to: '/admin/metricas', label: 'Metricas' },
            { to: '/admin/metrics-lab', label: 'Metrics Lab' },
          ],
        },
      ]
    }

    const sections = [
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
        title: 'Ferramentas',
        items: [
          { to: '/alocador', label: 'Alocador' },
          { to: '/scanner', label: 'Scanner' },
          { to: '/swing-trade', label: 'Swing Trade' },
        ],
      },
    ]
    if (!isViewerUser) {
      sections.splice(2, 0, {
        title: 'Lancamentos',
        items: [
          { to: '/nova', label: 'Nova transacao' },
          { to: '/novo', label: 'Novo provento' },
        ],
      })
      sections.push({
        title: 'Configuracao',
        items: [
          { to: '/carteiras', label: 'Carteiras' },
        ],
      })
    }
    return sections
  }, [isAdminUser, isViewerUser])

  if (authLoading) {
    return <main className="auth-shell"><p>Carregando autenticacao...</p></main>
  }

  if (!currentUser) {
    return (
      <Suspense fallback={<main className="auth-shell"><RouteFallback title="Abrindo login..." /></main>}>
        <Routes>
          <Route path="/login" element={<LoginPage onLoggedIn={onLoggedIn} />} />
          <Route path="*" element={<Navigate to="/login" replace />} />
        </Routes>
      </Suspense>
    )
  }

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
                {currentUser.username}{isAdminUser ? ' · Admin' : isViewerUser ? ' · Viewer' : ` · ${activePortfolioNames}`}
              </Typography>
              <Button
                color="inherit"
                variant="outlined"
                onClick={onToggleTheme}
                className="app-v2-header-btn"
                sx={{ borderColor: 'rgba(255,255,255,0.35)' }}
              >
                {themeMode === 'dark' ? 'Modo claro' : 'Modo escuro'}
              </Button>
              <Button
                color="inherit"
                variant="outlined"
                onClick={onLogout}
                className="app-v2-header-btn"
                sx={{ borderColor: 'rgba(255,255,255,0.35)' }}
              >
                Sair
              </Button>
            </Box>
          </Box>
          <Box component="form" onSubmit={onSearchAsset} className="app-v2-header-search">
            <input
              className="app-v2-search"
              value={assetSearch}
              onChange={(event) => setAssetSearch(event.target.value)}
              placeholder="Buscar ticker (ex: ITUB4)"
              list={shouldShowAssetSuggestions ? 'asset-ticker-suggestions' : undefined}
            />
            <datalist id="asset-ticker-suggestions">
              {assetSuggestions
                .map((asset) => ({
                  ticker: String(asset?.ticker || '').toUpperCase(),
                  name: String(asset?.name || '').trim(),
                }))
                .filter((item) => item.ticker)
                .filter((item, idx, arr) => arr.findIndex((other) => other.ticker === item.ticker) === idx)
                .map((item) => (
                  <option key={item.ticker} value={item.ticker}>{item.name}</option>
                ))}
            </datalist>
            <Button type="submit" variant="contained" color="primary" className="app-v2-search-btn">Buscar</Button>
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

          <div className="app-v2-sidebar-summary">
            <small>Contexto ativo</small>
            <strong>{activePortfolioNames}</strong>
            <span>{currentUser.username}{isAdminUser ? ' · Admin' : isViewerUser ? ' · Viewer' : ' · Investidor'}</span>
          </div>

          <div className="app-v2-portfolio-panel">
            <Typography variant="h6" sx={{ mt: 0, mb: 1 }}>Carteiras</Typography>
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
          </div>
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
          <Suspense fallback={<RouteFallback />}>
            <Routes>
              <Route
                path="/"
                element={
                  isAdminUser
                    ? <Navigate to="/admin" replace />
                    : <HomePage selectedPortfolioIds={selectedPortfolioIds} />
                }
              />
              <Route
                path="/carteira"
                element={isAdminUser ? <Navigate to="/admin" replace /> : <PortfolioPage selectedPortfolioIds={selectedPortfolioIds} />}
              />
              <Route
                path="/renda-fixa"
                element={isAdminUser ? <Navigate to="/admin" replace /> : <FixedIncomePage selectedPortfolioIds={selectedPortfolioIds} />}
              />
              <Route
                path="/graficos"
                element={isAdminUser ? <Navigate to="/admin" replace /> : <ChartsPage selectedPortfolioIds={selectedPortfolioIds} />}
              />
              <Route
                path="/ativo/:ticker"
                element={isAdminUser ? <Navigate to="/admin" replace /> : <AssetPage selectedPortfolioIds={selectedPortfolioIds} />}
              />
              <Route
                path="/nova"
                element={(isAdminUser || isViewerUser) ? <Navigate to="/" replace /> : <NewTransactionPage selectedPortfolioIds={selectedPortfolioIds} portfolios={portfolios} assets={assetSuggestions} />}
              />
              <Route
                path="/novo"
                element={(isAdminUser || isViewerUser) ? <Navigate to="/" replace /> : <NewIncomePage selectedPortfolioIds={selectedPortfolioIds} portfolios={portfolios} assets={assetSuggestions} />}
              />
              <Route
                path="/carteiras"
                element={
                  isAdminUser
                    ? <Navigate to="/admin" replace />
                    : isViewerUser
                      ? <Navigate to="/" replace />
                    : (
                      <PortfoliosPage
                        portfolios={portfolios}
                        selectedPortfolioIds={selectedPortfolioIds}
                        refreshPortfolios={refreshPortfolios}
                      />
                    )
                }
              />
              <Route
                path="/alocador"
                element={isAdminUser ? <Navigate to="/admin" replace /> : <AllocationPage assets={assetSuggestions} />}
              />
              <Route path="/scanner" element={<ScannerPage readOnly={isViewerUser} />} />
              <Route path="/swing-trade" element={<SwingTradePage readOnly={isViewerUser} />} />
              <Route
                path="/admin"
                element={isAdminUser ? <AdminPage currentUser={currentUser} /> : <Navigate to="/" replace />}
              />
              <Route
                path="/admin/sync-health"
                element={isAdminUser ? <SyncHealthPage /> : <Navigate to="/" replace />}
              />
              <Route
                path="/admin/metricas"
                element={isAdminUser ? <MetricFormulasPage /> : <Navigate to="/" replace />}
              />
              <Route
                path="/admin/metrics-lab"
                element={isAdminUser ? <ScannerMetricsLabPage /> : <Navigate to="/" replace />}
              />
              <Route path="/login" element={<Navigate to="/" replace />} />
              <Route
                path="/sync-health"
                element={isAdminUser ? <Navigate to="/admin/sync-health" replace /> : <Navigate to="/" replace />}
              />
              <Route path="/transacoes/nova" element={<Navigate to="/nova" replace />} />
              <Route path="/proventos/novo" element={<Navigate to="/novo" replace />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </Suspense>
        </Box>
      </Box>
      <Snackbar
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        open={toastState.open}
        autoHideDuration={toastState.durationMs}
        onClose={() => setToastState((current) => ({ ...current, open: false }))}
      >
        <Alert
          onClose={() => setToastState((current) => ({ ...current, open: false }))}
          severity={toastState.severity}
          variant="filled"
          className="app-v2-toast"
          sx={{ width: '100%' }}
        >
          <div className="app-v2-toast-body">
            <span>{toastState.message}</span>
            <small>{toastState.stamp}</small>
          </div>
        </Alert>
      </Snackbar>
    </Box>
  )
}

export default App
