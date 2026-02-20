import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, Navigate, Route, Routes } from 'react-router-dom'
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

function App() {
  const [portfolios, setPortfolios] = useState([])
  const [selectedPortfolioIds, setSelectedPortfolioIds] = useState([])
  const [loadingPortfolios, setLoadingPortfolios] = useState(true)
  const [error, setError] = useState('')

  const refreshPortfolios = useCallback(async () => {
    const data = await apiGet('/api/portfolios')
    setPortfolios(data)
    setSelectedPortfolioIds((current) => {
      const valid = current.filter((id) => data.some((p) => Number(p.id) === Number(id)))
      if (valid.length > 0) return valid
      if (data.length > 0) return [data[0].id]
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
    <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
      <AppBar position="static" color="secondary" elevation={0}>
        <Toolbar sx={{ display: 'flex', flexDirection: 'column', alignItems: 'stretch', gap: 1, py: 1 }}>
          <Stack direction={{ xs: 'column', md: 'row' }} spacing={1} alignItems={{ xs: 'flex-start', md: 'center' }} justifyContent="space-between">
            <Typography
              component={Link}
              to="/"
              variant="h5"
              sx={{ color: '#fff', textDecoration: 'none', fontWeight: 700 }}
            >
              Invest Portal
            </Typography>
            <Stack direction="row" spacing={1} flexWrap="wrap">
              <Button component={Link} to="/carteira" color="inherit">Renda Variavel</Button>
              <Button component={Link} to="/renda-fixa" color="inherit">Renda Fixa</Button>
              <Button component={Link} to="/graficos" color="inherit">Graficos</Button>
              <Button component={Link} to="/nova" color="inherit">Nova transacao</Button>
              <Button component={Link} to="/novo" color="inherit">Novo provento</Button>
            </Stack>
          </Stack>
          <Typography variant="body2" sx={{ opacity: 0.9 }}>
            Carteira ativa: {activePortfolioName}
          </Typography>
        </Toolbar>
      </AppBar>

      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '250px 1fr' }, gap: 2, p: 2 }}>
        <Paper sx={{ p: 2, alignSelf: 'start' }}>
          <Typography variant="h6" sx={{ mb: 1 }}>Carteiras</Typography>
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

        <Box>
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
