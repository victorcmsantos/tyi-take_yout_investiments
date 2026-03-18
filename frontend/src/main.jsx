import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { CssBaseline, ThemeProvider, createTheme } from '@mui/material'
import App from './App'
import './styles.css'

function AppRoot() {
  const [mode, setMode] = React.useState(() => {
    const saved = localStorage.getItem('themeMode')
    if (saved === 'dark' || saved === 'light') return saved
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
  })

  React.useEffect(() => {
    localStorage.setItem('themeMode', mode)
    document.documentElement.setAttribute('data-theme', mode)
  }, [mode])

  const theme = React.useMemo(
    () =>
      createTheme({
        palette: {
          mode,
          primary: { main: '#38bdf8' },
          secondary: { main: mode === 'dark' ? '#0d1b2a' : '#14263d' },
          background: {
            default: mode === 'dark' ? '#06111f' : '#eaf4ff',
            paper: mode === 'dark' ? '#0d1828' : '#f7fbff',
          },
        },
        shape: {
          borderRadius: 20,
        },
        typography: {
          fontFamily: '"Manrope", "Segoe UI", sans-serif',
          h1: {
            fontFamily: '"Sora", "Manrope", sans-serif',
            fontWeight: 700,
          },
          h2: {
            fontFamily: '"Sora", "Manrope", sans-serif',
            fontWeight: 700,
          },
          h3: {
            fontFamily: '"Sora", "Manrope", sans-serif',
            fontWeight: 700,
          },
          button: {
            fontWeight: 700,
            textTransform: 'none',
            letterSpacing: '0.01em',
          },
        },
        components: {
          MuiPaper: {
            styleOverrides: {
              root: {
                backgroundImage: 'none',
              },
            },
          },
          MuiButton: {
            styleOverrides: {
              root: {
                borderRadius: 999,
                paddingInline: 16,
                boxShadow: 'none',
              },
            },
          },
        },
      }),
    [mode],
  )

  const toggleTheme = () => setMode((current) => (current === 'dark' ? 'light' : 'dark'))

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BrowserRouter>
        <App themeMode={mode} onToggleTheme={toggleTheme} />
      </BrowserRouter>
    </ThemeProvider>
  )
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <AppRoot />
  </React.StrictMode>,
)
