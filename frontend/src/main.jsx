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
          primary: { main: '#0f8a77' },
          secondary: { main: mode === 'dark' ? '#173a5f' : '#0b4c7a' },
          background: {
            default: mode === 'dark' ? '#0f1722' : '#edf2f9',
            paper: mode === 'dark' ? '#172232' : '#ffffff',
          },
        },
        shape: {
          borderRadius: 10,
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
