import { useState } from 'react'
import { Button, Paper, Typography } from '@mui/material'
import { apiPost } from '../api'

function LoginPage({ onLoggedIn }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const onSubmit = async (event) => {
    event.preventDefault()
    setLoading(true)
    setError('')
    try {
      const payload = await apiPost('/api/auth/login', { username, password })
      onLoggedIn(payload.user)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <main className="auth-shell">
      <Paper className="auth-card" elevation={6}>
        <Typography variant="h4" className="auth-title">Entrar</Typography>
        <Typography variant="body2" className="auth-subtitle">
          Use seu usuario cadastrado para acessar o portal.
        </Typography>
        <form onSubmit={onSubmit} className="auth-form">
          <label className="auth-field">
            <span>Usuario</span>
            <input
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              autoComplete="username"
              placeholder="admin"
            />
          </label>
          <label className="auth-field">
            <span>Senha</span>
            <input
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              autoComplete="current-password"
              placeholder="Sua senha"
            />
          </label>
          {!!error && <p className="error">{error}</p>}
          <Button type="submit" variant="contained" size="large" disabled={loading}>
            {loading ? 'Entrando...' : 'Entrar'}
          </Button>
        </form>
      </Paper>
    </main>
  )
}

export default LoginPage
