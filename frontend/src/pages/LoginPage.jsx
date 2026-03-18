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
      <div className="auth-layout">
        <section className="auth-aside">
          <div className="auth-brand">
            <img src="/logo-casalinvest.svg" alt="CasalInvest" className="auth-brand-logo" />
            <span>CasalInvest</span>
          </div>
          <small className="auth-kicker">Painel financeiro pessoal</small>
          <Typography variant="h2" className="auth-display">
            Seu patrimonio com leitura mais elegante, clara e confiavel.
          </Typography>
          <Typography variant="body1" className="auth-lead">
            Acompanhe carteira, renda passiva e sinais de mercado em uma experiencia mais limpa e focada em decisao.
          </Typography>
          <div className="auth-feature-list">
            <article className="auth-feature">
              <strong>Visao executiva</strong>
              <span>Dashboard pronta para leitura rapida, com contexto e hierarquia visual.</span>
            </article>
            <article className="auth-feature">
              <strong>Carteira organizada</strong>
              <span>Ativos, setores e proventos apresentados em blocos mais nativos e sofisticados.</span>
            </article>
            <article className="auth-feature">
              <strong>Fluxo continuo</strong>
              <span>Busca, navegacao e acompanhamento diario em um ambiente mais leve e premium.</span>
            </article>
          </div>
        </section>

        <Paper className="auth-card" elevation={0}>
          <small className="auth-card-kicker">Acesso seguro</small>
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
            <Button type="submit" variant="contained" size="large" disabled={loading} className="auth-submit-btn">
              {loading ? 'Entrando...' : 'Entrar'}
            </Button>
          </form>
        </Paper>
      </div>
    </main>
  )
}

export default LoginPage
