import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Button, Checkbox, FormControlLabel, Paper, Stack, Typography } from '@mui/material'
import { apiGet, apiPost } from '../api'

function AdminPage({ currentUser }) {
  const navigate = useNavigate()
  const [users, setUsers] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [isAdmin, setIsAdmin] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [backupLoading, setBackupLoading] = useState(false)
  const [lastBackup, setLastBackup] = useState(null)

  const loadUsers = async () => {
    setLoading(true)
    setError('')
    try {
      const payload = await apiGet('/api/admin/users')
      setUsers(payload.users || [])
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadUsers()
  }, [])

  const onCreateUser = async (event) => {
    event.preventDefault()
    setSubmitting(true)
    setError('')
    setMessage('')
    try {
      const payload = await apiPost('/api/admin/users', {
        username,
        password,
        is_admin: isAdmin,
      })
      setMessage(payload.message || 'Usuario criado com sucesso.')
      setUsername('')
      setPassword('')
      setIsAdmin(false)
      await loadUsers()
    } catch (err) {
      setError(err.message)
    } finally {
      setSubmitting(false)
    }
  }

  const onToggleUser = async (user) => {
    setError('')
    setMessage('')
    try {
      const payload = await apiPost(`/api/admin/users/${user.id}/status`, {
        is_active: !user.is_active,
      })
      setMessage(payload.message || 'Usuario atualizado com sucesso.')
      await loadUsers()
    } catch (err) {
      setError(err.message)
    }
  }

  const onCreateBackup = async () => {
    setBackupLoading(true)
    setError('')
    setMessage('')
    try {
      const payload = await apiPost('/api/backup/database')
      const backup = payload?.backup || null
      setLastBackup(backup)
      setMessage(backup ? `Backup criado: ${backup.filename}` : 'Backup criado com sucesso.')
    } catch (err) {
      setError(err.message)
    } finally {
      setBackupLoading(false)
    }
  }

  return (
    <section>
      <div className="hero-actions">
        <button type="button" className="btn-primary btn-link" onClick={() => navigate(-1)}>
          Voltar
        </button>
      </div>
      <div className="hero-line">
        <div>
          <h1>Admin</h1>
          <p className="subtitle">Usuario atual: {currentUser?.username}</p>
        </div>
      </div>

      {!!message && <p className="notice-ok">{message}</p>}
      {!!error && <p className="notice-warn">{error}</p>}

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Backup do banco</Typography>
        <Typography variant="body2" sx={{ mb: 2, opacity: 0.8 }}>
          Gere um snapshot manual do banco SQLite atual.
        </Typography>
        <Button variant="contained" onClick={onCreateBackup} disabled={backupLoading}>
          {backupLoading ? 'Gerando backup...' : 'Criar backup agora'}
        </Button>
        {lastBackup && (
          <div className="admin-user-meta" style={{ marginTop: 12 }}>
            <span>Arquivo: {lastBackup.filename}</span>
            <span>Criado em: {lastBackup.created_at}</span>
          </div>
        )}
      </Paper>

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 2 }}>Criar usuario</Typography>
        <form onSubmit={onCreateUser} className="admin-user-form">
          <label className="auth-field">
            <span>Usuario</span>
            <input value={username} onChange={(event) => setUsername(event.target.value)} placeholder="novo.usuario" />
          </label>
          <label className="auth-field">
            <span>Senha</span>
            <input type="password" value={password} onChange={(event) => setPassword(event.target.value)} placeholder="Minimo de 8 caracteres" />
          </label>
          <FormControlLabel
            control={<Checkbox checked={isAdmin} onChange={(event) => setIsAdmin(event.target.checked)} />}
            label="Administrador"
          />
          <Button type="submit" variant="contained" disabled={submitting}>
            {submitting ? 'Criando...' : 'Criar usuario'}
          </Button>
        </form>
      </Paper>

      <Paper className="admin-panel" sx={{ p: 2 }}>
        <Typography variant="h6" sx={{ mb: 2 }}>Usuarios</Typography>
        {loading ? (
          <p>Carregando...</p>
        ) : (
          <Stack spacing={1.5}>
            {users.map((user) => (
              <div key={user.id} className="admin-user-row">
                <div>
                  <strong>{user.username}</strong>
                  <div className="admin-user-meta">
                    <span>{user.is_admin ? 'Admin' : 'Usuario'}</span>
                    <span>{user.is_active ? 'Ativo' : 'Inativo'}</span>
                    <span>Ultimo login: {user.last_login_at || 'nunca'}</span>
                  </div>
                </div>
                <Button
                  variant={user.is_active ? 'outlined' : 'contained'}
                  color={user.is_active ? 'warning' : 'success'}
                  onClick={() => onToggleUser(user)}
                >
                  {user.is_active ? 'Desabilitar' : 'Habilitar'}
                </Button>
              </div>
            ))}
            {users.length === 0 && <p>Nenhum usuario cadastrado.</p>}
          </Stack>
        )}
      </Paper>
    </section>
  )
}

export default AdminPage
