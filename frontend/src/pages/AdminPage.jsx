import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Button, Checkbox, FormControlLabel, Paper, Stack, Typography } from '@mui/material'
import { apiGet, apiPost } from '../api'
import { currentBrowserTimeZone, formatDateTimeLocal } from '../datetime'

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
  const [backups, setBackups] = useState([])
  const [backupsLoading, setBackupsLoading] = useState(false)
  const [batchLoading, setBatchLoading] = useState(false)
  const [batchOnlyMissing, setBatchOnlyMissing] = useState(true)
  const [batchLimit, setBatchLimit] = useState('10')
  const [batchTickers, setBatchTickers] = useState('')
  const [batchResult, setBatchResult] = useState(null)
  const browserTimeZone = currentBrowserTimeZone()

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

  const loadBackups = async () => {
    setBackupsLoading(true)
    setError('')
    try {
      const payload = await apiGet('/api/backup/database')
      setBackups(payload.backups || [])
    } catch (err) {
      setError(err.message)
    } finally {
      setBackupsLoading(false)
    }
  }

  useEffect(() => {
    loadBackups()
  }, [])

  const formatBytes = (value) => {
    const size = Number(value)
    if (!Number.isFinite(size) || size <= 0) return '0 B'
    const units = ['B', 'KB', 'MB', 'GB', 'TB']
    const exponent = Math.min(Math.floor(Math.log(size) / Math.log(1024)), units.length - 1)
    const number = size / 1024 ** exponent
    const digits = exponent === 0 ? 0 : number < 10 ? 2 : 1
    return `${number.toFixed(digits)} ${units[exponent]}`
  }

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
      const createdBackups = Array.isArray(payload?.backups) ? payload.backups : []
      const backup = payload?.backup || createdBackups[0] || null
      setLastBackup(backup)
      if (createdBackups.length > 1) {
        const names = createdBackups.map((item) => item.filename).join(', ')
        setMessage(`Backups criados (${createdBackups.length}): ${names}`)
      } else {
        setMessage(backup ? `Backup criado: ${backup.filename}` : 'Backup criado com sucesso.')
      }
      await loadBackups()
    } catch (err) {
      setError(err.message)
    } finally {
      setBackupLoading(false)
    }
  }

  const onRunOpenClawBatch = async () => {
    setBatchLoading(true)
    setError('')
    setMessage('')
    setBatchResult(null)
    try {
      const payload = await apiPost('/api/admin/openclaw/enrich-assets', {
        only_missing: batchOnlyMissing,
        limit: String(batchLimit || '').trim() ? Number(batchLimit) : null,
        tickers: String(batchTickers || '')
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean),
      })
      setBatchResult(payload || null)
      setMessage(
        payload
          ? `Lote finalizado: ${payload.success_count || 0} sucesso(s), ${payload.failure_count || 0} falha(s), ${payload.skipped_count || 0} ignorado(s).`
          : 'Lote concluido.'
      )
    } catch (err) {
      setError(err.message)
    } finally {
      setBatchLoading(false)
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
          Gere um snapshot manual dos bancos SQLite (backend e market scanner).
        </Typography>
        <Typography variant="caption" sx={{ mb: 1.5, display: 'block', opacity: 0.7 }}>
          Horários exibidos no seu fuso: {browserTimeZone}
        </Typography>
        <Button variant="contained" onClick={onCreateBackup} disabled={backupLoading}>
          {backupLoading ? 'Gerando backup...' : 'Criar backup agora'}
        </Button>
        {lastBackup && (
          <div className="admin-user-meta" style={{ marginTop: 12 }}>
            <span>Arquivo: {lastBackup.filename}</span>
            <span>Criado em: {formatDateTimeLocal(lastBackup.created_at)}</span>
          </div>
        )}

        <div style={{ marginTop: 16 }}>
          <Typography variant="subtitle2" sx={{ mb: 1, opacity: 0.85 }}>Backups existentes</Typography>
          {backupsLoading ? (
            <p>Carregando backups...</p>
          ) : backups.length === 0 ? (
            <p>Nenhum backup encontrado.</p>
          ) : (
            <div className="table-wrap">
              <table className="asset-table">
                <thead>
                  <tr>
                    <th>Banco</th>
                    <th>Arquivo</th>
                    <th>Modificado em</th>
                    <th>Tamanho</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {backups.map((backup) => (
                    <tr key={backup.filename}>
                      <td>{backup.database_label || backup.database_key || '-'}</td>
                      <td>{backup.filename}</td>
                      <td>{formatDateTimeLocal(backup.modified_at)}</td>
                      <td>{formatBytes(backup.size_bytes)}</td>
                      <td style={{ textAlign: 'right' }}>
                        <Button
                          variant="outlined"
                          size="small"
                          component="a"
                          href={`/api/backup/database/${encodeURIComponent(backup.filename)}`}
                        >
                          Download
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </Paper>

      <Paper className="admin-panel" sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>Enriquecimento OpenClaw em lote</Typography>
        <Typography variant="body2" sx={{ mb: 2, opacity: 0.8 }}>
          Processa um ativo por vez, aguarda a resposta do OpenClaw e grava cada resultado no banco antes de seguir.
        </Typography>
        <div className="form-grid" style={{ marginBottom: 12 }}>
          <label className="auth-field">
            <span>Limite</span>
            <input
              value={batchLimit}
              onChange={(event) => setBatchLimit(event.target.value)}
              placeholder="10"
              inputMode="numeric"
            />
          </label>
          <label className="auth-field" style={{ gridColumn: 'span 3' }}>
            <span>Tickers específicos (opcional)</span>
            <input
              value={batchTickers}
              onChange={(event) => setBatchTickers(event.target.value)}
              placeholder="ITUB4, BBDC4, MXRF11"
            />
          </label>
        </div>
        <FormControlLabel
          control={<Checkbox checked={batchOnlyMissing} onChange={(event) => setBatchOnlyMissing(event.target.checked)} />}
          label="Processar apenas ativos sem enriquecimento salvo"
        />
        <div style={{ marginTop: 12 }}>
          <Button variant="contained" onClick={onRunOpenClawBatch} disabled={batchLoading}>
            {batchLoading ? 'Rodando lote...' : 'Rodar lote OpenClaw'}
          </Button>
        </div>

        {batchResult && (
          <div style={{ marginTop: 16 }}>
            <div className="admin-user-meta" style={{ marginBottom: 12 }}>
              <span>Processados: {batchResult.processed_count || 0}</span>
              <span>Sucesso: {batchResult.success_count || 0}</span>
              <span>Falhas: {batchResult.failure_count || 0}</span>
              <span>Ignorados: {batchResult.skipped_count || 0}</span>
            </div>

            {Array.isArray(batchResult.results) && batchResult.results.length > 0 && (
              <div className="table-wrap">
                <table className="asset-table">
                  <thead>
                    <tr>
                      <th>Ticker</th>
                      <th>Status</th>
                      <th>Mensagem</th>
                      <th>Atualizado em</th>
                    </tr>
                  </thead>
                  <tbody>
                    {batchResult.results.map((item) => (
                      <tr key={`${item.ticker}-${item.updated_at || item.message}`}>
                        <td>{item.ticker}</td>
                        <td>{item.ok ? 'OK' : 'Falha'}</td>
                        <td>{item.message}</td>
                        <td>{item.updated_at ? formatDateTimeLocal(item.updated_at) : '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
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
                    <span>Ultimo login: {user.last_login_at ? formatDateTimeLocal(user.last_login_at) : 'nunca'}</span>
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
