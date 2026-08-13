import { useEffect, useState } from 'react'
import { apiPost } from '../api'
import { useApiQuery } from '../hooks/useApiQuery'
import { formatCurrencyBRL } from '../formatters'

// OpenClaw responses can take up to ~150s; the default 30s fetch timeout would
// abort the generation. Give it generous headroom.
const GENERATE_TIMEOUT_MS = 180000

const HEALTH = {
  boa: { label: 'Boa', tone: 'up' },
  atencao: { label: 'Atenção', tone: '' },
  alerta: { label: 'Alerta', tone: 'down' },
}

const SEVERITY = {
  alta: { label: 'Alta', tone: 'down' },
  media: { label: 'Média', tone: '' },
  baixa: { label: 'Baixa', tone: 'up' },
}

function FinanceInsightsCard({ monthParam }) {
  const { data, refetch } = useApiQuery('/api/financas/insights', {
    params: { month: monthParam },
  })
  const [generating, setGenerating] = useState(false)
  const [message, setMessage] = useState('')
  const [override, setOverride] = useState(null)

  // Drop the locally-generated result when the viewed month changes, so insights
  // generated for one month never leak onto another.
  useEffect(() => {
    setOverride(null)
    setMessage('')
  }, [monthParam])

  const insights = override || data?.insights || null
  const payload = insights?.payload || null
  const hasPayload = !!payload && (
    (payload.resumo && payload.resumo.trim()) ||
    (Array.isArray(payload.insights) && payload.insights.length > 0) ||
    (Array.isArray(payload.sugestoes) && payload.sugestoes.length > 0)
  )
  const rawReply = (insights?.raw_reply || '').trim()
  const health = HEALTH[payload?.saude] || HEALTH.atencao
  const stale = insights?.month && monthParam && insights.month !== monthParam

  const generate = async () => {
    setGenerating(true)
    setMessage('')
    try {
      const res = await apiPost('/api/financas/insights/openclaw', { month: monthParam }, {}, {
        timeoutMs: GENERATE_TIMEOUT_MS,
      })
      setMessage(res?.message || 'OK')
      setOverride(res?.insights || null)
      refetch()
    } catch (err) {
      setMessage(err?.message || 'Falha ao gerar insights.')
    } finally {
      setGenerating(false)
    }
  }

  const items = Array.isArray(payload?.insights) ? payload.insights : []
  const suggestions = Array.isArray(payload?.sugestoes) ? payload.sugestoes : []

  return (
    <article className="card detail-card openclaw-card">
      <div className="hero-line">
        <div>
          <h3>Insights do mês (IA)</h3>
          <p className="subtitle">
            Leitura proativa via OpenClaw sobre seus gastos do mês, ancorada nas categorias e variações reais das suas contas. O botão ignora o cache.
          </p>
        </div>
        <button type="button" className="btn-primary" onClick={generate} disabled={generating}>
          {generating ? 'Analisando...' : (hasPayload ? 'Atualizar insights' : 'Gerar insights')}
        </button>
      </div>

      {!!message && (
        <p className={message === 'OK' || message.includes('OK') ? 'notice-ok' : 'notice-warn'}>{message}</p>
      )}

      {hasPayload ? (
        <>
          <div className="openclaw-meta">
            <span className={`analysis-pill ${health.tone}`}>Saúde: {health.label}</span>
            {!!insights?.month && <span className="meta-chip">Mês {insights.month}</span>}
            {stale && <span className="analysis-pill down">Insights de outro mês</span>}
            {!!insights?.generated_at && <span className="meta-chip">Gerado em {insights.generated_at}</span>}
          </div>

          {!!payload.resumo && (
            <div className="openclaw-grid">
              <section className="openclaw-section openclaw-section-wide openclaw-section-lead">
                <span className="section-kicker">Resumo do mês</span>
                <p>{payload.resumo}</p>
              </section>
            </div>
          )}

          {items.length > 0 && (
            <div className="openclaw-grid">
              {items.map((it, idx) => {
                const severity = SEVERITY[it.severidade] || SEVERITY.media
                return (
                  <section key={`ins-${idx}`} className="openclaw-section">
                    <div className="openclaw-meta">
                      <span className={`analysis-pill ${severity.tone}`}>{severity.label}</span>
                      {!!it.categoria && <span className="meta-chip">{it.categoria}</span>}
                      {typeof it.valor === 'number' && <span className="meta-chip">{formatCurrencyBRL(it.valor)}</span>}
                      {typeof it.delta_pct === 'number' && (
                        <span className={`analysis-pill ${it.delta_pct >= 0 ? 'down' : 'up'}`}>
                          {it.delta_pct >= 0 ? '+' : ''}{it.delta_pct.toFixed(1)}%
                        </span>
                      )}
                    </div>
                    {!!it.titulo && <strong>{it.titulo}</strong>}
                    {!!it.mensagem && <p>{it.mensagem}</p>}
                  </section>
                )
              })}
            </div>
          )}

          {suggestions.length > 0 && (
            <div className="openclaw-grid">
              <section className="openclaw-section openclaw-section-wide openclaw-section-list">
                <span className="section-kicker">Sugestões</span>
                <ul className="analysis-list">
                  {suggestions.map((item, idx) => (
                    <li key={`sug-${idx}`}>{item}</li>
                  ))}
                </ul>
              </section>
            </div>
          )}
        </>
      ) : rawReply ? (
        <>
          <p><strong>Resposta bruta do OpenClaw:</strong></p>
          <pre style={{ whiteSpace: 'pre-wrap', margin: 0 }}>{rawReply}</pre>
        </>
      ) : (
        <p className="subtitle">Sem insights ainda. Clique em “Gerar insights”.</p>
      )}
    </article>
  )
}

export default FinanceInsightsCard
