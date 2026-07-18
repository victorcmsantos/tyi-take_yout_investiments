import { useEffect, useMemo, useState } from 'react'
import { apiPost } from '../api'
import { useApiQuery } from '../hooks/useApiQuery'

// OpenClaw responses can take up to ~150s; the default 30s fetch timeout would
// abort the generation. Give it generous headroom.
const GENERATE_TIMEOUT_MS = 180000

const STANCE = {
  defensivo: { label: 'Defensivo', tone: 'down' },
  neutro: { label: 'Neutro', tone: '' },
  construtivo: { label: 'Construtivo', tone: 'up' },
}

const ACTION = {
  aumentar: { label: 'Aumentar', tone: 'up' },
  reduzir: { label: 'Reduzir', tone: 'down' },
  manter: { label: 'Manter', tone: '' },
  revisar: { label: 'Revisar', tone: '' },
}

const PRIORITY = {
  alta: { label: 'Alta', tone: 'down' },
  media: { label: 'Média', tone: '' },
  baixa: { label: 'Baixa', tone: 'up' },
}

function PortfolioAiAnalysisCard({ selectedPortfolioIds }) {
  const { data, refetch } = useApiQuery('/api/portfolio/analysis', {
    params: { portfolio_id: selectedPortfolioIds },
  })
  const [generating, setGenerating] = useState(false)
  const [message, setMessage] = useState('')
  const [override, setOverride] = useState(null)

  // Drop the locally-generated result when the selected scope changes, so an
  // analysis generated for one portfolio set never leaks onto another.
  const scopeKey = JSON.stringify(selectedPortfolioIds || [])
  useEffect(() => {
    setOverride(null)
    setMessage('')
  }, [scopeKey])

  const analysis = override || data?.analysis || null
  const payload = analysis?.payload || null
  const hasPayload = !!payload && (
    (payload.resumo && payload.resumo.trim()) ||
    (Array.isArray(payload.sugestoes) && payload.sugestoes.length > 0) ||
    (Array.isArray(payload.riscos_concentracao) && payload.riscos_concentracao.length > 0)
  )
  const rawReply = (analysis?.raw_reply || '').trim()
  const stance = STANCE[payload?.postura_geral] || STANCE.neutro

  const generate = async () => {
    setGenerating(true)
    setMessage('')
    try {
      const res = await apiPost('/api/portfolio/analyze/openclaw', {}, {
        portfolio_id: selectedPortfolioIds,
      }, { timeoutMs: GENERATE_TIMEOUT_MS })
      setMessage(res?.message || 'OK')
      setOverride(res?.analysis || null)
      refetch()
    } catch (err) {
      setMessage(err?.message || 'Falha ao gerar análise.')
    } finally {
      setGenerating(false)
    }
  }

  const suggestions = useMemo(
    () => (Array.isArray(payload?.sugestoes) ? payload.sugestoes : []),
    [payload],
  )

  return (
    <article className="card detail-card openclaw-card">
      <div className="hero-line">
        <div>
          <h3>Análise da carteira (IA)</h3>
          <p className="subtitle">
            Leitura da carteira inteira via OpenClaw, ancorada nos pesos, P&amp;L e concentração reais. O botão ignora o cache.
          </p>
        </div>
        <button type="button" className="btn-primary" onClick={generate} disabled={generating}>
          {generating ? 'Analisando...' : (hasPayload ? 'Atualizar análise' : 'Gerar análise')}
        </button>
      </div>

      {!!message && (
        <p className={message === 'OK' || message.includes('OK') ? 'notice-ok' : 'notice-warn'}>{message}</p>
      )}

      {hasPayload ? (
        <>
          <div className="openclaw-meta">
            <span className={`analysis-pill ${stance.tone}`}>Postura: {stance.label}</span>
            {!!analysis?.generated_at && (
              <span className="meta-chip">Gerado em {analysis.generated_at}</span>
            )}
          </div>

          <div className="openclaw-grid">
            {!!payload.resumo && (
              <section className="openclaw-section openclaw-section-wide openclaw-section-lead">
                <span className="section-kicker">Resumo</span>
                <p>{payload.resumo}</p>
              </section>
            )}

            {!!payload.diversificacao && (
              <section className="openclaw-section">
                <span className="section-kicker">Diversificação</span>
                <p>{payload.diversificacao}</p>
              </section>
            )}

            {Array.isArray(payload.riscos_concentracao) && payload.riscos_concentracao.length > 0 && (
              <section className="openclaw-section openclaw-section-list">
                <span className="section-kicker">Riscos de concentração</span>
                <ul className="analysis-list">
                  {payload.riscos_concentracao.map((item, idx) => (
                    <li key={`risco-${idx}`}>{item}</li>
                  ))}
                </ul>
              </section>
            )}

            {!!payload.observacoes && (
              <section className="openclaw-section">
                <span className="section-kicker">Observações</span>
                <p>{payload.observacoes}</p>
              </section>
            )}
          </div>

          {suggestions.length > 0 && (
            <div className="openclaw-grid">
              {suggestions.map((sug, idx) => {
                const action = ACTION[sug.acao] || ACTION.revisar
                const priority = PRIORITY[sug.prioridade] || PRIORITY.media
                return (
                  <section key={`sug-${idx}`} className="openclaw-section">
                    <div className="openclaw-meta">
                      <span className={`analysis-pill ${action.tone}`}>{action.label}</span>
                      {!!sug.alvo && <span className="meta-chip">{sug.alvo}</span>}
                      <span className={`analysis-pill ${priority.tone}`}>Prioridade {priority.label}</span>
                    </div>
                    {!!sug.titulo && <strong>{sug.titulo}</strong>}
                    {!!sug.motivo && <p>{sug.motivo}</p>}
                  </section>
                )
              })}
            </div>
          )}
        </>
      ) : rawReply ? (
        <>
          <p><strong>Resposta bruta do OpenClaw:</strong></p>
          <pre style={{ whiteSpace: 'pre-wrap', margin: 0 }}>{rawReply}</pre>
        </>
      ) : (
        <p className="subtitle">Sem análise ainda. Clique em “Gerar análise”.</p>
      )}
    </article>
  )
}

export default PortfolioAiAnalysisCard
