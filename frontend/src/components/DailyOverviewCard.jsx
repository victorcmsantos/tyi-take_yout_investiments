import { useEffect, useState } from 'react'
import { apiPost } from '../api'
import { useApiQuery } from '../hooks/useApiQuery'

// OpenClaw pode levar ate ~150s; o timeout padrao de fetch abortaria a geracao.
const GENERATE_TIMEOUT_MS = 180000

const TONE = {
  positivo: { label: 'Dia positivo', tone: 'up' },
  negativo: { label: 'Dia negativo', tone: 'down' },
  neutro: { label: 'Dia neutro', tone: '' },
}

const brlFull = (value) => `R$ ${Number(value || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
const brlCompact = (value) => `R$ ${Number(value || 0).toLocaleString('pt-BR', { notation: 'compact', maximumFractionDigits: 2 })}`
const signedBrl = (value) => `${Number(value || 0) >= 0 ? '+' : ''}${brlFull(value)}`
const signedPct = (value) => `${Number(value || 0) >= 0 ? '+' : ''}${Number(value || 0).toFixed(2).replace('.', ',')}%`
const dateBr = (iso) => {
  const [year, month, day] = String(iso || '').split('-')
  return year ? `${day}/${month}/${year}` : String(iso || '')
}

function MoverChip({ direction, mover }) {
  if (!mover) return null
  const up = direction === 'best'
  return (
    <span className={`daily-overview-mover ${up ? 'up' : 'down'}`}>
      {up ? '▲' : '▼'} {mover.ticker} {signedPct(mover.variation_day)}
    </span>
  )
}

function DailyOverviewCard({ selectedPortfolioIds }) {
  const { data, refetch, loading } = useApiQuery('/api/portfolio/daily-overview', {
    params: { portfolio_id: selectedPortfolioIds },
  })
  const [generating, setGenerating] = useState(false)
  const [message, setMessage] = useState('')
  const [override, setOverride] = useState(null)

  const scopeKey = JSON.stringify(selectedPortfolioIds || [])
  useEffect(() => {
    setOverride(null)
    setMessage('')
  }, [scopeKey])

  const facts = data?.facts || null
  const ai = override || data?.ai || null
  const aiPayload = ai?.payload || null
  const hasAi = !!aiPayload && (aiPayload.resumo || (aiPayload.destaques || []).length > 0)
  const tone = TONE[aiPayload?.tom] || TONE.neutro

  const generate = async () => {
    setGenerating(true)
    setMessage('')
    try {
      const res = await apiPost('/api/portfolio/daily-overview/openclaw', {}, {
        portfolio_id: selectedPortfolioIds,
      }, { timeoutMs: GENERATE_TIMEOUT_MS })
      setMessage(res?.message || 'OK')
      setOverride(res?.ai || null)
      refetch()
    } catch (err) {
      setMessage(err?.message || 'Falha ao gerar leitura.')
    } finally {
      setGenerating(false)
    }
  }

  const portfolio = facts?.portfolio || null
  const dayUp = Number(portfolio?.day_change_value || 0) >= 0
  const classes = facts?.classes || []
  const fixed = facts?.fixed_income || null
  const upcoming = fixed?.upcoming || []

  return (
    <article className="card detail-card openclaw-card daily-overview-card dashboard-animate">
      <div className="hero-line">
        <div>
          <h3>Overview do dia (IA)</h3>
          <p className="subtitle">
            Variação de hoje, destaques por classe e vencimentos de renda fixa — números reais; a IA só interpreta.
          </p>
        </div>
        <button type="button" className="btn-primary" onClick={generate} disabled={generating}>
          {generating ? 'Gerando...' : (hasAi ? 'Atualizar leitura IA' : 'Gerar leitura IA')}
        </button>
      </div>

      {!!message && (
        <p className={message === 'OK' || message.includes('OK') ? 'notice-ok' : 'notice-warn'}>{message}</p>
      )}

      {loading && !facts ? (
        <p className="subtitle">Calculando o dia da carteira...</p>
      ) : portfolio ? (
        <>
          <p className={`daily-overview-headline ${dayUp ? 'up' : 'down'}`}>
            Sua carteira {dayUp ? 'subiu' : 'caiu'} {brlFull(Math.abs(portfolio.day_change_value))}
            {' '}({signedPct(portfolio.day_change_pct)}) hoje
            <small> · renda variável de {brlCompact(portfolio.total_value)} · {dateBr(facts.ref_date)}</small>
          </p>

          {classes.length > 0 && (
            <div className="daily-overview-classes">
              {classes.map((cls) => (
                <div key={cls.key} className="daily-overview-class-row">
                  <span className="daily-overview-class-label">{cls.label}</span>
                  <strong className={Number(cls.day_change_pct) >= 0 ? 'up' : 'down'}>
                    {signedPct(cls.day_change_pct)}
                  </strong>
                  <span className={`daily-overview-class-value ${Number(cls.change_value) >= 0 ? 'up' : 'down'}`}>
                    {signedBrl(cls.change_value)}
                  </span>
                  <span className="daily-overview-movers">
                    <MoverChip direction="best" mover={cls.best} />
                    {cls.worst && cls.best && cls.worst.ticker !== cls.best.ticker ? (
                      <MoverChip direction="worst" mover={cls.worst} />
                    ) : null}
                  </span>
                </div>
              ))}
            </div>
          )}

          {fixed && (fixed.future_count > 0 || upcoming.length > 0) && (
            <section className="openclaw-section daily-overview-fixed">
              <span className="section-kicker">Vencimentos de renda fixa</span>
              <div className="openclaw-meta">
                <span className={`analysis-pill ${Number(fixed.maturing_30d_total) > 0 ? 'down' : ''}`}>
                  30 dias: {brlCompact(fixed.maturing_30d_total)}
                </span>
                <span className="analysis-pill">90 dias: {brlCompact(fixed.maturing_90d_total)}</span>
                <span className="meta-chip">{fixed.future_count} título(s) a vencer</span>
              </div>
              {upcoming.length > 0 && (
                <ul className="analysis-list">
                  {upcoming.map((item, idx) => (
                    <li key={`venc-${idx}`}>
                      {item.issuer} {item.investment_type} · {brlFull(item.amount)} · vence {dateBr(item.maturity_date)} (em {item.days_left} dia{item.days_left === 1 ? '' : 's'})
                    </li>
                  ))}
                </ul>
              )}
            </section>
          )}

          {hasAi ? (
            <div className="openclaw-grid">
              <section className="openclaw-section openclaw-section-wide openclaw-section-lead">
                <div className="openclaw-meta">
                  <span className={`analysis-pill ${tone.tone}`}>{tone.label}</span>
                  {!!ai?.generated_at && <span className="meta-chip">Leitura gerada em {ai.generated_at}</span>}
                  {!!ai?.ref_date && ai.ref_date !== facts.ref_date && (
                    <span className="analysis-pill down">Leitura de {dateBr(ai.ref_date)} — gere novamente</span>
                  )}
                </div>
                {!!aiPayload.resumo && <p>{aiPayload.resumo}</p>}
                {(aiPayload.destaques || []).length > 0 && (
                  <ul className="analysis-list">
                    {aiPayload.destaques.map((item, idx) => (
                      <li key={`destaque-${idx}`}>{item}</li>
                    ))}
                  </ul>
                )}
                {!!aiPayload.alerta_renda_fixa && <p><strong>Renda fixa:</strong> {aiPayload.alerta_renda_fixa}</p>}
              </section>
            </div>
          ) : (
            <p className="subtitle">Sem leitura IA ainda. Clique em “Gerar leitura IA” para a narrativa do dia.</p>
          )}
        </>
      ) : (
        <p className="subtitle">Sem dados suficientes para montar o overview do dia.</p>
      )}
    </article>
  )
}

export default DailyOverviewCard
