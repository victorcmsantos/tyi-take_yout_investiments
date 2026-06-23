import { useMemo, useState } from 'react'
import { Chart as ChartJS } from 'chart.js/auto'
import { Line } from 'react-chartjs-2'
import StatePanel from '../components/StatePanel'
import { useApiQuery } from '../hooks/useApiQuery'
import { formatCurrencyBRL, formatCompactBrl, formatPercent } from '../formatters'

const MONTHS = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
const TABS = ['Visão geral', 'Transações', 'Cartões', 'Categorias']
// Categories that are not real spending (settlement / internal moves).
const HIDDEN_CATS = new Set(['Pagamento de cartão de crédito', 'Transferência mesma titularidade'])
const CAT_EMOJI = {
  'Vestiário': '🛍️', 'Supermercado': '🛒', 'Compras': '🛍️', 'Compras online': '🛒',
  'Transferências': '💸', 'Transferência - PIX': '💸', 'Transferência - TED': '💸',
  'Utensílios para casa': '🏠', 'Manutenção de veículos': '🚗', 'Eletrônicos': '📱',
  'Restaurantes, bares e lanchonetes': '🍽️', 'Delivery de alimentos': '🛵', 'Farmácia': '💊',
  'Saúde': '🩺', 'Saúde e bem-estar': '🧘', 'Serviços': '🧾', 'Serviços digitais': '💻',
  'Seguros': '🛡️', 'Escola': '🎓', 'Telecomunicação': '📡', 'Internet': '🌐',
  'Postos de gasolina': '⛽', 'Táxi e transporte privado urbano': '🚕', 'Viagens': '✈️',
  'Pet Shops e veterinários': '🐾', 'Estacionamentos': '🅿️', 'Bilhetes': '🎫',
  'Pedágios e pagamentos no veículo': '🛣️', 'Impostos sobre operações financeiras': '🏛️',
  'Alimentos e bebidas': '🍔', 'Não categorizada': '❔',
}
const emojiFor = (c) => CAT_EMOJI[c] || '🏷️'

function BucketStrip({ buckets }) {
  const items = [
    { key: 'receita', label: 'Receita', tone: 'pos' },
    { key: 'despfixa', label: 'Desp. fixa', tone: 'neg' },
    { key: 'cartao', label: 'Cartão', tone: 'neg' },
    { key: 'despavulsa', label: 'Desp. avulsa', tone: 'neg' },
    { key: 'sobra', label: 'Sobra', tone: (buckets.sobra || 0) >= 0 ? 'pos' : 'neg' },
  ]
  return (
    <div className="fin2-strip">
      {items.map((it) => (
        <div key={it.key} className={`fin2-strip-item fin2-${it.tone}`}>
          <span className="fin2-strip-label">{it.label}</span>
          <strong>{formatCurrencyBRL(buckets[it.key])}</strong>
        </div>
      ))}
    </div>
  )
}

function RecentList({ kicker, items }) {
  return (
    <article className="fin2-card fin2-recent">
      <span className="fin2-kicker">{kicker}</span>
      <ul className="fin2-tx-list">
        {(items || []).length === 0 ? (
          <li className="fin2-tx-empty"><small className="fin2-muted">Sem transações no período.</small></li>
        ) : (
          items.map((t, i) => (
            <li key={i}>
              <div><strong>{t.description || t.category}</strong><small>{t.date} · {t.category}</small></div>
              <span className={t.flow === 'in' ? 'fin2-pos' : 'fin2-neg'}>
                {t.flow === 'in' ? '' : '−'}{formatCurrencyBRL(t.amount)}
              </span>
            </li>
          ))
        )}
      </ul>
    </article>
  )
}

function FinancasPage() {
  const [ref, setRef] = useState(() => {
    const now = new Date()
    return { year: now.getFullYear(), month: now.getMonth() + 1 }
  })
  const [tab, setTab] = useState('Visão geral')
  const monthParam = `${ref.year}-${String(ref.month).padStart(2, '0')}`
  const { data, loading, error, refetch } = useApiQuery('/api/pierre/overview', {
    params: { month: monthParam },
    cached: true,
    cacheOptions: { ttlMs: 60000, staleWhileRevalidate: true },
  })

  const shiftMonth = (delta) => setRef((cur) => {
    const d = new Date(cur.year, cur.month - 1 + delta, 1)
    return { year: d.getFullYear(), month: d.getMonth() + 1 }
  })

  const rhythm = useMemo(() => {
    const c = data?.spend?.cumulative
    if (!c) return null
    return {
      labels: c.days,
      datasets: [
        { label: 'Este mês', data: c.current, borderColor: '#f0563f', backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 2 },
        { label: 'Mês passado', data: c.previous, borderColor: 'rgba(150,160,175,0.6)', borderDash: [5, 5], backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 1.5 },
      ],
    }
  }, [data])

  if (loading && !data) {
    return <StatePanel busy eyebrow="Finanças" title="Carregando fluxo de caixa" description="Buscando contas, gastos e categorias no Open Finance (Pierre)." />
  }
  if (error) {
    const notCfg = /configurad/i.test(error)
    return (
      <StatePanel
        eyebrow="Finanças"
        title={notCfg ? 'Pierre ainda não conectado' : 'Não foi possível carregar'}
        description={notCfg ? 'Conecte suas contas no Pierre e configure a PIERRE_API_KEY.' : error}
        actionLabel="Tentar novamente"
        onAction={refetch}
      />
    )
  }

  const spend = data?.spend || {}
  const accounts = data?.accounts || {}
  const buckets = data?.buckets || {}
  const topCat = spend.top_category
  const categories = (data?.categories || []).filter((c) => !HIDDEN_CATS.has(c.category)).slice(0, 6)
  const maxCat = categories.reduce((m, c) => Math.max(m, c.total), 0) || 1

  return (
    <section className="fin2">
      <nav className="fin2-tabs">
        {TABS.map((t) => (
          <button key={t} type="button" className={`fin2-tab${tab === t ? ' active' : ''}`} onClick={() => setTab(t)}>
            {t}
          </button>
        ))}
        <div className="fin2-monthnav">
          <button type="button" onClick={() => shiftMonth(-1)} aria-label="Mês anterior">‹</button>
          <span>{MONTHS[ref.month - 1]} {ref.year}</span>
          <button type="button" onClick={() => shiftMonth(1)} aria-label="Próximo mês">›</button>
        </div>
      </nav>

      {tab !== 'Visão geral' ? (
        <div className="fin2-card fin2-soon"><h3>{tab}</h3><p>Em breve — esta aba ainda está sendo construída.</p></div>
      ) : (
        <div className="fin2-grid">
          {/* Hero */}
          <article className="fin2-card fin2-hero">
            <h2>Pronto para descobrir o que seu extrato esconde?</h2>
            {topCat?.delta_pct != null && topCat.delta_pct > 50 ? (
              <p className="fin2-hero-insight">Suas compras de <strong>{topCat.category}</strong> {topCat.delta_pct >= 0 ? 'subiram' : 'caíram'} {formatPercent(Math.abs(topCat.delta_pct), 0)} este mês.</p>
            ) : (
              <p className="fin2-hero-insight">Visão consolidada do seu mês no Open Finance.</p>
            )}
            <div className="fin2-hero-stats">
              <div><small>Gasto em {MONTHS[ref.month - 1]}</small><strong>{formatCurrencyBRL(spend.month_total)}</strong></div>
              <div><small>vs mês anterior</small><strong className={spend.delta_pct >= 0 ? 'fin2-neg' : 'fin2-pos'}>{spend.delta_pct == null ? '—' : formatPercent(spend.delta_pct, 0, { signed: true })}</strong></div>
              <div><small>Maior categoria</small><strong>{topCat?.category || '—'}</strong></div>
            </div>
          </article>

          {/* Ritmo de gastos */}
          <article className="fin2-card fin2-rhythm">
            <header><span className="fin2-kicker">Ritmo de gastos</span></header>
            <div className="fin2-bignum">{formatCurrencyBRL(spend.month_total)}</div>
            <small className="fin2-muted">Média diária {formatCurrencyBRL(spend.daily_avg)}</small>
            {rhythm ? (
              <Line data={rhythm} options={{
                responsive: true,
                plugins: { legend: { position: 'bottom', labels: { boxWidth: 12, color: '#aeb6c2' } } },
                scales: {
                  x: { grid: { display: false }, ticks: { color: '#7c8694', maxTicksLimit: 6 } },
                  y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#7c8694', callback: (v) => formatCompactBrl(v) } },
                },
              }} />
            ) : null}
          </article>

          {/* Contas */}
          <article className="fin2-card fin2-accounts">
            <span className="fin2-kicker">Contas correntes</span>
            <div className="fin2-bignum">{formatCurrencyBRL(accounts.total_bank_balance)}</div>
            <small className="fin2-muted">saldo total</small>
            <ul className="fin2-acc-list">
              {(accounts.bank || []).map((b, i) => (
                <li key={i}>
                  {b.logo ? <img src={b.logo} alt="" /> : <span className="fin2-acc-dot" />}
                  <div><strong>{b.name}</strong><small>{b.subtype === 'SAVINGS' ? 'Poupança' : 'Conta corrente'}</small></div>
                  <span className="fin2-acc-val">{formatCurrencyBRL(b.balance)}</span>
                </li>
              ))}
            </ul>
          </article>

          {/* Cartões */}
          <article className="fin2-card fin2-limits">
            <span className="fin2-kicker">Cartões</span>
            <div className="fin2-bignum">{formatCurrencyBRL(buckets.cartao)}</div>
            <small className="fin2-muted">gasto no mês</small>
            <ul className="fin2-acc-list">
              {(accounts.credit || []).map((c, i) => (
                <li key={i}>
                  {c.logo ? <img src={c.logo} alt="" /> : <span className="fin2-acc-dot" />}
                  <div>
                    <strong>{c.name}{c.level ? <em className="fin2-card-level">{c.level}</em> : null}</strong>
                    <small>•••• {c.last4}{c.additional_cards?.length ? ` · ${c.additional_cards.length} adicionais` : ''}</small>
                    {c.additional_cards?.length ? (
                      <small className="fin2-addcards">{c.additional_cards.map((n) => `••${n}`).join('  ')}</small>
                    ) : null}
                  </div>
                  <span className="fin2-acc-val">{formatCurrencyBRL(c.spent)}<small className="fin2-muted">gasto</small></span>
                </li>
              ))}
            </ul>
          </article>

          {/* Sobra / buckets */}
          <article className="fin2-card fin2-bucketscard">
            <span className="fin2-kicker">Fluxo do mês (modelo TYI)</span>
            <BucketStrip buckets={buckets} />
          </article>

          {/* Categorias */}
          <article className="fin2-card fin2-cats">
            <span className="fin2-kicker">Principais categorias</span>
            <div className="fin2-cat-head">
              <span>Categoria</span>
              <span className="r">Atual</span>
              <span>vs mês anterior</span>
              <span className="r">Variação</span>
              <span className="r">Anterior</span>
            </div>
            <ul className="fin2-cat-list">
              {categories.map((c) => (
                <li key={c.category}>
                  <span className="fin2-cat-name"><i className="fin2-cat-emoji">{emojiFor(c.category)}</i>{c.category}</span>
                  <span className="fin2-cat-atual">{formatCurrencyBRL(c.total)}</span>
                  <span className="fin2-cat-bar"><span style={{ width: `${(c.total / maxCat) * 100}%` }} /></span>
                  <span className={`fin2-cat-delta ${c.is_new ? 'new' : c.delta_pct >= 0 ? 'up' : 'down'}`}>
                    {c.is_new ? 'novo' : c.delta_pct == null ? '—' : `${c.delta_pct >= 0 ? '↗' : '↘'} ${formatPercent(Math.abs(c.delta_pct), 0)}`}
                  </span>
                  <span className="fin2-cat-prev">{c.is_new ? '--' : formatCurrencyBRL(c.prev_total)}</span>
                </li>
              ))}
            </ul>
          </article>

          {/* Recentes: cartão e conta */}
          <RecentList kicker="Recentes no cartão" items={data?.recent_card} />
          <RecentList kicker="Recentes na conta" items={data?.recent_account} />
        </div>
      )}
    </section>
  )
}

export default FinancasPage
