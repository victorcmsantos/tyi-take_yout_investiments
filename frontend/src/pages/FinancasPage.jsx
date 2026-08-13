import { Fragment, useEffect, useMemo, useState } from 'react'
import { Chart as ChartJS } from 'chart.js/auto'
import { Line } from 'react-chartjs-2'
import StatePanel from '../components/StatePanel'
import FinanceInsightsCard from '../components/FinanceInsightsCard'
import { useApiQuery } from '../hooks/useApiQuery'
import { apiPost, apiDelete, clearApiCache } from '../api'
import { formatCurrencyBRL, formatCompactBrl, formatPercent } from '../formatters'

const cleanMerchant = (desc) => String(desc || '')
  .replace(/\s+\d{1,2}\/\d{1,2}\s*$/, '')
  .replace(/\s+\d{3,}\s*$/, '')
  .trim()

function TxEditor({ tx, monthParam, onSaved }) {
  const cats = useMemo(() => Object.keys(CAT_EMOJI).sort((a, b) => a.localeCompare(b, 'pt-BR')), [])
  const [pattern, setPattern] = useState(() => cleanMerchant(tx.description))
  const [category, setCategory] = useState(tx.category || cats[0])
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState('')

  // Recurring capture: infer bucket from the transaction, and detect a parcela
  // ("DESC 6/12") to default to installment with its total.
  const parc = /(\d{1,2})\s*\/\s*(\d{1,2})/.exec(tx.description || '')
  const recBucket = tx.source === 'cartao' ? 'cartao' : (tx.flow === 'in' ? 'receita' : 'despfixa')
  const [recKind, setRecKind] = useState(parc ? 'installment' : 'fixed')
  const [recBusy, setRecBusy] = useState(false)
  const [recDone, setRecDone] = useState(false)

  const isManual = !!tx.manual_id
  const saveCat = async (cat) => {
    setSaving(true); setErr('')
    try {
      if (isManual) {
        // Manual transaction: edit THIS one. Manual transfers often share a
        // description (e.g. "Victor..."), so a pattern rule would hit all of them.
        await apiPost('/api/pierre/manual-transactions', {
          id: tx.manual_id, date: tx.date, description: tx.description,
          category: cat, amount: Math.abs(Number(tx.amount) || 0), flow: tx.flow || 'out',
        })
      } else {
        await apiPost('/api/pierre/overrides', { pattern, category: cat })
      }
      clearApiCache('/api/pierre')
      onSaved()
    } catch (e) {
      setErr(e?.message || 'Falha ao salvar')
      setSaving(false)
    }
  }
  const save = () => saveCat(category)

  const addRecurring = async () => {
    setRecBusy(true); setErr('')
    try {
      const total = recKind === 'installment' && parc ? Number(parc[2]) : 0
      // Fixed items apply from the month being viewed onwards. Installments keep
      // their real schedule: start so parcela 1 lands on its month (this tx is
      // parcela `parc[1]`, so parcela 1 was `parc[1]-1` months earlier).
      let start = monthParam || ''
      if (recKind === 'installment' && parc && tx.date) {
        const d = new Date(`${tx.date}T00:00:00`)
        d.setMonth(d.getMonth() - (Number(parc[1]) - 1))
        start = d.toISOString().slice(0, 7)
      }
      await apiPost('/api/pierre/recurring-items', {
        description: pattern || tx.description,
        bucket: recBucket, amount: Number(tx.amount) || 0, category: tx.category || '',
        kind: recKind, total_installments: total, start_month: start,
      })
      clearApiCache('/api/pierre')
      setRecDone(true)
    } catch (e) {
      setErr(e?.message || 'Falha ao adicionar')
    } finally { setRecBusy(false) }
  }

  return (
    <div className="fin2-tx-editor" onClick={(e) => e.stopPropagation()}>
      {isManual ? (
        <small className="fin2-muted">✍️ Lançamento manual — a mudança vale <strong>só para esta transação</strong>.</small>
      ) : (
        <label>Quando a descrição contém
          <input value={pattern} onChange={(e) => setPattern(e.target.value)} placeholder="ex: MINI MARKET OF JOY" />
        </label>
      )}
      <label>{isManual ? 'categoria desta transação' : 'categorizar como'}
        <select value={category} onChange={(e) => setCategory(e.target.value)}>
          {[...new Set([category, ...cats])].filter(Boolean).map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
      </label>
      <button type="button" className="fin2-chip active" disabled={saving || (!isManual && !pattern.trim()) || !category} onClick={save}>
        {saving ? 'Salvando...' : (isManual ? 'Salvar' : 'Salvar regra')}
      </button>
      <div className="fin2-tx-editor-rec">
        <span className="fin2-muted">Reclassificar{isManual ? ' (só esta)' : ''}:</span>
        <button type="button" className="fin2-chip" disabled={saving || (!isManual && !pattern.trim())} title="Entre suas próprias contas — não conta na sobra" onClick={() => saveCat('Movimentação interna')}>🔄 Movimentação interna</button>
        <button type="button" className="fin2-chip" disabled={saving || (!isManual && !pattern.trim())} title="Conta como saída (Desp. avulsa)" onClick={() => saveCat('Investimentos')}>📈 Investimento</button>
      </div>
      <small className="fin2-muted">
        {MOVIMENTACAO_CATS.has(tx.category)
          ? '🔄 Movimentação interna (entre suas contas) — não conta na sobra.'
          : 'Movimentação interna (entre suas contas) é neutra; investimento e transferências p/ fora contam como saída.'}
      </small>
      <div className="fin2-tx-editor-rec">
        {recDone ? (
          <small className="fin2-pos">✓ Adicionado aos recorrentes — ajuste na aba Recorrentes se precisar.</small>
        ) : (
          <>
            <span className="fin2-muted">Recorrente · {BUCKET_LABELS[recBucket]}</span>
            <select value={recKind} onChange={(e) => setRecKind(e.target.value)}>
              <option value="fixed">Mensal fixo</option>
              <option value="installment">Parcelado{parc ? ` (${parc[1]}/${parc[2]})` : ''}</option>
            </select>
            <button type="button" className="fin2-chip" disabled={recBusy} onClick={addRecurring}>
              {recBusy ? 'Adicionando...' : '➕ Adicionar aos recorrentes'}
            </button>
          </>
        )}
      </div>
      {err ? <small className="fin2-neg">{err}</small> : null}
    </div>
  )
}

const MONTHS = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
const ymShort = (ym) => {
  const [y, m] = String(ym || '').split('-')
  return (MONTHS[Number(m) - 1] || '').slice(0, 3).toLowerCase() + (y ? `/${y.slice(2)}` : '')
}
const ymPrev = (ym) => {
  const [y, m] = String(ym || '').split('-').map(Number)
  if (!y || !m) return ''
  return m === 1 ? `${y - 1}-12` : `${y}-${String(m - 1).padStart(2, '0')}`
}
const ymIdx = (ym) => { const [y, m] = String(ym || '').split('-').map(Number); return (y && m) ? y * 12 + (m - 1) : null }
// Mirrors recurring.active_in_month: active within [start_month, end_month).
const recActive = (it, ym) => {
  const idx = ymIdx(ym), s = ymIdx(it.start_month), e = ymIdx(it.end_month)
  if (e !== null && idx !== null && idx >= e) return false
  if (it.kind === 'installment') {
    if (s === null || idx === null) return false
    const n = idx - s + 1
    return n >= 1 && n <= Number(it.total_installments || 0)
  }
  if (s !== null && idx !== null && idx < s) return false
  return true
}
const TABS = ['Visão geral', 'Transações', 'Contas', 'Cartões', 'Categorias', 'Recorrentes', 'Manual']
const TODAY_ISO = new Date().toISOString().slice(0, 10)
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
  'Faturamento': '💼', 'Impostos': '🏛️', 'Transferência mesma titularidade': '🔁',
  'Movimentação interna': '🔄', 'Investimentos': '📈', 'Transferências': '↔️', 'Transferência - PIX': '↔️',
}
// Categories neutral for the Sobra: only transfers between the user's OWN
// accounts. Investments/external transfers count as a normal outflow.
const MOVIMENTACAO_CATS = new Set(['Transferência mesma titularidade', 'Movimentação interna'])
const emojiFor = (c) => CAT_EMOJI[c] || '🏷️'

function TxLogo({ t }) {
  return t.logo
    ? <img className="fin2-tx-logo" src={t.logo} alt="" />
    : <span className="fin2-tx-logo fallback">{t.source === 'cartao' ? '💳' : '🏦'}</span>
}

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
              <TxLogo t={t} />
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

function CategoryList({ categories, openCat, setOpenCat }) {
  const maxCat = categories.reduce((m, c) => Math.max(m, c.total), 0) || 1
  return (
    <>
      <div className="fin2-cat-head">
        <span>Categoria</span>
        <span className="r">Atual</span>
        <span>vs mês anterior</span>
        <span className="r">Variação</span>
        <span className="r">Anterior</span>
      </div>
      <ul className="fin2-cat-list">
        {categories.map((c) => {
          const open = openCat === c.category
          const tone = c.is_new || c.delta_pct == null ? 'gray' : c.delta_pct >= 50 ? 'red' : c.delta_pct > 0 ? 'yellow' : 'green'
          return (
            <li key={c.category} className={open ? 'open' : ''}>
              <button type="button" className="fin2-cat-row" onClick={() => setOpenCat(open ? null : c.category)}>
                <span className="fin2-cat-name">
                  <span className={`fin2-cat-dot tone-${tone}`} />
                  <i className="fin2-cat-emoji">{emojiFor(c.category)}</i>{c.category}
                </span>
                <span className="fin2-cat-atual">{formatCurrencyBRL(c.total)}</span>
                <span className="fin2-cat-bar"><span style={{ width: `${(c.total / maxCat) * 100}%` }} /></span>
                <span className={`fin2-cat-delta ${c.is_new ? 'new' : c.delta_pct >= 0 ? 'up' : 'down'}`}>
                  {c.is_new ? 'novo' : c.delta_pct == null ? '—' : `${c.delta_pct >= 0 ? '↗' : '↘'} ${formatPercent(Math.abs(c.delta_pct), 0)}`}
                </span>
                <span className="fin2-cat-prev">{c.is_new ? '--' : formatCurrencyBRL(c.prev_total)}</span>
              </button>
              {open ? (
                <div className="fin2-cat-detail">
                  {(c.transactions || []).length === 0 ? (
                    <small className="fin2-muted">Sem transações no período.</small>
                  ) : (
                    c.transactions.map((t, i) => (
                      <div key={i} className="fin2-cat-tx">
                        <TxLogo t={t} />
                        <div><strong>{t.description}</strong><small>{t.date} · {t.source === 'cartao' ? 'cartão' : 'conta'}</small></div>
                        <span className="fin2-neg">−{formatCurrencyBRL(t.amount)}</span>
                      </div>
                    ))
                  )}
                </div>
              ) : null}
            </li>
          )
        })}
      </ul>
    </>
  )
}

function VisaoGeral({ data, monthRef, openCat, setOpenCat }) {
  const spend = data.spend || {}
  const accounts = data.accounts || {}
  const buckets = data.buckets || {}
  const cardsOpen = (accounts.credit || []).some((c) => !c.invoice_closed && c.spent > 0)
  const topCat = spend.top_category
  const categories = (data.categories || []).slice(0, 6)

  const rhythm = useMemo(() => {
    const c = spend.cumulative
    if (!c) return null
    return {
      labels: c.days,
      datasets: [
        { label: 'Este mês', data: c.current, borderColor: '#f0563f', backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 2 },
        { label: 'Mês passado', data: c.previous, borderColor: 'rgba(150,160,175,0.6)', borderDash: [5, 5], backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 1.5 },
      ],
    }
  }, [spend])

  return (
    <div className="fin2-grid">
      <article className="fin2-card fin2-hero">
        <h2>Pronto para descobrir o que seu extrato esconde?</h2>
        {topCat?.delta_pct != null && topCat.delta_pct > 50 ? (
          <p className="fin2-hero-insight">Suas compras de <strong>{topCat.category}</strong> {topCat.delta_pct >= 0 ? 'subiram' : 'caíram'} {formatPercent(Math.abs(topCat.delta_pct), 0)} este mês.</p>
        ) : (
          <p className="fin2-hero-insight">Visão consolidada do seu mês no Open Finance.</p>
        )}
        <div className="fin2-hero-stats">
          <div><small>Gasto em {MONTHS[monthRef.month - 1]}</small><strong>{formatCurrencyBRL(spend.month_total)}</strong></div>
          <div><small>vs mês anterior</small><strong className={spend.delta_pct >= 0 ? 'fin2-neg' : 'fin2-pos'}>{spend.delta_pct == null ? '—' : formatPercent(spend.delta_pct, 0, { signed: true })}</strong></div>
          <div><small>Maior categoria</small><strong>{topCat?.category || '—'}</strong></div>
        </div>
      </article>

      <article className="fin2-card fin2-rhythm">
        <span className="fin2-kicker">Ritmo de gastos</span>
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

      <article className="fin2-card fin2-limits">
        <span className="fin2-kicker">Cartões{cardsOpen ? <em className="fin2-badge-partial">PARCIAL</em> : null}</span>
        <div className="fin2-bignum">{formatCurrencyBRL(buckets.cartao)}</div>
        <small className="fin2-muted">{cardsOpen ? '⚠ fatura em aberto · parcial (incompleto)' : 'gasto no mês'}</small>
        <ul className="fin2-acc-list">
          {(accounts.credit || []).map((c, i) => (
            <li key={i}>
              {c.logo ? <img src={c.logo} alt="" /> : <span className="fin2-acc-dot" />}
              <div>
                <strong>{c.name}{c.level ? <em className="fin2-card-level">{c.level}</em> : null}</strong>
                <small>•••• {c.last4}{c.additional_cards?.length ? ` · ${c.additional_cards.length} adicionais` : ''}</small>
              </div>
              <span className="fin2-acc-val">{formatCurrencyBRL(c.spent)}<small className="fin2-muted">{c.invoice_closed ? 'gasto' : 'parcial'}</small></span>
            </li>
          ))}
        </ul>
      </article>

      <article className="fin2-card fin2-bucketscard fin2-span2">
        <span className="fin2-kicker">Fluxo do mês — realizado × projetado (fim do mês)</span>
        <FluxoCard realized={buckets} planned={data.planned} bucketItems={data.bucket_items} />
      </article>

      <article className="fin2-card fin2-cats">
        <span className="fin2-kicker">Principais categorias</span>
        <CategoryList categories={categories} openCat={openCat} setOpenCat={setOpenCat} />
      </article>

      <RecentList kicker="Recentes no cartão" items={data.recent_card} />
      <RecentList kicker="Recentes na conta" items={data.recent_account} />
    </div>
  )
}

function CategoriasTab({ data, openCat, setOpenCat }) {
  return (
    <article className="fin2-card fin2-cats">
      <span className="fin2-kicker">Todas as categorias</span>
      <CategoryList categories={data.categories || []} openCat={openCat} setOpenCat={setOpenCat} />
    </article>
  )
}

function CartoesTab({ data, onRefetch }) {
  const credit = data.accounts?.credit || []
  const [open, setOpen] = useState(() => new Set())
  // final do cartão selecionado por conta: filtra a lista de compras
  const [cardFilter, setCardFilter] = useState({})
  const toggle = (i) => setOpen((s) => {
    const n = new Set(s)
    if (n.has(i)) n.delete(i); else n.add(i)
    return n
  })
  const { data: cfg, refetch: refetchCfg } = useApiQuery('/api/pierre/settings')
  const [day, setDay] = useState('')
  useEffect(() => { if (cfg?.card_closing_day) setDay(String(cfg.card_closing_day)) }, [cfg])
  const saveDay = async () => {
    await apiPost('/api/pierre/settings', { card_closing_day: Number(day) || 1 })
    clearApiCache('/api/pierre'); refetchCfg(); if (onRefetch) onRefetch()
  }
  const ym = data.month  // month being viewed (YYYY-MM)
  // Per-card DEFAULT closing day (by last4), applies to every month. Merge this
  // card into the saved map; out-of-range clears it (falls back to bank/global).
  const saveCardDefault = async (last4, value) => {
    if (!last4) return
    const map = { ...(cfg?.card_closing_by_last4 || {}) }
    const n = Number(value)
    if (n >= 1 && n <= 28) map[last4] = n
    else delete map[last4]
    await apiPost('/api/pierre/settings', { card_closing_by_last4: map })
    clearApiCache('/api/pierre'); refetchCfg(); if (onRefetch) onRefetch()
  }
  // Per-MONTH override (the cut shifts month to month). Keyed "last4|YYYY-MM";
  // empty/out-of-range removes it so the card uses its default that month.
  // Apelido do plástico (final -> "Victor"/"Eliane"), usado na quebra por cartão.
  const saveCardLabel = async (last4, value) => {
    if (!last4) return
    const map = { ...(cfg?.card_labels_by_last4 || {}) }
    const name = String(value || '').trim().slice(0, 24)
    if (name) map[last4] = name
    else delete map[last4]
    await apiPost('/api/pierre/settings', { card_labels_by_last4: map })
    clearApiCache('/api/pierre'); refetchCfg(); if (onRefetch) onRefetch()
  }
  const saveCardMonth = async (last4, value) => {
    if (!last4 || !ym) return
    const map = { ...(cfg?.card_closing_overrides || {}) }
    const key = `${last4}|${ym}`
    const n = Number(value)
    if (value !== '' && n >= 1 && n <= 28) map[key] = n
    else delete map[key]
    await apiPost('/api/pierre/settings', { card_closing_overrides: map })
    clearApiCache('/api/pierre'); refetchCfg(); if (onRefetch) onRefetch()
  }
  return (
    <>
    <div className="fin2-card fin2-cards-toolbar">
      <span className="fin2-kicker" style={{ margin: 0 }}>Fatura</span>
      <span className="fin2-muted">Total de cada cartão = fatura real do banco (exato). Lançada no mês das compras (paga dia 01 do mês seguinte, como você orça). Fechamento por cartão: dia padrão (todos os meses) + ajuste só do mês quando o corte muda; a lista soma o total com uma linha de “Ajuste de fechamento”.</span>
      <span className="fin2-muted">·  padrão outros bancos: dia</span>
      <input type="number" min="1" max="28" value={day} onChange={(e) => setDay(e.target.value)} className="fin2-day-input" />
      <button type="button" className="fin2-chip active" onClick={saveDay}>Aplicar</button>
    </div>
    <div className="fin2-grid">
      {credit.map((c, i) => {
        const isOpen = open.has(i)
        return (
          <article key={i} className={`fin2-card fin2-cardbig${isOpen ? ' open' : ''}`}>
            <button type="button" className="fin2-cardbig-btn" onClick={() => toggle(i)}>
              <div className="fin2-cardbig-head">
                {c.logo ? <img src={c.logo} alt="" /> : <span className="fin2-acc-dot" />}
                <div>
                  <strong>{c.name}{c.level ? <em className="fin2-card-level">{c.level}</em> : null}{!c.invoice_closed && c.spent > 0 ? <em className="fin2-badge-partial">PARCIAL</em> : null}</strong>
                  <small>{c.brand} · •••• {c.last4}</small>
                </div>
                <span className="fin2-cardbig-chevron">{isOpen ? '⌄' : '›'}</span>
              </div>
              <div className={`fin2-bignum${!c.invoice_closed && c.spent > 0 ? ' fin2-bignum-partial' : ''}`}>{formatCurrencyBRL(c.spent)}</div>
              <small className="fin2-muted">
                {c.invoice_closed ? 'fatura fechada' : '⚠ parcial · fatura ainda em aberto (o banco ainda não fechou o ciclo)'}
                {c.due_date ? ` · vence ${c.due_date.split('-').reverse().join('/')}` : ''}
                {' · '}{c.transactions?.length || 0} compras · toque para ver
              </small>
            </button>
            <div className="fin2-cardbig-closing">
              <small className="fin2-muted">Fecha dia</small>
              <input
                key={`cdef-${c.last4}-${c.closing_day_default}`}
                type="number" min="1" max="28" defaultValue={c.closing_day_default}
                className="fin2-day-input" title="Dia padrão (todos os meses)"
                onClick={(e) => e.stopPropagation()}
                onBlur={(e) => { if (String(e.target.value) !== String(c.closing_day_default ?? '')) saveCardDefault(c.last4, e.target.value) }}
                onKeyDown={(e) => { if (e.key === 'Enter') e.target.blur() }}
              />
              <small className="fin2-muted">· só em {ymShort(ym)}:</small>
              <input
                key={`cmon-${c.last4}-${ym}-${c.closing_day_month ?? ''}`}
                type="number" min="1" max="28"
                defaultValue={c.closing_day_month ?? ''}
                placeholder={c.closing_day_default}
                className={`fin2-day-input${c.closing_day_month ? ' fin2-day-override' : ''}`}
                title={`Ajuste só de ${ymShort(ym)} (vazio = usa o padrão ${c.closing_day_default})`}
                onClick={(e) => e.stopPropagation()}
                onBlur={(e) => { if (String(e.target.value) !== String(c.closing_day_month ?? '')) saveCardMonth(c.last4, e.target.value) }}
                onKeyDown={(e) => { if (e.key === 'Enter') e.target.blur() }}
              />
              {c.closing_day_month ? (
                <button type="button" className="fin2-icon-btn" title="Remover ajuste do mês"
                  onClick={(e) => { e.stopPropagation(); saveCardMonth(c.last4, '') }}>✕</button>
              ) : null}
            </div>
            {(c.cards_breakdown || []).length > 1 ? (
              <div className="fin2-cardbig-add">
                <small className="fin2-muted">Gasto por cartão · toque para filtrar a fatura</small>
                <div className="fin2-cardsplit">
                  {c.cards_breakdown.map((b) => {
                    const selected = cardFilter[c.last4] === b.last4
                    return (
                      <button
                        key={b.last4 || 'none'}
                        type="button"
                        className={`fin2-cardsplit-item${selected ? ' active' : ''}`}
                        onClick={(e) => {
                          e.stopPropagation()
                          setCardFilter((s) => ({ ...s, [c.last4]: selected ? undefined : b.last4 }))
                          if (!isOpen) toggle(i)
                        }}
                      >
                        <strong>{formatCurrencyBRL(b.amount)}</strong>
                        <small>
                          ••{b.last4 || '????'}{b.is_account_card ? ' · titular' : ''}
                          {b.pct != null ? ` · ${b.pct}%` : ''} · {b.count} compras
                        </small>
                        <input
                          className="fin2-cardsplit-alias"
                          defaultValue={b.label || ''}
                          placeholder="apelido"
                          onClick={(e) => e.stopPropagation()}
                          onBlur={(e) => { if (e.target.value !== (b.label || '')) saveCardLabel(b.last4, e.target.value) }}
                          onKeyDown={(e) => { if (e.key === 'Enter') e.target.blur() }}
                        />
                      </button>
                    )
                  })}
                </div>
              </div>
            ) : c.additional_cards?.length ? (
              <div className="fin2-cardbig-add">
                <small className="fin2-muted">{c.additional_cards.length} cartões adicionais</small>
                <div>{c.additional_cards.map((n) => <span key={n} className="fin2-addchip">••{n}</span>)}</div>
              </div>
            ) : null}
            {isOpen ? (
              <ul className="fin2-tx-list fin2-cardbig-txs">
                {(c.transactions || []).filter((t) => !cardFilter[c.last4] || t.card_last4 === cardFilter[c.last4]).length === 0 ? (
                  <li className="fin2-tx-empty"><small className="fin2-muted">Sem compras no período.</small></li>
                ) : c.transactions.filter((t) => !cardFilter[c.last4] || t.card_last4 === cardFilter[c.last4]).map((t, j) => (
                  <li key={j} className={t.adjustment ? 'fin2-tx-adjust' : ''}>
                    {t.adjustment ? <span className="fin2-acc-dot">⚙</span> : <TxLogo t={t} />}
                    <div>
                      <strong>{t.description}</strong>
                      <small>
                        {t.date ? `${t.date} · ` : ''}{t.category}
                        {t.card_last4 ? ` · ••${t.card_last4}` : ''}
                      </small>
                    </div>
                    <span className={t.amount < 0 ? 'fin2-pos' : 'fin2-neg'}>{t.amount < 0 ? '+' : '−'}{formatCurrencyBRL(Math.abs(t.amount))}</span>
                  </li>
                ))}
              </ul>
            ) : null}
          </article>
        )
      })}
    </div>
    </>
  )
}

function ContasTab({ monthParam }) {
  const { data, loading, error, refetch } = useApiQuery('/api/pierre/accounts-activity', {
    params: { month: monthParam },
    cached: true,
    cacheOptions: { ttlMs: 60000, staleWhileRevalidate: true },
  })
  const [open, setOpen] = useState(() => new Set())
  const toggle = (name) => setOpen((s) => {
    const n = new Set(s)
    if (n.has(name)) n.delete(name); else n.add(name)
    return n
  })

  if (loading && !data) return <StatePanel busy eyebrow="Finanças" title="Carregando contas" description="Buscando a movimentação de cada conta." />
  if (error) return <StatePanel eyebrow="Finanças" title="Não foi possível carregar" description={error} actionLabel="Tentar novamente" onAction={refetch} />

  const accounts = data?.accounts || []
  return (
    <div className="fin2-grid">
      {accounts.length === 0 ? (
        <article className="fin2-card"><small className="fin2-muted">Nenhuma conta com movimentação no mês.</small></article>
      ) : accounts.map((a) => {
        const isOpen = open.has(a.name)
        return (
          <article key={a.name} className={`fin2-card fin2-cardbig${isOpen ? ' open' : ''}`}>
            <button type="button" className="fin2-cardbig-btn" onClick={() => toggle(a.name)}>
              <div className="fin2-cardbig-head">
                {a.logo ? <img src={a.logo} alt="" /> : <span className="fin2-acc-dot" />}
                <div>
                  <strong>{a.name}{a.manual ? <em className="fin2-card-level">MANUAL</em> : null}</strong>
                  <small>{a.number ? `conta ${a.number}` : 'conta'}</small>
                </div>
                <span className="fin2-cardbig-chevron">{isOpen ? '⌄' : '›'}</span>
              </div>
              {a.balance != null ? <div className="fin2-bignum">{formatCurrencyBRL(a.balance)}</div> : null}
              <small className="fin2-muted">
                <span className="fin2-pos">+{formatCurrencyBRL(a.total_in)}</span>
                {' · '}
                <span className="fin2-neg">−{formatCurrencyBRL(a.total_out)}</span>
                {' · '}{a.transactions?.length || 0} movimentações · toque para ver
              </small>
            </button>
            {isOpen ? (
              <ul className="fin2-tx-list fin2-cardbig-txs">
                {(a.transactions || []).length === 0 ? (
                  <li className="fin2-tx-empty"><small className="fin2-muted">Sem movimentação no mês.</small></li>
                ) : a.transactions.map((t, j) => (
                  <li key={j}>
                    <div><strong>{t.description}</strong><small>{t.date} · {t.category}</small></div>
                    <span className={t.flow === 'in' ? 'fin2-pos' : 'fin2-neg'}>{t.flow === 'in' ? '+' : '−'}{formatCurrencyBRL(t.amount)}</span>
                  </li>
                ))}
              </ul>
            ) : null}
          </article>
        )
      })}
    </div>
  )
}

const LEDGER_FILTERS = [['todos', 'Todos'], ['cartao', 'Cartão'], ['conta', 'Conta']]

function TransacoesTab({ monthParam }) {
  const { data, loading, error, refetch } = useApiQuery('/api/pierre/ledger', {
    params: { month: monthParam },
    cached: true,
    cacheOptions: { ttlMs: 60000, staleWhileRevalidate: true },
  })
  const [filter, setFilter] = useState('todos')
  const [search, setSearch] = useState('')
  const [openTx, setOpenTx] = useState(null)

  const rows = useMemo(() => {
    const all = data?.transactions || []
    const q = search.trim().toLowerCase()
    return all.filter((t) => {
      if (filter !== 'todos' && t.source !== filter) return false
      if (q && !(`${t.description} ${t.category}`.toLowerCase().includes(q))) return false
      return true
    })
  }, [data, filter, search])

  if (loading && !data) return <StatePanel busy eyebrow="Finanças" title="Carregando transações" description="Buscando o extrato do mês." />
  if (error) return <StatePanel eyebrow="Finanças" title="Não foi possível carregar" description={error} />

  return (
    <article className="fin2-card">
      <div className="fin2-ledger-controls">
        <div className="fin2-ledger-filters">
          {LEDGER_FILTERS.map(([v, l]) => (
            <button key={v} type="button" className={`fin2-chip${filter === v ? ' active' : ''}`} onClick={() => setFilter(v)}>{l}</button>
          ))}
        </div>
        <input className="fin2-search" placeholder="Buscar descrição ou categoria..." value={search} onChange={(e) => setSearch(e.target.value)} />
      </div>
      <ul className="fin2-tx-list fin2-ledger">
        {rows.length === 0 ? (
          <li className="fin2-tx-empty"><small className="fin2-muted">Nenhuma transação encontrada.</small></li>
        ) : (
          rows.map((t, i) => {
            const key = `${t.date}|${t.description}|${t.amount}|${i}`
            const open = openTx === key
            return (
              <li key={key} className={open ? 'open' : ''}>
                <button type="button" className="fin2-ledger-row" onClick={() => setOpenTx(open ? null : key)}>
                  <TxLogo t={t} />
                  <div>
                    <strong>{t.description}</strong>
                    <small>{t.date} · {t.category} · {t.source === 'cartao' ? 'cartão' : 'conta'}</small>
                  </div>
                  <span className={t.flow === 'in' ? 'fin2-pos' : 'fin2-neg'}>{t.flow === 'in' ? '' : '−'}{formatCurrencyBRL(t.amount)}</span>
                </button>
                {open ? <TxEditor tx={t} monthParam={monthParam} onSaved={() => { setOpenTx(null); refetch() }} /> : null}
              </li>
            )
          })
        )}
      </ul>
      <small className="fin2-muted">{rows.length} transações · toque numa transação para recategorizar</small>
    </article>
  )
}

const CAT_LIST = Object.keys(CAT_EMOJI).sort((a, b) => a.localeCompare(b, 'pt-BR'))
const PLUGGY_ICON = (id) => `https://cdn.pluggy.ai/assets/connector-icons/${id}.svg`
const LOGO_PRESETS = [
  { label: 'Santander', url: PLUGGY_ICON(208) },
  { label: 'Inter', url: PLUGGY_ICON(215) },
  { label: 'Itaú', url: PLUGGY_ICON(201) },
  { label: 'Nubank', url: PLUGGY_ICON(212) },
]

function ManualAccountCard({ account, onChange }) {
  const { data: txs, refetch } = useApiQuery('/api/pierre/manual-transactions', {
    params: { account_id: account.id },
  })
  const BLANK = { date: TODAY_ISO, description: '', category: 'Supermercado', amount: '', flow: 'out' }
  const [form, setForm] = useState(BLANK)
  const [editingId, setEditingId] = useState(null)
  const [saving, setSaving] = useState(false)
  const set = (k, v) => setForm((f) => ({ ...f, [k]: v }))
  const reset = () => { setForm(BLANK); setEditingId(null) }

  const save = async () => {
    if (!form.amount) return
    setSaving(true)
    try {
      const body = editingId
        ? { id: editingId, ...form, amount: Number(form.amount) }
        : { account_id: account.id, ...form, amount: Number(form.amount) }
      await apiPost('/api/pierre/manual-transactions', body)
      reset()
      clearApiCache('/api/pierre'); refetch(); onChange()
    } finally { setSaving(false) }
  }
  const startEdit = (t) => {
    setForm({ date: t.date, description: t.description, category: t.category, amount: String(t.amount), flow: t.flow })
    setEditingId(t.id)
  }
  const delTx = async (id) => { await apiDelete('/api/pierre/manual-transactions', {}, { id }); clearApiCache('/api/pierre'); refetch(); onChange() }
  const delAccount = async () => {
    if (!window.confirm(`Excluir a conta "${account.name}" e seus lançamentos?`)) return
    await apiDelete('/api/pierre/manual-accounts', {}, { id: account.id }); clearApiCache('/api/pierre'); onChange()
  }

  return (
    <article className="fin2-card fin2-manual-acc">
      <div className="fin2-manual-acc-head">
        {account.logo ? <img className="fin2-manual-logo" src={account.logo} alt="" /> : <span className="fin2-acc-dot" />}
        <div>
          <strong>{account.name}</strong>
          <small className="fin2-muted">conta manual</small>
        </div>
        <span className={`fin2-acc-val ${account.balance >= 0 ? 'fin2-pos' : 'fin2-neg'}`}>{formatCurrencyBRL(account.balance)}</span>
        <button type="button" className="fin2-icon-btn" title="Excluir conta" onClick={delAccount}>✕</button>
      </div>

      <div className="fin2-manual-form">
        <input type="date" value={form.date} onChange={(e) => set('date', e.target.value)} />
        <input placeholder="Descrição" value={form.description} onChange={(e) => set('description', e.target.value)} />
        <select value={form.category} onChange={(e) => set('category', e.target.value)}>
          {[...new Set([form.category, ...CAT_LIST])].filter(Boolean).map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <select value={form.flow} onChange={(e) => set('flow', e.target.value)}>
          <option value="out">Saída</option>
          <option value="in">Entrada</option>
        </select>
        <input type="number" step="0.01" placeholder="Valor" value={form.amount} onChange={(e) => set('amount', e.target.value)} />
        <button type="button" className="fin2-chip active" disabled={saving || !form.amount} onClick={save}>{saving ? '...' : editingId ? 'Salvar' : 'Lançar'}</button>
        {editingId ? <button type="button" className="fin2-chip" onClick={reset}>Cancelar</button> : null}
      </div>

      <ul className="fin2-tx-list fin2-manual-txs">
        {(txs || []).length === 0 ? (
          <li className="fin2-tx-empty"><small className="fin2-muted">Nenhum lançamento ainda.</small></li>
        ) : txs.map((t) => (
          <li key={t.id} className={editingId === t.id ? 'editing' : ''}>
            <div><strong>{t.description}</strong><small>{t.date} · {t.category}</small></div>
            <span className={t.flow === 'in' ? 'fin2-pos' : 'fin2-neg'}>{t.flow === 'in' ? '' : '−'}{formatCurrencyBRL(t.amount)}</span>
            <button type="button" className="fin2-icon-btn" title="Editar" onClick={() => startEdit(t)}>✎</button>
            <button type="button" className="fin2-icon-btn" title="Excluir" onClick={() => delTx(t.id)}>✕</button>
          </li>
        ))}
      </ul>
    </article>
  )
}

function ManualTab() {
  const { data: accounts, loading, error, refetch } = useApiQuery('/api/pierre/manual-accounts')
  const [name, setName] = useState('')
  const [logo, setLogo] = useState('')
  const create = async () => {
    if (!name.trim()) return
    await apiPost('/api/pierre/manual-accounts', { name, logo })
    setName(''); setLogo(''); clearApiCache('/api/pierre'); refetch()
  }
  if (loading && !accounts) return <StatePanel busy eyebrow="Finanças" title="Carregando contas manuais" />
  if (error) return <StatePanel eyebrow="Finanças" title="Não foi possível carregar" description={error} />

  return (
    <div className="fin2-grid">
      <article className="fin2-card fin2-span2">
        <span className="fin2-kicker">Nova conta manual</span>
        <div className="fin2-manual-form">
          <input placeholder="Nome (ex: VCMS, ECS, Inter PJ)" value={name} onChange={(e) => setName(e.target.value)} />
          <div className="fin2-logo-picker">
            {LOGO_PRESETS.map((p) => (
              <button key={p.url} type="button" title={p.label}
                className={`fin2-logo-opt${logo === p.url ? ' active' : ''}`}
                onClick={() => setLogo(logo === p.url ? '' : p.url)}>
                <img src={p.url} alt={p.label} />
              </button>
            ))}
          </div>
          <button type="button" className="fin2-chip active" disabled={!name.trim()} onClick={create}>Criar conta</button>
        </div>
      </article>
      {(accounts || []).map((acc) => (
        <ManualAccountCard key={acc.id} account={acc} onChange={refetch} />
      ))}
    </div>
  )
}

const BUCKET_LABELS = { receita: 'Receita', despfixa: 'Desp. fixa', cartao: 'Cartão', despavulsa: 'Desp. avulsa' }

function FluxoCard({ realized, planned, bucketItems }) {
  const rows = ['receita', 'despfixa', 'cartao', 'despavulsa']
  const items = bucketItems || {}
  const [showPend, setShowPend] = useState(false)
  const [openB, setOpenB] = useState(null)
  // Receita amounts are inflows (+); everything else is an outflow (−), except a
  // negative value (e.g. the card closing adjustment) which is a credit (+).
  const sign = (k, amt) => (k === 'receita' ? '+' : (amt < 0 ? '+' : '−'))
  // Projetado (fim do mês) = Realizado + recorrentes que ainda NÃO caíram. The
  // `posted` flag keeps already-cleared items out, so there's no double count.
  const pendItems = (planned?.items || []).filter((i) => !i.posted)
  const pend = {}
  for (const it of pendItems) pend[it.bucket] = (pend[it.bucket] || 0) + Number(it.amount || 0)
  const proj = {}
  for (const k of rows) proj[k] = (realized[k] || 0) + (pend[k] || 0)
  const projSobra = (proj.receita || 0) - (proj.despfixa || 0) - (proj.cartao || 0) - (proj.despavulsa || 0)
  const pendIn = pendItems.filter((i) => i.bucket === 'receita')
  const pendOut = pendItems.filter((i) => i.bucket !== 'receita')
  const sum = (arr) => arr.reduce((s, i) => s + Number(i.amount || 0), 0)
  return (
    <div className="fin2-fluxo">
      <div className="fin2-fluxo-row fin2-fluxo-head">
        <span>Categoria</span><span className="r">Realizado</span><span className="r">Projetado (fim)</span>
      </div>
      {rows.map((k) => {
        const list = items[k] || []
        const open = openB === k
        return (
          <Fragment key={k}>
            <div className="fin2-fluxo-row fin2-fluxo-rowbtn" onClick={() => setOpenB(open ? null : k)}>
              <span>{BUCKET_LABELS[k]}{list.length ? <em className="fin2-fluxo-cnt">{open ? '⌄' : '›'} {list.length}</em> : null}</span>
              <span className={`r ${k === 'receita' ? 'fin2-pos' : 'fin2-neg'}`}>{formatCurrencyBRL(realized[k])}</span>
              <span className={`r ${pend[k] ? (k === 'receita' ? 'fin2-pos' : 'fin2-neg') : 'fin2-muted'}`}>{formatCurrencyBRL(proj[k])}</span>
            </div>
            {open ? (
              <ul className="fin2-fluxo-pend fin2-fluxo-buckitems">
                {list.length === 0 ? (
                  <li><span className="fin2-muted">Sem transações neste mês.</span></li>
                ) : list.map((t, idx) => (
                  <li key={idx} className={t.adjustment ? 'fin2-tx-adjust' : ''}>
                    <span><em className="fin2-muted">{t.date}</em> {t.description}</span>
                    <span className={sign(k, t.amount) === '+' ? 'fin2-pos' : 'fin2-neg'}>{sign(k, t.amount)}{formatCurrencyBRL(Math.abs(t.amount || 0))}</span>
                  </li>
                ))}
              </ul>
            ) : null}
          </Fragment>
        )
      })}
      <div className="fin2-fluxo-row fin2-fluxo-sobra">
        <span>Sobra</span>
        <span className={`r ${(realized.sobra || 0) >= 0 ? 'fin2-pos' : 'fin2-neg'}`}>{formatCurrencyBRL(realized.sobra)}</span>
        <span className={`r ${projSobra >= 0 ? 'fin2-pos' : 'fin2-neg'}`}><strong>{formatCurrencyBRL(projSobra)}</strong></span>
      </div>
      {pendItems.length > 0 ? (
        <div className="fin2-fluxo-foot">
          <button type="button" className="fin2-fluxo-pendbtn" onClick={() => setShowPend((s) => !s)}>
            <span>Falta cair: {pendIn.length ? <strong className="fin2-pos">+{formatCurrencyBRL(sum(pendIn))} a receber</strong> : null}{pendIn.length && pendOut.length ? ' · ' : ''}{pendOut.length ? <strong className="fin2-neg">−{formatCurrencyBRL(sum(pendOut))} a pagar</strong> : null}</span>
            <span className="fin2-muted">{showPend ? 'ocultar ⌄' : `ver ${pendItems.length} itens ›`}</span>
          </button>
          {showPend ? (
            <ul className="fin2-fluxo-pend">
              {[...pendOut, ...pendIn].map((i, idx) => (
                <li key={idx}>
                  <span><em className="fin2-muted">{BUCKET_LABELS[i.bucket]}</em> {i.description}{i.installment_label ? ` (${i.installment_label})` : ''}</span>
                  <span className={i.bucket === 'receita' ? 'fin2-pos' : 'fin2-neg'}>{i.bucket === 'receita' ? '+' : '−'}{formatCurrencyBRL(i.amount)}</span>
                </li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : (
        <div className="fin2-fluxo-foot fin2-muted">Tudo que estava planejado já caiu — projetado = realizado.</div>
      )}
    </div>
  )
}

const RECUR_BLANK = { description: '', bucket: 'despfixa', amount: '', kind: 'fixed', total_installments: '', start_month: '' }

function RecorrentesTab({ monthParam }) {
  const { data: items, loading, error, refetch } = useApiQuery('/api/pierre/recurring-items')
  const [form, setForm] = useState(RECUR_BLANK)
  const [editingId, setEditingId] = useState(null)
  const set = (k, v) => setForm((f) => ({ ...f, [k]: v }))
  const reset = () => { setForm(RECUR_BLANK); setEditingId(null) }
  const save = async () => {
    if (!form.description.trim() || !form.amount) return
    // New fixed items apply from the month being viewed onwards; installments
    // keep their explicit start; edits keep whatever start they already had.
    const start_month = (!editingId && form.kind === 'fixed' && !form.start_month) ? (monthParam || '') : form.start_month
    const body = {
      ...(editingId ? { id: editingId } : {}),
      ...form, start_month, amount: Number(form.amount), total_installments: Number(form.total_installments) || 0,
    }
    await apiPost('/api/pierre/recurring-items', body); reset(); clearApiCache('/api/pierre'); refetch()
  }
  const startEdit = (it) => {
    setForm({ description: it.description, bucket: it.bucket, amount: String(it.amount), kind: it.kind, total_installments: String(it.total_installments || ''), start_month: it.start_month || '' })
    setEditingId(it.id)
  }
  // Removing a recurring stops it FROM the month being viewed onwards (earlier
  // months keep it). The backend hard-deletes if that leaves no active month.
  const del = async (id) => { await apiDelete('/api/pierre/recurring-items', {}, { id, from_month: monthParam }); clearApiCache('/api/pierre'); refetch() }

  if (loading && !items) return <StatePanel busy eyebrow="Finanças" title="Carregando recorrentes" />
  if (error) return <StatePanel eyebrow="Finanças" title="Não foi possível carregar" description={error} />

  const list = items || []
  const committed = list.filter((i) => i.bucket !== 'receita' && recActive(i, monthParam)).reduce((s, i) => s + Number(i.amount || 0), 0)

  return (
    <div className="fin2-grid">
      <article className="fin2-card fin2-span2">
        <span className="fin2-kicker">{editingId ? 'Editar recorrente' : 'Novo recorrente (fixo, assinatura, parcela)'}</span>
        <div className="fin2-manual-form">
          <input placeholder="Descrição (ex: Claro, Condomínio, IPLACE)" value={form.description} onChange={(e) => set('description', e.target.value)} style={{ flex: 1, minWidth: 180 }} />
          <select value={form.bucket} onChange={(e) => set('bucket', e.target.value)}>
            <option value="despfixa">Desp. fixa</option>
            <option value="cartao">Cartão</option>
            <option value="despavulsa">Desp. avulsa</option>
            <option value="receita">Receita</option>
          </select>
          <select value={form.kind} onChange={(e) => set('kind', e.target.value)}>
            <option value="fixed">Mensal fixo</option>
            <option value="installment">Parcelado</option>
          </select>
          {form.kind === 'installment' ? (
            <>
              <input type="month" value={form.start_month} onChange={(e) => set('start_month', e.target.value)} title="Mês inicial" />
              <input type="number" placeholder="Nº parcelas" value={form.total_installments} onChange={(e) => set('total_installments', e.target.value)} style={{ width: 110 }} />
            </>
          ) : null}
          <input type="number" step="0.01" placeholder="Valor/mês" value={form.amount} onChange={(e) => set('amount', e.target.value)} style={{ width: 120 }} />
          <button type="button" className="fin2-chip active" disabled={!form.description.trim() || !form.amount} onClick={save}>{editingId ? 'Salvar' : 'Adicionar'}</button>
          {editingId ? <button type="button" className="fin2-chip" onClick={reset}>Cancelar</button> : null}
        </div>
        <small className="fin2-muted">Comprometido em {ymShort(monthParam)}: <strong>{formatCurrencyBRL(committed)}</strong> · {list.filter((i) => recActive(i, monthParam)).length} ativos de {list.length}</small>
      </article>

      <article className="fin2-card fin2-span2">
        <span className="fin2-kicker">Recorrentes</span>
        <ul className="fin2-tx-list fin2-recur-list">
          {list.length === 0 ? (
            <li className="fin2-tx-empty"><small className="fin2-muted">Nenhum recorrente. Cadastre seus fixos, assinaturas e parcelas acima.</small></li>
          ) : list.map((it) => (
            <li key={it.id} className={`${editingId === it.id ? 'editing' : ''}${recActive(it, monthParam) ? '' : ' fin2-recur-off'}`}>
              <span className={`financas-tag financas-tag-${it.bucket === 'cartao' ? 'cartao' : it.bucket === 'receita' ? 'receita' : 'fixa'}`}>{BUCKET_LABELS[it.bucket]}</span>
              <div>
                <strong>{it.description}</strong>
                <small>
                  {it.kind === 'installment' ? `parcelado ${it.total_installments}x · início ${ymShort(it.start_month) || '?'}` : 'mensal fixo'}
                  {it.start_month && it.kind !== 'installment' ? ` · desde ${ymShort(it.start_month)}` : ''}
                  {it.end_month ? ` · até ${ymShort(ymPrev(it.end_month))}` : ''}
                </small>
              </div>
              <span className={it.bucket === 'receita' ? 'fin2-pos' : 'fin2-neg'}>{formatCurrencyBRL(it.amount)}</span>
              <button type="button" className="fin2-icon-btn" title="Editar" onClick={() => startEdit(it)}>✎</button>
              <button type="button" className="fin2-icon-btn" title={`Remover a partir de ${ymShort(monthParam)}`} onClick={() => del(it.id)}>✕</button>
            </li>
          ))}
        </ul>
      </article>
    </div>
  )
}

function FinancasPage() {
  const [ref, setRef] = useState(() => {
    const now = new Date()
    return { year: now.getFullYear(), month: now.getMonth() + 1 }
  })
  const [tab, setTab] = useState('Visão geral')
  const [openCat, setOpenCat] = useState(null)
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

  const needsOverview = tab !== 'Transações' && tab !== 'Manual' && tab !== 'Recorrentes' && tab !== 'Contas'

  return (
    <section className="fin2">
      <nav className="fin2-tabs">
        {TABS.map((t) => (
          <button key={t} type="button" className={`fin2-tab${tab === t ? ' active' : ''}`} onClick={() => setTab(t)}>{t}</button>
        ))}
        <div className="fin2-monthnav">
          <button type="button" onClick={() => shiftMonth(-1)} aria-label="Mês anterior">‹</button>
          <span>{MONTHS[ref.month - 1]} {ref.year}</span>
          <button type="button" onClick={() => shiftMonth(1)} aria-label="Próximo mês">›</button>
        </div>
      </nav>

      {needsOverview && loading && !data ? (
        <StatePanel busy eyebrow="Finanças" title="Carregando fluxo de caixa" description="Buscando contas, gastos e categorias no Open Finance." />
      ) : needsOverview && error ? (
        <StatePanel
          eyebrow="Finanças"
          title={/configurad/i.test(error) ? 'Open Finance ainda não conectado' : 'Não foi possível carregar'}
          description={/configurad/i.test(error) ? 'Conecte suas contas na Pluggy e configure PLUGGY_CLIENT_ID/SECRET.' : error}
          actionLabel="Tentar novamente"
          onAction={refetch}
        />
      ) : tab === 'Visão geral' ? (
        <>
          <VisaoGeral data={data} monthRef={ref} openCat={openCat} setOpenCat={setOpenCat} />
          <FinanceInsightsCard monthParam={monthParam} />
        </>
      ) : tab === 'Categorias' ? (
        <CategoriasTab data={data} openCat={openCat} setOpenCat={setOpenCat} />
      ) : tab === 'Cartões' ? (
        <CartoesTab data={data} onRefetch={refetch} />
      ) : tab === 'Contas' ? (
        <ContasTab monthParam={monthParam} />
      ) : tab === 'Manual' ? (
        <ManualTab />
      ) : tab === 'Recorrentes' ? (
        <RecorrentesTab monthParam={monthParam} />
      ) : (
        <TransacoesTab monthParam={monthParam} />
      )}
    </section>
  )
}

export default FinancasPage
