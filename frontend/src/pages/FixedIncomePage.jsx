import { useState } from 'react'
import { apiDelete, apiGet, apiPatch } from '../api'
import StatePanel from '../components/StatePanel'
import { useApiQuery } from '../hooks/useApiQuery'

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`
const FIXED_INVESTMENT_TYPE_OPTIONS = [
  'CDB',
  'LCI',
  'LCA',
  'LCI/LCA',
  'CRI',
  'CRA',
  'DEBENTURE',
  'TESOURO DIRETO',
  'LETRA FINANCEIRA',
  'COE',
]
const FIXED_INVESTMENT_TYPE_SET = new Set(FIXED_INVESTMENT_TYPE_OPTIONS)
const EMPTY_EDIT_FORM = {
  target_portfolio_id: '',
  distributor: '',
  issuer: '',
  investment_type: '',
  rate_type: 'FIXO',
  annual_rate: '',
  juros_fixo: '',
  ipca: '',
  cdi: '',
  date_aporte: '',
  maturity_date: '',
  aporte: '',
  reinvested: '',
}

function dateBr(value) {
  if (!value) return ''
  const text = String(value)
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const [y, m, d] = text.split('-')
    return `${d}/${m}/${y}`
  }
  return text
}

function pct(value) {
  return `${Number(value || 0).toFixed(2)}%`
}

function formatRateType(item) {
  const rateType = String(item.rate_type || '').toUpperCase()
  const fixed = Number(item.rate_fixed || 0)
  const ipca = Number(item.rate_ipca || 0)
  const cdi = Number(item.rate_cdi || 0)
  const annual = Number(item.annual_rate || 0)

  if (rateType === 'FIXO') return `${rateType} (${pct(fixed || annual)})`
  if (rateType === 'IPCA') return `${rateType} (${pct(ipca || annual)})`
  if (rateType === 'CDI') return `${rateType} (${pct(cdi || annual)})`
  if (rateType === 'FIXO+IPCA') return `${rateType} (${pct(fixed)} + ${pct(ipca)})`
  if (rateType === 'FIXO+CDI') return `${rateType} (${pct(fixed)} + ${pct(cdi)})`
  return `${rateType || 'N/A'} (${pct(annual)})`
}

function groupSummary(items) {
  return {
    count: items.length,
    aporte: items.reduce((acc, item) => acc + Number(item.aporte || 0), 0),
    reinvested: items.reduce((acc, item) => acc + Number(item.reinvested || 0), 0),
    applied: items.reduce((acc, item) => acc + Number(item.active_applied_value || 0), 0),
    current: items.reduce((acc, item) => acc + Number(item.current_gross_value || 0), 0),
    final: items.reduce((acc, item) => acc + (item.is_matured ? 0 : Number(item.final_gross_value || 0)), 0),
    income: items.reduce((acc, item) => acc + Number(item.current_income || 0), 0),
    totalReceived: items.reduce((acc, item) => acc + Number(item.total_received || 0), 0),
  }
}

function FixedIncomePage({ selectedPortfolioIds, portfolios = [] }) {
  const [message, setMessage] = useState('')
  const [actionError, setActionError] = useState('')
  const [sortBy, setSortBy] = useState('date_aporte')
  const [sortDir, setSortDir] = useState('desc')
  const [openGroups, setOpenGroups] = useState({ prefixado: true, posfixado: false })
  const [selectedFixedIds, setSelectedFixedIds] = useState([])
  const [removingFixed, setRemovingFixed] = useState(false)
  const [editingFixedId, setEditingFixedId] = useState(null)
  const [savingEdit, setSavingEdit] = useState(false)
  const [editForm, setEditForm] = useState(EMPTY_EDIT_FORM)
  const {
    data: payload,
    setData: setPayload,
    loading,
    refreshing,
    error,
  } = useApiQuery('/api/fixed-incomes', {
    params: {
      portfolio_id: selectedPortfolioIds,
      sort_by: sortBy,
      sort_dir: sortDir,
    },
  })
  const editInvestmentTypeSelectValue = FIXED_INVESTMENT_TYPE_SET.has(editForm.investment_type)
    ? editForm.investment_type
    : (editForm.investment_type ? 'OUTRO' : '')

  const reloadFixedIncomes = async () => {
    const data = await apiGet('/api/fixed-incomes', {
      portfolio_id: selectedPortfolioIds,
      sort_by: sortBy,
      sort_dir: sortDir,
    })
    setPayload(data)
    return data
  }

  const toggleSort = (field) => {
    if (sortBy === field) {
      setSortDir((current) => (current === 'asc' ? 'desc' : 'asc'))
      return
    }
    setSortBy(field)
    setSortDir('desc')
  }

  const sortLabel = (label, field) => {
    if (sortBy !== field) return label
    return `${label} ${sortDir === 'asc' ? '↑' : '↓'}`
  }

  const toggleGroup = (key) => {
    setOpenGroups((current) => ({ ...current, [key]: !current[key] }))
  }

  const toggleFixed = (fixedId) => {
    setSelectedFixedIds((current) => (
      current.includes(fixedId) ? current.filter((id) => id !== fixedId) : [...current, fixedId]
    ))
  }

  const startEditFixed = (item) => {
    setActionError('')
    setMessage('')
    setEditingFixedId(item.id)
    setEditForm({
      target_portfolio_id: String(item.portfolio_id || ''),
      distributor: String(item.distributor || ''),
      issuer: String(item.issuer || ''),
      investment_type: String(item.investment_type || ''),
      rate_type: String(item.rate_type || 'FIXO').toUpperCase(),
      annual_rate: item.annual_rate == null ? '' : String(item.annual_rate),
      juros_fixo: Number(item.rate_fixed || 0) > 0 ? String(item.rate_fixed) : (String(item.rate_type || '').toUpperCase() === 'FIXO' ? String(item.annual_rate || '') : ''),
      ipca: Number(item.rate_ipca || 0) > 0 ? String(item.rate_ipca) : (String(item.rate_type || '').toUpperCase() === 'IPCA' ? String(item.annual_rate || '') : ''),
      cdi: Number(item.rate_cdi || 0) > 0 ? String(item.rate_cdi) : (String(item.rate_type || '').toUpperCase() === 'CDI' ? String(item.annual_rate || '') : ''),
      date_aporte: String(item.date_aporte || ''),
      maturity_date: String(item.maturity_date || ''),
      aporte: item.aporte == null ? '' : String(item.aporte),
      reinvested: Number(item.reinvested || 0) > 0 ? String(item.reinvested) : '',
    })
  }

  const cancelEditFixed = () => {
    setEditingFixedId(null)
    setSavingEdit(false)
    setEditForm(EMPTY_EDIT_FORM)
  }

  const onEditChange = (event) => {
    const { name, value } = event.target
    if (name === 'investment_type') {
      setEditForm((current) => ({
        ...current,
        investment_type: value === 'OUTRO'
          ? (FIXED_INVESTMENT_TYPE_SET.has(current.investment_type) ? '' : current.investment_type)
          : value,
      }))
      return
    }
    setEditForm((current) => ({ ...current, [name]: value }))
  }

  const onSubmitEdit = async (event) => {
    event.preventDefault()
    if (!editingFixedId) return
    setSavingEdit(true)
    setActionError('')
    setMessage('')
    try {
      const result = await apiPatch(`/api/fixed-incomes/${editingFixedId}`, editForm)
      setMessage(result.message || 'Renda fixa atualizada com sucesso.')
      await reloadFixedIncomes()
      cancelEditFixed()
    } catch (err) {
      setActionError(err?.message || 'Falha ao atualizar registro.')
    } finally {
      setSavingEdit(false)
    }
  }

  const onRemoveFixed = async () => {
    if (selectedFixedIds.length === 0) {
      setActionError('Selecione ao menos um registro de renda fixa para remover.')
      return
    }
    setRemovingFixed(true)
    setMessage('')
    setActionError('')
    try {
      const result = await apiDelete(
        '/api/fixed-incomes',
        { fixed_income_ids: selectedFixedIds },
        { portfolio_id: selectedPortfolioIds },
      )
      setMessage(`${Number(result.removed || 0)} registro(s) de renda fixa removido(s).`)
      await reloadFixedIncomes()
      setSelectedFixedIds([])
      if (selectedFixedIds.includes(editingFixedId)) cancelEditFixed()
    } catch (err) {
      setActionError(err?.message || 'Falha ao remover registros.')
    } finally {
      setRemovingFixed(false)
    }
  }

  if (loading && !payload) {
    return (
      <StatePanel
        busy
        eyebrow="Renda fixa"
        title="Montando a carteira de renda fixa"
        description="Buscando emissor, rentabilidade e fluxo de recebimentos."
      />
    )
  }
  if (!payload && error) return <p className="error">{error}</p>
  if (!payload) {
    return (
      <StatePanel
        eyebrow="Renda fixa"
        title="Nenhum titulo encontrado"
        description="Adicione um registro de renda fixa para acompanhar juros, valor atual e vencimento aqui."
      />
    )
  }

  const summary = payload.summary || {}
  const items = payload.items || []
  const editingItem = items.find((item) => Number(item.id) === Number(editingFixedId)) || null
  const prefixadoItems = items.filter((item) => String(item.rate_type || '').toUpperCase() === 'FIXO')
  const posfixadoItems = items.filter((item) => String(item.rate_type || '').toUpperCase() !== 'FIXO')
  const groups = [
    { key: 'prefixado', label: 'Juros Prefixado', items: prefixadoItems },
    { key: 'posfixado', label: 'Juros Pos-fixado', items: posfixadoItems },
  ]

  return (
    <section>
      <h1>Renda Fixa</h1>
      {refreshing && <p className="subtitle">Atualizando ordenacao...</p>}
      {!!error && <p className="notice-warn">{error}</p>}
      {!!actionError && <p className="notice-warn">{actionError}</p>}
      {!!message && <p className="notice-ok">{message}</p>}
      <div className="cards">
        <article className="card"><h3>Total aporte</h3><p>{brl(summary.aporte_total)}</p></article>
        <article className="card"><h3>Total reinvestido</h3><p>{brl(summary.reinvested_total)}</p></article>
        <article className="card"><h3>Total aplicado</h3><p>{brl(summary.applied_total)}</p></article>
        <article className="card"><h3>Valor atual bruto</h3><p>{brl(summary.current_total)}</p></article>
        <article className="card"><h3>Valor final estimado</h3><p>{brl(summary.final_total)}</p></article>
        <article className="card"><h3>Rendimento bruto</h3><p>{brl(summary.income_total)}</p></article>
        <article className="card"><h3>Total recebido</h3><p>{brl(summary.total_received)}</p></article>
      </div>

      <div className="accordion-wrap">
        {groups.map((group) => {
          const sum = groupSummary(group.items)
          const isOpen = !!openGroups[group.key]
          const fixedTotal = Number(summary.current_total || 0)
          const groupWeight = fixedTotal > 0 ? (sum.current / fixedTotal) * 100 : 0
          return (
            <section key={group.key} className="asset-group">
              <button type="button" className="asset-group-summary-btn" onClick={() => toggleGroup(group.key)}>
                <div className="asset-group-summary">
                <div>
                  <strong>{group.label}</strong>
                  <small>{sum.count} registro(s)</small>
                </div>
                <div className="asset-group-metrics">
                  <div className="metric-item">
                    <span className="metric-label">Aporte</span>
                    <strong>{brl(sum.aporte)}</strong>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Reinvestido</span>
                    <strong>{brl(sum.reinvested)}</strong>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Aplicado</span>
                    <strong>{brl(sum.applied)}</strong>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Valor atual bruto</span>
                    <strong>{brl(sum.current)}</strong>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">Rendimento bruto</span>
                    <strong className={sum.income >= 0 ? 'up' : 'down'}>{brl(sum.income)}</strong>
                  </div>
                  <div className="metric-item">
                    <span className="metric-label">% na carteira</span>
                    <strong>{Number(groupWeight || 0).toFixed(2)}%</strong>
                  </div>
                </div>
                <span className={`asset-group-chevron ${isOpen ? 'open' : ''}`}>⌄</span>
                </div>
              </button>

              {isOpen && (
                <>
              <div className="cards">
                <article className="card"><h3>Total aporte</h3><p>{brl(sum.aporte)}</p></article>
                <article className="card"><h3>Total reinvestido</h3><p>{brl(sum.reinvested)}</p></article>
                <article className="card"><h3>Total aplicado</h3><p>{brl(sum.applied)}</p></article>
                <article className="card"><h3>Valor atual bruto</h3><p>{brl(sum.current)}</p></article>
                <article className="card"><h3>Valor final estimado</h3><p>{brl(sum.final)}</p></article>
                <article className="card"><h3>Rendimento bruto</h3><p className={sum.income >= 0 ? 'up' : 'down'}>{brl(sum.income)}</p></article>
                <article className="card"><h3>Total recebido</h3><p>{brl(sum.totalReceived)}</p></article>
              </div>

              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Sel.</th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('portfolio_name')}>{sortLabel('Carteira', 'portfolio_name')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('distributor')}>{sortLabel('Distribuidor', 'distributor')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('issuer')}>{sortLabel('Emissor', 'issuer')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('investment_type')}>{sortLabel('Aplicação', 'investment_type')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('annual_rate')}>{sortLabel('Taxa', 'annual_rate')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('date_aporte')}>{sortLabel('Data aporte', 'date_aporte')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('maturity_date')}>{sortLabel('Data final', 'maturity_date')}</button></th>
                      <th>Aporte</th>
                      <th>Reinvestido</th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('active_applied_value')}>{sortLabel('Aplicado', 'active_applied_value')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('elapsed_days')}>{sortLabel('Dias', 'elapsed_days')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('total_days')}>{sortLabel('Total dias', 'total_days')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('current_gross_value')}>{sortLabel('Atual bruto', 'current_gross_value')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('total_received')}>{sortLabel('Total recebido', 'total_received')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('rendimento')}>{sortLabel('Rendimento Recebido', 'rendimento')}</button></th>
                      <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('final_gross_value')}>{sortLabel('Valor final', 'final_gross_value')}</button></th>
                      <th>Acoes</th>
                    </tr>
                  </thead>
                  <tbody>
                    {group.items.map((item) => (
                      <tr key={`${group.key}-${item.id}`}>
                        <td>
                          <input
                            type="checkbox"
                            checked={selectedFixedIds.includes(item.id)}
                            onChange={() => toggleFixed(item.id)}
                          />
                        </td>
                        <td>{item.portfolio_name}</td>
                        <td>{item.distributor}</td>
                        <td>{item.issuer}</td>
                        <td>{item.investment_type}</td>
                        <td>{formatRateType(item)}</td>
                        <td>{dateBr(item.date_aporte)}</td>
                        <td>{dateBr(item.maturity_date)}</td>
                        <td>{brl(item.aporte)}</td>
                        <td>{brl(item.reinvested)}</td>
                        <td>{brl(item.active_applied_value)}</td>
                        <td>{Number(item.elapsed_days || 0)}</td>
                        <td>{Number(item.total_days || 0)}</td>
                        <td>{brl(item.current_gross_value)}</td>
                        <td>{brl(item.total_received)}</td>
                        <td className={Number(item.rendimento || 0) >= 0 ? 'up' : 'down'}>{brl(item.rendimento)}</td>
                        <td>{brl(item.final_gross_value)}</td>
                        <td>
                          <button type="button" className="btn-secondary" onClick={() => startEditFixed(item)}>
                            {editingFixedId === item.id ? 'Editando...' : 'Editar'}
                          </button>
                        </td>
                      </tr>
                    ))}
                    {group.items.length === 0 && (
                      <tr>
                        <td colSpan={18}>Sem registros nesse grupo.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
                <div className="table-actions">
                  <button type="button" className="btn-danger" disabled={removingFixed} onClick={onRemoveFixed}>
                    {removingFixed ? 'Removendo...' : 'Remover selecionados'}
                  </button>
                </div>
              </div>
                </>
              )}
            </section>
          )
        })}
      </div>

      {editingItem && (
        <div className="health-modal-backdrop" role="presentation" onClick={cancelEditFixed}>
          <div
            className="health-modal"
            role="dialog"
            aria-modal="true"
            aria-label="Editar renda fixa"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="health-modal-header">
              <div>
                <h3>Editar renda fixa</h3>
                <p className="subtitle">{editingItem.portfolio_name} • {editingItem.issuer} • {editingItem.investment_type}</p>
              </div>
              <button type="button" className="btn-secondary" onClick={cancelEditFixed} disabled={savingEdit}>
                Fechar
              </button>
            </div>
            <form onSubmit={onSubmitEdit} className="form-grid">
              <div>
                <label htmlFor={`edit-target_portfolio_id-${editingItem.id}`}>Carteira destino</label>
                <select
                  id={`edit-target_portfolio_id-${editingItem.id}`}
                  name="target_portfolio_id"
                  value={editForm.target_portfolio_id}
                  onChange={onEditChange}
                  required
                >
                  {portfolios.map((item) => (
                    <option key={`edit-fixed-${item.id}`} value={item.id}>{item.name}</option>
                  ))}
                </select>
              </div>
              <div>
                <label htmlFor={`edit-distributor-${editingItem.id}`}>Distribuidor</label>
                <input id={`edit-distributor-${editingItem.id}`} name="distributor" type="text" value={editForm.distributor} onChange={onEditChange} required />
              </div>
              <div>
                <label htmlFor={`edit-issuer-${editingItem.id}`}>Emissor</label>
                <input id={`edit-issuer-${editingItem.id}`} name="issuer" type="text" value={editForm.issuer} onChange={onEditChange} required />
              </div>
              <div>
                <label htmlFor={`edit-investment_type-${editingItem.id}`}>Aplicação</label>
                <select
                  id={`edit-investment_type-${editingItem.id}`}
                  name="investment_type"
                  value={editInvestmentTypeSelectValue}
                  onChange={onEditChange}
                  required
                >
                  <option value="">Selecione</option>
                  {FIXED_INVESTMENT_TYPE_OPTIONS.map((option) => (
                    <option key={`edit-option-${option}`} value={option}>{option}</option>
                  ))}
                  <option value="OUTRO">Outro</option>
                </select>
                {editInvestmentTypeSelectValue === 'OUTRO' ? (
                  <input
                    style={{ marginTop: 8 }}
                    id={`edit-investment_type_custom-${editingItem.id}`}
                    name="investment_type"
                    type="text"
                    placeholder="Ex: RDB, LF, LCD..."
                    value={editForm.investment_type}
                    onChange={onEditChange}
                    required
                  />
                ) : null}
              </div>
              <div>
                <label htmlFor={`edit-rate_type-${editingItem.id}`}>Tipo de taxa</label>
                <select id={`edit-rate_type-${editingItem.id}`} name="rate_type" value={editForm.rate_type} onChange={onEditChange} required>
                  <option value="FIXO">FIXO</option>
                  <option value="FIXO+IPCA">FIXO+IPCA</option>
                  <option value="IPCA">IPCA</option>
                  <option value="CDI">CDI</option>
                  <option value="FIXO+CDI">FIXO+CDI</option>
                </select>
              </div>
              <div>
                <label htmlFor={`edit-juros_fixo-${editingItem.id}`}>Juros Fixo (%)</label>
                <input id={`edit-juros_fixo-${editingItem.id}`} name="juros_fixo" type="text" value={editForm.juros_fixo} onChange={onEditChange} />
              </div>
              <div>
                <label htmlFor={`edit-ipca-${editingItem.id}`}>IPCA (%)</label>
                <input id={`edit-ipca-${editingItem.id}`} name="ipca" type="text" value={editForm.ipca} onChange={onEditChange} />
              </div>
              <div>
                <label htmlFor={`edit-cdi-${editingItem.id}`}>CDI (%)</label>
                <input id={`edit-cdi-${editingItem.id}`} name="cdi" type="text" value={editForm.cdi} onChange={onEditChange} />
              </div>
              <div>
                <label htmlFor={`edit-date_aporte-${editingItem.id}`}>Data aporte</label>
                <input id={`edit-date_aporte-${editingItem.id}`} name="date_aporte" type="date" value={editForm.date_aporte} onChange={onEditChange} required />
              </div>
              <div>
                <label htmlFor={`edit-maturity_date-${editingItem.id}`}>Data final</label>
                <input id={`edit-maturity_date-${editingItem.id}`} name="maturity_date" type="date" value={editForm.maturity_date} onChange={onEditChange} required />
              </div>
              <div>
                <label htmlFor={`edit-aporte-${editingItem.id}`}>Aporte (R$)</label>
                <input id={`edit-aporte-${editingItem.id}`} name="aporte" type="text" value={editForm.aporte} onChange={onEditChange} required />
              </div>
              <div>
                <label htmlFor={`edit-reinvested-${editingItem.id}`}>Reinvestido (R$)</label>
                <input id={`edit-reinvested-${editingItem.id}`} name="reinvested" type="text" value={editForm.reinvested} onChange={onEditChange} />
              </div>
              <div className="form-actions">
                <button type="submit" className="btn-primary" disabled={savingEdit}>
                  {savingEdit ? 'Salvando...' : 'Salvar edicao'}
                </button>
                <button type="button" className="btn-secondary" onClick={cancelEditFixed} disabled={savingEdit}>
                  Cancelar
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </section>
  )
}

export default FixedIncomePage
