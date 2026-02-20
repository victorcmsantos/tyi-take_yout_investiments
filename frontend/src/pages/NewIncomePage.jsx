import { useEffect, useMemo, useState } from 'react'
import { apiDelete, apiGet, apiPost, apiPostForm } from '../api'

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`

function dateBr(value) {
  if (!value) return ''
  const text = String(value)
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const [y, m, d] = text.split('-')
    return `${d}/${m}/${y}`
  }
  return text
}

function NewIncomePage({ selectedPortfolioIds, portfolios }) {
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(true)
  const [selectedIncomeIds, setSelectedIncomeIds] = useState([])
  const [removingIncomes, setRemovingIncomes] = useState(false)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const [importError, setImportError] = useState('')
  const [importMessage, setImportMessage] = useState('')
  const [importWarnings, setImportWarnings] = useState([])
  const [form, setForm] = useState({
    target_portfolio_id: '',
    ticker: '',
    income_type: 'dividendo',
    amount: '',
    date: '',
  })

  const activePortfolioId = useMemo(
    () => selectedPortfolioIds?.[0] || portfolios?.[0]?.id || '',
    [selectedPortfolioIds, portfolios],
  )

  const loadIncomes = async () => {
    setLoading(true)
    setError('')
    try {
      const data = await apiGet('/api/incomes', { portfolio_id: selectedPortfolioIds })
      setRows(data)
      setSelectedIncomeIds([])
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadIncomes()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(selectedPortfolioIds)])

  const toggleIncome = (incomeId) => {
    setSelectedIncomeIds((current) => (
      current.includes(incomeId) ? current.filter((id) => id !== incomeId) : [...current, incomeId]
    ))
  }

  const onRemoveIncomes = async () => {
    if (selectedIncomeIds.length === 0) {
      setError('Selecione ao menos um provento para remover.')
      return
    }
    setRemovingIncomes(true)
    setError('')
    setMessage('')
    try {
      const result = await apiDelete(
        '/api/incomes',
        { income_ids: selectedIncomeIds },
        { portfolio_id: selectedPortfolioIds },
      )
      setMessage(`${Number(result.removed || 0)} provento(s) removido(s).`)
      await loadIncomes()
    } catch (err) {
      setError(err.message)
    } finally {
      setRemovingIncomes(false)
    }
  }

  useEffect(() => {
    setForm((current) => ({ ...current, target_portfolio_id: String(activePortfolioId || '') }))
  }, [activePortfolioId])

  const onChange = (event) => {
    const { name, value } = event.target
    setForm((current) => ({ ...current, [name]: value }))
  }

  const onSubmit = async (event) => {
    event.preventDefault()
    setError('')
    setMessage('')
    try {
      const payload = { ...form, ticker: form.ticker.toUpperCase() }
      const result = await apiPost('/api/incomes', payload)
      setMessage(result.message || 'Provento registrado com sucesso.')
      setForm((current) => ({ ...current, ticker: '', amount: '', date: '' }))
      await loadIncomes()
    } catch (err) {
      setError(err.message)
    }
  }

  const onImportCsv = async (event) => {
    event.preventDefault()
    setImportError('')
    setImportMessage('')
    setImportWarnings([])
    const selectedPortfolioId = event.target.target_portfolio_id?.value || form.target_portfolio_id
    const file = event.target.csv_file?.files?.[0]
    if (!file) {
      setImportError('Selecione um arquivo CSV.')
      return
    }
    try {
      const formData = new FormData()
      formData.append('target_portfolio_id', selectedPortfolioId)
      formData.append('csv_file', file)
      const result = await apiPostForm('/api/imports/transactions-csv', formData)
      setImportMessage(`Importacao finalizada. ${Number(result.imported || 0)} linha(s) importada(s).`)
      setImportWarnings(result.errors || [])
      await loadIncomes()
      event.target.reset()
    } catch (err) {
      setImportError(err.message)
    }
  }

  return (
    <section>
      <h1>Lancar provento</h1>
      <p className="subtitle">Registre dividendos, JCP e aluguel por ativo.</p>

      {!!error && <p className="notice-warn">{error}</p>}
      {!!message && <p className="notice-ok">{message}</p>}

      <article className="card form-card">
        <form onSubmit={onSubmit} className="form-grid">
          <div>
            <label htmlFor="target_portfolio_id">Carteira destino</label>
            <select id="target_portfolio_id" name="target_portfolio_id" value={form.target_portfolio_id} onChange={onChange} required>
              {portfolios.map((item) => (
                <option key={item.id} value={item.id}>{item.name}</option>
              ))}
            </select>
          </div>
          <div>
            <label htmlFor="ticker">Ticker</label>
            <input id="ticker" name="ticker" type="text" value={form.ticker} onChange={onChange} placeholder="Ex: ITUB4" required />
          </div>
          <div>
            <label htmlFor="income_type">Tipo</label>
            <select id="income_type" name="income_type" value={form.income_type} onChange={onChange} required>
              <option value="dividendo">Dividendo</option>
              <option value="jcp">JCP</option>
              <option value="aluguel">Aluguel</option>
            </select>
          </div>
          <div>
            <label htmlFor="amount">Valor recebido (R$)</label>
            <input id="amount" name="amount" type="text" value={form.amount} onChange={onChange} required />
          </div>
          <div>
            <label htmlFor="date">Data</label>
            <input id="date" name="date" type="date" value={form.date} onChange={onChange} required />
          </div>
          <div className="form-actions">
            <button type="submit" className="btn-primary">Salvar provento</button>
          </div>
        </form>
      </article>

      <article className="card form-card">
        <h3>Importar proventos por CSV</h3>
        <p className="subtitle">Use linhas com tipo: dividendo, jcp ou aluguel.</p>
        {!!importError && <p className="notice-warn">{importError}</p>}
        {!!importMessage && <p className="notice-ok">{importMessage}</p>}
        {importWarnings.length > 0 && (
          <ul className="import-errors">
            {importWarnings.map((item, idx) => (
              <li key={`income-warn-${idx}`}>{item}</li>
            ))}
          </ul>
        )}
        <form onSubmit={onImportCsv} className="form-grid">
          <div>
            <label htmlFor="target_portfolio_id_csv_income">Carteira destino</label>
            <select id="target_portfolio_id_csv_income" name="target_portfolio_id" value={form.target_portfolio_id} onChange={(e) => setForm((current) => ({ ...current, target_portfolio_id: e.target.value }))} required>
              {portfolios.map((item) => (
                <option key={`income-csv-${item.id}`} value={item.id}>{item.name}</option>
              ))}
            </select>
          </div>
          <div>
            <label htmlFor="csv_file_income">Arquivo CSV</label>
            <input id="csv_file_income" name="csv_file" type="file" accept=".csv,text/csv" required />
          </div>
          <div className="form-actions">
            <button type="submit" className="btn-primary">Importar CSV</button>
          </div>
        </form>
      </article>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Sel.</th>
              <th>Carteira</th>
              <th>Ticker</th>
              <th>Tipo</th>
              <th>Valor</th>
              <th>Data</th>
            </tr>
          </thead>
          <tbody>
            {!loading && rows.map((item, idx) => (
              <tr key={`${item.ticker}-${item.date}-${idx}`}>
                <td>
                  <input
                    type="checkbox"
                    checked={selectedIncomeIds.includes(item.id)}
                    onChange={() => toggleIncome(item.id)}
                  />
                </td>
                <td>{item.portfolio_name}</td>
                <td>{item.ticker}</td>
                <td>{String(item.income_type || '').toUpperCase()}</td>
                <td>{brl(item.amount)}</td>
                <td>{dateBr(item.date)}</td>
              </tr>
            ))}
            {!loading && rows.length === 0 && (
              <tr>
                <td colSpan={6}>Nenhum provento cadastrado ainda.</td>
              </tr>
            )}
            {loading && (
              <tr>
                <td colSpan={6}>Carregando...</td>
              </tr>
            )}
          </tbody>
        </table>
        <div className="table-actions">
          <button type="button" className="btn-danger" disabled={removingIncomes} onClick={onRemoveIncomes}>
            {removingIncomes ? 'Removendo...' : 'Remover selecionados'}
          </button>
        </div>
      </div>
    </section>
  )
}

export default NewIncomePage
