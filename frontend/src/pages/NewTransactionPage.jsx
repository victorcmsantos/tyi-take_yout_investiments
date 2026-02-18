import { useEffect, useMemo, useState } from 'react'
import { apiGet, apiPost, apiPostForm } from '../api'

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

function NewTransactionPage({ selectedPortfolioIds, portfolios }) {
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const [form, setForm] = useState({
    target_portfolio_id: '',
    tx_type: 'buy',
    ticker: '',
    shares: '',
    price: '',
    date: '',
    name: '',
    sector: '',
  })
  const [fixedError, setFixedError] = useState('')
  const [fixedMessage, setFixedMessage] = useState('')
  const [importError, setImportError] = useState('')
  const [importMessage, setImportMessage] = useState('')
  const [importWarnings, setImportWarnings] = useState([])
  const [fixedImportError, setFixedImportError] = useState('')
  const [fixedImportMessage, setFixedImportMessage] = useState('')
  const [fixedImportWarnings, setFixedImportWarnings] = useState([])
  const [fixedImporting, setFixedImporting] = useState(false)
  const [fixedForm, setFixedForm] = useState({
    target_portfolio_id: '',
    distributor: '',
    issuer: '',
    investment_type: '',
    rate_type: 'FIXO',
    juros_fixo: '',
    ipca: '',
    cdi: '',
    date_aporte: '',
    maturity_date: '',
    aporte: '',
    reinvested: '',
  })

  const activePortfolioId = useMemo(
    () => selectedPortfolioIds?.[0] || portfolios?.[0]?.id || '',
    [selectedPortfolioIds, portfolios],
  )

  const loadTransactions = async () => {
    setLoading(true)
    setError('')
    try {
      const data = await apiGet('/api/transactions', { portfolio_id: selectedPortfolioIds })
      setRows(data)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadTransactions()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(selectedPortfolioIds)])

  useEffect(() => {
    setForm((current) => ({ ...current, target_portfolio_id: String(activePortfolioId || '') }))
    setFixedForm((current) => ({ ...current, target_portfolio_id: String(activePortfolioId || '') }))
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
      const result = await apiPost('/api/transactions', payload)
      setMessage(result.message || 'Transacao registrada com sucesso.')
      setForm((current) => ({ ...current, ticker: '', shares: '', price: '', date: '', name: '', sector: '' }))
      await loadTransactions()
    } catch (err) {
      setError(err.message)
    }
  }

  const onFixedChange = (event) => {
    const { name, value } = event.target
    setFixedForm((current) => ({ ...current, [name]: value }))
  }

  const onSubmitFixed = async (event) => {
    event.preventDefault()
    setFixedError('')
    setFixedMessage('')
    try {
      const result = await apiPost('/api/fixed-incomes', fixedForm)
      setFixedMessage(result.message || 'Registro de renda fixa salvo com sucesso.')
      setFixedForm((current) => ({
        ...current,
        distributor: '',
        issuer: '',
        investment_type: '',
        juros_fixo: '',
        ipca: '',
        cdi: '',
        date_aporte: '',
        maturity_date: '',
        aporte: '',
        reinvested: '',
      }))
    } catch (err) {
      setFixedError(err.message)
    }
  }

  const onImportTransactionsCsv = async (event) => {
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
      setImportMessage(`Importacao finalizada. ${Number(result.imported || 0)} transacao(oes) importada(s).`)
      setImportWarnings(result.errors || [])
      await loadTransactions()
      event.target.reset()
    } catch (err) {
      setImportError(err.message)
    }
  }

  const onImportFixedCsv = async (event) => {
    event.preventDefault()
    setFixedImporting(true)
    setFixedImportError('')
    setFixedImportMessage('')
    setFixedImportWarnings([])
    const selectedPortfolioId = event.target.target_portfolio_id?.value || fixedForm.target_portfolio_id
    const file = event.target.fixed_income_csv_file?.files?.[0]
    if (!file) {
      setFixedImportError('Selecione um arquivo CSV.')
      setFixedImporting(false)
      return
    }
    try {
      const formData = new FormData()
      formData.append('target_portfolio_id', selectedPortfolioId)
      formData.append('fixed_income_csv_file', file)
      const result = await apiPostForm('/api/imports/fixed-incomes-csv', formData)
      setFixedImportMessage(`Importacao de renda fixa finalizada. ${Number(result.imported || 0)} registro(s) importado(s).`)
      setFixedImportWarnings(result.errors || [])
      event.target.reset()
    } catch (err) {
      setFixedImportError(err.message)
    } finally {
      setFixedImporting(false)
    }
  }

  return (
    <section>
      <h1>Lancar transacao</h1>
      <p className="subtitle">Adicione compra ou venda. Se o ticker nao existir, sera criado automaticamente.</p>

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
            <label htmlFor="tx_type">Tipo</label>
            <select id="tx_type" name="tx_type" value={form.tx_type} onChange={onChange} required>
              <option value="buy">Compra</option>
              <option value="sell">Venda</option>
            </select>
          </div>
          <div>
            <label htmlFor="ticker">Ticker</label>
            <input id="ticker" name="ticker" type="text" value={form.ticker} onChange={onChange} placeholder="Ex: BBAS3" required />
          </div>
          <div>
            <label htmlFor="shares">Quantidade</label>
            <input id="shares" name="shares" type="number" min="0.00000001" step="any" value={form.shares} onChange={onChange} required />
          </div>
          <div>
            <label htmlFor="price">Preco da operacao (R$)</label>
            <input id="price" name="price" type="text" value={form.price} onChange={onChange} required />
          </div>
          <div>
            <label htmlFor="date">Data da transacao</label>
            <input id="date" name="date" type="date" value={form.date} onChange={onChange} required />
          </div>
          <div>
            <label htmlFor="name">Nome (opcional)</label>
            <input id="name" name="name" type="text" value={form.name} onChange={onChange} />
          </div>
          <div>
            <label htmlFor="sector">Setor (opcional)</label>
            <input id="sector" name="sector" type="text" value={form.sector} onChange={onChange} />
          </div>
          <div className="form-actions">
            <button type="submit" className="btn-primary">Salvar transacao</button>
          </div>
        </form>
      </article>

      <article className="card form-card">
        <h3>Importar transacoes por CSV</h3>
        <p className="subtitle">Colunas aceitas: ticker, tipo/tx_type, quantidade/shares, preco/price, data/date, nome/name, setor/sector, valor/amount.</p>
        {!!importError && <p className="notice-warn">{importError}</p>}
        {!!importMessage && <p className="notice-ok">{importMessage}</p>}
        {importWarnings.length > 0 && (
          <ul className="import-errors">
            {importWarnings.map((item, idx) => (
              <li key={`tx-warn-${idx}`}>{item}</li>
            ))}
          </ul>
        )}
        <form onSubmit={onImportTransactionsCsv} className="form-grid">
          <div>
            <label htmlFor="target_portfolio_id_csv">Carteira destino</label>
            <select id="target_portfolio_id_csv" name="target_portfolio_id" value={form.target_portfolio_id} onChange={(e) => setForm((current) => ({ ...current, target_portfolio_id: e.target.value }))} required>
              {portfolios.map((item) => (
                <option key={`csv-${item.id}`} value={item.id}>{item.name}</option>
              ))}
            </select>
          </div>
          <div>
            <label htmlFor="csv_file">Arquivo CSV</label>
            <input id="csv_file" name="csv_file" type="file" accept=".csv,text/csv" required />
          </div>
          <div className="form-actions">
            <button type="submit" className="btn-primary">Importar CSV</button>
          </div>
        </form>
      </article>

      <details className="asset-group">
        <summary className="asset-group-summary">
          <div>
            <strong>Transacoes registradas</strong>
            <small>{loading ? 'Carregando...' : `${rows.length} registro(s)`}</small>
          </div>
          <span className="asset-group-chevron">⌄</span>
        </summary>
        <div className="asset-group-body">
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Carteira</th>
                  <th>Ticker</th>
                  <th>Tipo</th>
                  <th>Qtd</th>
                  <th>Preco</th>
                  <th>Total</th>
                  <th>Data</th>
                </tr>
              </thead>
              <tbody>
                {!loading && rows.map((tx) => (
                  <tr key={tx.id}>
                    <td>{tx.portfolio_name}</td>
                    <td>{tx.ticker}</td>
                    <td>{tx.tx_type === 'buy' ? 'Compra' : 'Venda'}</td>
                    <td>{Number(tx.shares || 0).toFixed(4)}</td>
                    <td>{brl(tx.price)}</td>
                    <td>{brl(tx.total_value)}</td>
                    <td>{dateBr(tx.date)}</td>
                  </tr>
                ))}
                {!loading && rows.length === 0 && (
                  <tr>
                    <td colSpan={7}>Sem transacoes para as carteiras selecionadas.</td>
                  </tr>
                )}
                {loading && (
                  <tr>
                    <td colSpan={7}>Carregando...</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </details>

      <h2 style={{ marginTop: 16 }}>Lancar renda fixa</h2>
      <p className="subtitle">Cadastre CDB/LCI/LCA e outros titulos.</p>
      {!!fixedError && <p className="notice-warn">{fixedError}</p>}
      {!!fixedMessage && <p className="notice-ok">{fixedMessage}</p>}

      <article className="card form-card">
        <form onSubmit={onSubmitFixed} className="form-grid">
          <div>
            <label htmlFor="target_portfolio_id_fixed">Carteira destino</label>
            <select id="target_portfolio_id_fixed" name="target_portfolio_id" value={fixedForm.target_portfolio_id} onChange={onFixedChange} required>
              {portfolios.map((item) => (
                <option key={`fixed-${item.id}`} value={item.id}>{item.name}</option>
              ))}
            </select>
          </div>
          <div>
            <label htmlFor="distributor">Distribuidor</label>
            <input id="distributor" name="distributor" type="text" value={fixedForm.distributor} onChange={onFixedChange} required />
          </div>
          <div>
            <label htmlFor="issuer">Emissor</label>
            <input id="issuer" name="issuer" type="text" value={fixedForm.issuer} onChange={onFixedChange} required />
          </div>
          <div>
            <label htmlFor="investment_type">Investimento</label>
            <input id="investment_type" name="investment_type" type="text" value={fixedForm.investment_type} onChange={onFixedChange} required />
          </div>
          <div>
            <label htmlFor="rate_type">Tipo de taxa</label>
            <select id="rate_type" name="rate_type" value={fixedForm.rate_type} onChange={onFixedChange} required>
              <option value="FIXO">FIXO</option>
              <option value="FIXO+IPCA">FIXO+IPCA</option>
              <option value="IPCA">IPCA</option>
              <option value="CDI">CDI</option>
              <option value="FIXO+CDI">FIXO+CDI</option>
            </select>
          </div>
          <div>
            <label htmlFor="juros_fixo">Juros Fixo (%)</label>
            <input id="juros_fixo" name="juros_fixo" type="text" value={fixedForm.juros_fixo} onChange={onFixedChange} />
          </div>
          <div>
            <label htmlFor="ipca">IPCA (%)</label>
            <input id="ipca" name="ipca" type="text" value={fixedForm.ipca} onChange={onFixedChange} />
          </div>
          <div>
            <label htmlFor="cdi">CDI (%)</label>
            <input id="cdi" name="cdi" type="text" value={fixedForm.cdi} onChange={onFixedChange} />
          </div>
          <div>
            <label htmlFor="date_aporte">Data aporte</label>
            <input id="date_aporte" name="date_aporte" type="date" value={fixedForm.date_aporte} onChange={onFixedChange} required />
          </div>
          <div>
            <label htmlFor="maturity_date">Data final</label>
            <input id="maturity_date" name="maturity_date" type="date" value={fixedForm.maturity_date} onChange={onFixedChange} required />
          </div>
          <div>
            <label htmlFor="aporte">Aporte (R$)</label>
            <input id="aporte" name="aporte" type="text" value={fixedForm.aporte} onChange={onFixedChange} required />
          </div>
          <div>
            <label htmlFor="reinvested">Reinvestido (R$)</label>
            <input id="reinvested" name="reinvested" type="text" value={fixedForm.reinvested} onChange={onFixedChange} />
          </div>
          <div className="form-actions">
            <button type="submit" className="btn-primary">Salvar renda fixa</button>
          </div>
        </form>
      </article>

      <article className="card form-card">
        <h3>Importar renda fixa por CSV</h3>
        <p className="subtitle">Ordem padrao: Distribuidor, Emissor, Investimento, tipo, data aporte, aporte, Reinvestido, data final, Juros Fixo, IPCA, CDI.</p>
        {!!fixedImportError && <p className="notice-warn">{fixedImportError}</p>}
        {!!fixedImportMessage && <p className="notice-ok">{fixedImportMessage}</p>}
        {fixedImportWarnings.length > 0 && (
          <ul className="import-errors">
            {fixedImportWarnings.map((item, idx) => (
              <li key={`fixed-warn-${idx}`}>{item}</li>
            ))}
          </ul>
        )}
        <form onSubmit={onImportFixedCsv} className="form-grid">
          <div>
            <label htmlFor="target_portfolio_id_fixed_csv">Carteira destino</label>
            <select id="target_portfolio_id_fixed_csv" name="target_portfolio_id" value={fixedForm.target_portfolio_id} onChange={(e) => setFixedForm((current) => ({ ...current, target_portfolio_id: e.target.value }))} required>
              {portfolios.map((item) => (
                <option key={`fixed-csv-${item.id}`} value={item.id}>{item.name}</option>
              ))}
            </select>
          </div>
          <div>
            <label htmlFor="fixed_income_csv_file">Arquivo CSV</label>
            <input id="fixed_income_csv_file" name="fixed_income_csv_file" type="file" accept=".csv,text/csv" required />
          </div>
          <div className="form-actions">
            <button type="submit" className="btn-primary" disabled={fixedImporting}>
              {fixedImporting ? 'Importando renda fixa...' : 'Importar renda fixa'}
            </button>
          </div>
        </form>
      </article>
    </section>
  )
}

export default NewTransactionPage
