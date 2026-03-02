import { useMemo, useState } from 'react'

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`

function normalizeTickers(rawTickers) {
  const unique = []
  const seen = new Set()
  ;(Array.isArray(rawTickers) ? rawTickers : [])
    .map((item) => String(item || '').trim().toUpperCase())
    .filter(Boolean)
    .forEach((ticker) => {
      if (seen.has(ticker)) return
      seen.add(ticker)
      unique.push(ticker)
    })
  return unique
}

function AllocationPage({ assets }) {
  const [totalAmount, setTotalAmount] = useState('')
  const [tickers, setTickers] = useState([''])
  const [result, setResult] = useState(null)
  const [message, setMessage] = useState('')

  const assetsByTicker = useMemo(() => {
    const map = {}
    ;(Array.isArray(assets) ? assets : []).forEach((asset) => {
      const ticker = String(asset?.ticker || '').toUpperCase()
      if (!ticker) return
      map[ticker] = asset
    })
    return map
  }, [assets])

  const onChangeTicker = (idx, nextValue) => {
    setTickers((current) => {
      const next = [...current]
      next[idx] = nextValue
      const lastIdx = next.length - 1
      const lastHasValue = String(next[lastIdx] || '').trim().length > 0
      if (lastHasValue) next.push('')
      if (next.length === 0) next.push('')
      return next
    })
  }

  const onCalculate = (event) => {
    event.preventDefault()
    setMessage('')

    const amount = Number(totalAmount)
    if (!Number.isFinite(amount) || amount <= 0) {
      setResult(null)
      setMessage('Informe um valor total valido.')
      return
    }

    const selectedTickers = normalizeTickers(tickers)
    if (selectedTickers.length === 0) {
      setResult(null)
      setMessage('Informe ao menos 1 ticker.')
      return
    }

    const unknown = []
    const invalidPrice = []
    const items = []

    selectedTickers.forEach((ticker) => {
      const asset = assetsByTicker[ticker]
      if (!asset) {
        unknown.push(ticker)
        return
      }
      const price = Number(asset.price)
      if (!Number.isFinite(price) || price <= 0) {
        invalidPrice.push(ticker)
        return
      }
      items.push({
        ticker,
        name: String(asset.name || '').trim(),
        price,
      })
    })

    if (items.length === 0) {
      setResult(null)
      setMessage('Nenhum ticker com preco valido encontrado (verifique se o ativo existe e se o preco esta atualizado).')
      return
    }

    const targetAllocation = amount / items.length
    const computed = items.map((item) => {
      const shares = Math.floor(targetAllocation / item.price)
      const cost = shares * item.price
      return {
        ...item,
        shares,
        cost,
      }
    })

    const totalSpent = computed.reduce((acc, row) => acc + Number(row.cost || 0), 0)
    const remainingBalance = amount - totalSpent

    setResult({
      amount,
      tickers: selectedTickers,
      targetAllocation,
      totalSpent,
      remainingBalance,
      rows: computed,
      unknown,
      invalidPrice,
    })
  }

  return (
    <section>
      <h1>Alocador</h1>
      <p className="subtitle">Divide o valor igualmente entre tickers e sugere quantidades com base no preco atual cadastrado.</p>

      {!!message && <p className="notice-warn">{message}</p>}

      <article className="card form-card">
        <form onSubmit={onCalculate} className="form-grid">
          <div>
            <label htmlFor="allocation-total">Valor total</label>
            <input
              id="allocation-total"
              type="number"
              step="0.01"
              min="0"
              value={totalAmount}
              onChange={(e) => setTotalAmount(e.target.value)}
              placeholder="Ex: 6000"
              required
            />
          </div>

          <div>
            <label>Tickers</label>
            {tickers.map((value, idx) => (
              <input
                key={`ticker-${idx}`}
                type="text"
                value={value}
                onChange={(e) => onChangeTicker(idx, e.target.value)}
                placeholder={idx === 0 ? 'Ex: WEGE3' : 'Adicionar outro ticker (opcional)'}
                list={String(value || '').trim().length >= 1 ? 'allocation-ticker-suggestions' : undefined}
              />
            ))}
            <datalist id="allocation-ticker-suggestions">
              {(Array.isArray(assets) ? assets : [])
                .map((asset) => ({
                  ticker: String(asset?.ticker || '').toUpperCase(),
                  name: String(asset?.name || '').trim(),
                }))
                .filter((item) => item.ticker)
                .map((item) => (
                  <option key={item.ticker} value={item.ticker}>{item.name}</option>
                ))}
            </datalist>
          </div>

          <div className="form-actions">
            <button type="submit" className="btn-primary">Calcular</button>
          </div>
        </form>
      </article>

      {result && (
        <>
          {(result.unknown.length > 0 || result.invalidPrice.length > 0) && (
            <p className="notice-warn">
              {result.unknown.length > 0 ? `Nao encontrados: ${result.unknown.join(', ')}. ` : ''}
              {result.invalidPrice.length > 0 ? `Sem preco valido: ${result.invalidPrice.join(', ')}.` : ''}
            </p>
          )}

          <article className="card">
            <h3>Resumo</h3>
            <p>Valor total: <strong>{brl(result.amount)}</strong></p>
            <p>Target por ativo ({result.rows.length}): <strong>{brl(result.targetAllocation)}</strong></p>
            <p>Total gasto: <strong>{brl(result.totalSpent)}</strong></p>
            <p>Saldo restante: <strong>{brl(result.remainingBalance)}</strong></p>
          </article>

          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Nome</th>
                  <th>Preco</th>
                  <th>Qtd</th>
                  <th>Custo</th>
                </tr>
              </thead>
              <tbody>
                {result.rows.map((row) => (
                  <tr key={row.ticker}>
                    <td>{row.ticker}</td>
                    <td>{row.name || '-'}</td>
                    <td>{brl(row.price)}</td>
                    <td>{row.shares}</td>
                    <td>{brl(row.cost)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  )
}

export default AllocationPage
