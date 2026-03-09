import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { apiGet } from '../api'

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`
const pct = (value) => `${Number(value || 0).toFixed(2)}%`
const formatSyncLabel = (asset) => {
  const marketData = asset?.market_data || {}
  if (marketData.is_stale) {
    return 'Desatualizado'
  }
  if (marketData.updated_at) {
    return `Atualizado via ${(marketData.source || 'provider').toUpperCase()}`
  }
  return 'Sem sincronizacao'
}

function HomePage({ selectedPortfolioIds }) {
  const [assets, setAssets] = useState([])
  const [sectors, setSectors] = useState([])
  const [incomesByTicker, setIncomesByTicker] = useState({})
  const [incomesTotal, setIncomesTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [sortBy, setSortBy] = useState('name')
  const [sortDir, setSortDir] = useState('asc')

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

  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    ;(async () => {
      try {
        const [assetsData, sectorsData, incomesData] = await Promise.all([
          apiGet('/api/assets'),
          apiGet('/api/sectors'),
          apiGet('/api/incomes', { portfolio_id: selectedPortfolioIds }),
        ])
        if (!active) return
        const byTicker = incomesData.reduce((acc, income) => {
          const ticker = String(income.ticker || '').toUpperCase()
          if (!ticker) return acc
          acc[ticker] = (acc[ticker] || 0) + Number(income.amount || 0)
          return acc
        }, {})
        setAssets(assetsData)
        setSectors(sectorsData)
        setIncomesByTicker(byTicker)
        setIncomesTotal(Object.values(byTicker).reduce((acc, value) => acc + Number(value || 0), 0))
      } catch (err) {
        if (!active) return
        setError(err.message)
      } finally {
        if (active) setLoading(false)
      }
    })()
    return () => {
      active = false
    }
  }, [selectedPortfolioIds])

  const highlights = assets.length > 0
    ? {
      highestDy: assets.reduce((best, asset) => (Number(asset.dy || 0) > Number(best.dy || 0) ? asset : best), assets[0]),
      highestGain: assets.reduce((best, asset) => (Number(asset.variation_day || 0) > Number(best.variation_day || 0) ? asset : best), assets[0]),
      largestCap: assets.reduce((best, asset) => (Number(asset.market_cap_bi || 0) > Number(best.market_cap_bi || 0) ? asset : best), assets[0]),
    }
    : null

  const sortedAssets = useMemo(() => {
    const toNumber = (value) => {
      const num = Number(value)
      return Number.isFinite(num) ? num : 0
    }
    const sorted = [...assets]
    sorted.sort((a, b) => {
      let left = null
      let right = null

      if (sortBy === 'incomes') {
        left = toNumber(incomesByTicker[a.ticker] || 0)
        right = toNumber(incomesByTicker[b.ticker] || 0)
      } else if (['ticker', 'name', 'sector'].includes(sortBy)) {
        left = String(a[sortBy] || '').toUpperCase()
        right = String(b[sortBy] || '').toUpperCase()
      } else {
        left = toNumber(a[sortBy])
        right = toNumber(b[sortBy])
      }

      if (left < right) return sortDir === 'asc' ? -1 : 1
      if (left > right) return sortDir === 'asc' ? 1 : -1
      return 0
    })
    return sorted
  }, [assets, incomesByTicker, sortBy, sortDir])

  const staleAssetsCount = useMemo(
    () => assets.filter((asset) => asset?.market_data?.is_stale).length,
    [assets],
  )

  if (loading) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>

  return (
    <section>
      <h1>Acoes</h1>
      {staleAssetsCount > 0 && (
        <p className="notice-warn">
          {staleAssetsCount} ativo(s) exibem cotacao possivelmente antiga. Veja a coluna de preco para identificar quais.
        </p>
      )}

      {highlights && (
        <div className="cards">
          <article className="card">
            <h3>Maior dividend yield</h3>
            <p>{highlights.highestDy.ticker}</p>
            <small>{pct(highlights.highestDy.dy)} a.a.</small>
          </article>
          <article className="card">
            <h3>Maior alta do dia</h3>
            <p>{highlights.highestGain.ticker}</p>
            <small>{pct(highlights.highestGain.variation_day)}</small>
          </article>
          <article className="card">
            <h3>Maior valor de mercado</h3>
            <p>{highlights.largestCap.ticker}</p>
            <small>R$ {Number(highlights.largestCap.market_cap_bi || 0).toFixed(2)} bi</small>
          </article>
          <article className="card">
            <h3>Proventos totais</h3>
            <p>{brl(incomesTotal)}</p>
            <small>Carteiras selecionadas</small>
          </article>
        </div>
      )}

      <div className="table-wrap">
        <table className="asset-table">
          <thead>
            <tr>
              <th className="sticky-col sticky-col-ticker"><button type="button" className="th-sort-btn" onClick={() => toggleSort('ticker')}>{sortLabel('Ticker', 'ticker')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('name')}>{sortLabel('Nome', 'name')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('sector')}>{sortLabel('Setor', 'sector')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('price')}>{sortLabel('Preco', 'price')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('dy')}>{sortLabel('DY', 'dy')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('pl')}>{sortLabel('P/L', 'pl')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('pvp')}>{sortLabel('P/VP', 'pvp')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('incomes')}>{sortLabel('Proventos', 'incomes')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('variation_day')}>{sortLabel('Dia', 'variation_day')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('variation_7d')}>{sortLabel('7 dias', 'variation_7d')}</button></th>
              <th><button type="button" className="th-sort-btn" onClick={() => toggleSort('variation_30d')}>{sortLabel('30 dias', 'variation_30d')}</button></th>
            </tr>
          </thead>
          <tbody>
            {sortedAssets.map((asset) => (
              <tr key={asset.ticker}>
                <td className="sticky-col sticky-col-ticker"><Link to={`/ativo/${asset.ticker}`}>{asset.ticker}</Link></td>
                <td>{asset.name}</td>
                <td>{asset.sector}</td>
                <td>
                  <div className="market-data-cell">
                    <span>{brl(asset.price)}</span>
                    <small className={asset?.market_data?.is_stale ? 'market-data-badge stale' : 'market-data-badge live'}>
                      {formatSyncLabel(asset)}
                    </small>
                  </div>
                </td>
                <td>{pct(asset.dy)}</td>
                <td>{Number(asset.pl || 0).toFixed(2)}</td>
                <td>{Number(asset.pvp || 0).toFixed(2)}</td>
                <td>{brl(incomesByTicker[asset.ticker] || 0)}</td>
                <td className={Number(asset.variation_day || 0) >= 0 ? 'up' : 'down'}>
                  {pct(asset.variation_day)}
                </td>
                <td className={Number(asset.variation_7d || 0) >= 0 ? 'up' : 'down'}>
                  {pct(asset.variation_7d)}
                </td>
                <td className={Number(asset.variation_30d || 0) >= 0 ? 'up' : 'down'}>
                  {pct(asset.variation_30d)}
                </td>
              </tr>
            ))}
            {assets.length === 0 && (
              <tr>
                <td colSpan={11}>Nenhum ativo cadastrado ainda.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <h2 style={{ marginTop: 24 }}>Mapa de setores</h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Setor</th>
              <th>Ativos</th>
              <th>DY medio</th>
              <th>Valor de mercado</th>
            </tr>
          </thead>
          <tbody>
            {sectors.map((sector) => (
              <tr key={sector.sector}>
                <td>{sector.sector}</td>
                <td>{sector.assets_count}</td>
                <td>{pct(sector.avg_dy)}</td>
                <td>R$ {Number(sector.market_cap_bi || 0).toFixed(2)} bi</td>
              </tr>
            ))}
            {sectors.length === 0 && (
              <tr>
                <td colSpan={4}>Sem dados de setores disponiveis.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default HomePage
