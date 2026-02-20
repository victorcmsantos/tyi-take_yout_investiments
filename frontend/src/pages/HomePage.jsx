import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { apiGet, apiPost } from '../api'

const brl = (value) => `R$ ${Number(value || 0).toFixed(2)}`
const pct = (value) => `${Number(value || 0).toFixed(2)}%`

function HomePage({ selectedPortfolioIds }) {
  const [assets, setAssets] = useState([])
  const [sectors, setSectors] = useState([])
  const [incomesByTicker, setIncomesByTicker] = useState({})
  const [incomesTotal, setIncomesTotal] = useState(0)
  const [syncMessage, setSyncMessage] = useState('')
  const [syncStatus, setSyncStatus] = useState('')
  const [syncing, setSyncing] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

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

  const onSyncYahoo = async () => {
    setSyncing(true)
    setSyncMessage('')
    setSyncStatus('')
    try {
      const result = await apiPost('/api/sync/yahoo')
      const failed = Number(result.failed_count || 0)
      if (failed > 0) {
        setSyncStatus('warn')
        setSyncMessage(`Atualizacao concluida com pendencias: ${failed} ticker(s) falharam no Yahoo.`)
      } else {
        setSyncStatus('ok')
        setSyncMessage('Consulta ao Yahoo concluida com sucesso (cotacoes/indicadores recebidos).')
      }
    } catch (err) {
      setSyncStatus('warn')
      setSyncMessage(err.message)
    } finally {
      setSyncing(false)
    }
  }

  if (loading) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>

  const highlights = assets.length > 0
    ? {
      highestDy: assets.reduce((best, asset) => (Number(asset.dy || 0) > Number(best.dy || 0) ? asset : best), assets[0]),
      highestGain: assets.reduce((best, asset) => (Number(asset.variation_day || 0) > Number(best.variation_day || 0) ? asset : best), assets[0]),
      largestCap: assets.reduce((best, asset) => (Number(asset.market_cap_bi || 0) > Number(best.market_cap_bi || 0) ? asset : best), assets[0]),
    }
    : null

  return (
    <section>
      <h1>Acoes</h1>
      <div className="hero-actions">
        <button type="button" className="btn-primary" onClick={onSyncYahoo} disabled={syncing}>
          {syncing ? 'Atualizando Yahoo...' : 'Buscar infos do Yahoo'}
        </button>
      </div>
      {!!syncMessage && (
        <p className={syncStatus === 'ok' ? 'notice-ok' : 'notice-warn'}>{syncMessage}</p>
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
        <table>
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Nome</th>
              <th>Setor</th>
              <th>Preco</th>
              <th>DY</th>
              <th>P/L</th>
              <th>P/VP</th>
              <th>Proventos</th>
              <th>Dia</th>
              <th>7 dias</th>
              <th>30 dias</th>
            </tr>
          </thead>
          <tbody>
            {assets.map((asset) => (
              <tr key={asset.ticker}>
                <td><Link to={`/ativo/${asset.ticker}`}>{asset.ticker}</Link></td>
                <td>{asset.name}</td>
                <td>{asset.sector}</td>
                <td>{brl(asset.price)}</td>
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
