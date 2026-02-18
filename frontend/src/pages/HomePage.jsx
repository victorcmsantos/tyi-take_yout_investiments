import { useEffect, useState } from 'react'
import { apiGet } from '../api'

function HomePage({ selectedPortfolioIds }) {
  const [assets, setAssets] = useState([])
  const [sectors, setSectors] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    ;(async () => {
      try {
        const [assetsData, sectorsData] = await Promise.all([
          apiGet('/api/assets'),
          apiGet('/api/sectors'),
        ])
        if (!active) return
        setAssets(assetsData)
        setSectors(sectorsData)
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

  if (loading) return <p>Carregando...</p>
  if (error) return <p className="error">{error}</p>

  return (
    <section>
      <h1>Acoes</h1>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Nome</th>
              <th>Setor</th>
              <th>Preco</th>
              <th>DY</th>
              <th>Dia</th>
            </tr>
          </thead>
          <tbody>
            {assets.map((asset) => (
              <tr key={asset.ticker}>
                <td>{asset.ticker}</td>
                <td>{asset.name}</td>
                <td>{asset.sector}</td>
                <td>R$ {Number(asset.price || 0).toFixed(2)}</td>
                <td>{Number(asset.dy || 0).toFixed(2)}%</td>
                <td className={Number(asset.variation_day || 0) >= 0 ? 'up' : 'down'}>
                  {Number(asset.variation_day || 0).toFixed(2)}%
                </td>
              </tr>
            ))}
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
                <td>{Number(sector.avg_dy || 0).toFixed(2)}%</td>
                <td>R$ {Number(sector.market_cap_bi || 0).toFixed(2)} bi</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default HomePage
