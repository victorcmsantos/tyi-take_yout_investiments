import { formatCompactBrl, formatPercent } from '../formatters'
import StatePanel from './StatePanel'

const pct = (value, signed = false) => formatPercent(value, 2, { signed, fallback: '0.00%' })

function SectorsTable({ sectors }) {
  return (
    <section className="dashboard-section-card">
      <div className="dashboard-section-heading">
        <div>
          <small>Distribuicao</small>
          <h2>Mapa de setores</h2>
        </div>
        <p>Veja concentracao, media de dividendos e peso economico por setor sem sair da dashboard.</p>
      </div>
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
                <td className="table-text-strong">{sector.sector}</td>
                <td className="table-number">{sector.assets_count}</td>
                <td className="table-number">{pct(sector.avg_dy)}</td>
                <td className="table-number">{formatCompactBrl(Number(sector.market_cap_bi || 0) * 1_000_000_000, '-')}</td>
              </tr>
            ))}
            {sectors.length === 0 && (
              <tr>
                <td colSpan={4}>
                  <StatePanel
                    compact
                    eyebrow="Setores"
                    title="Sem dados de setores disponiveis"
                    description="Quando houver ativos com classificacao setorial, este mapa passa a aparecer aqui."
                  />
                </td>
              </tr>
          )}
        </tbody>
      </table>
      </div>
    </section>
  )
}

export default SectorsTable
