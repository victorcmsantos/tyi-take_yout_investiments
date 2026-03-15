import { Link } from 'react-router-dom'
import { formatCurrencyBRL, formatDecimal, formatPercent } from '../formatters'
import StatePanel from './StatePanel'

const brl = (value) => formatCurrencyBRL(value, 'R$ 0,00')
const pct = (value, signed = false) => formatPercent(value, 2, { signed, fallback: '0.00%' })
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

function AssetsTable({ sortedAssets, incomesByTicker, toggleSort, sortLabel }) {
  return (
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
            <tr key={asset.ticker} className="asset-row">
              <td className="sticky-col sticky-col-ticker table-code"><Link to={`/ativo/${asset.ticker}`}>{asset.ticker}</Link></td>
              <td className="table-text-strong">{asset.name}</td>
              <td className="table-text-soft">{asset.sector}</td>
              <td className="table-number">
                <div className="market-data-cell">
                  <span className="table-number">{brl(asset.price)}</span>
                  <small className={asset?.market_data?.is_stale ? 'market-data-badge stale' : 'market-data-badge live'}>
                    {formatSyncLabel(asset)}
                  </small>
                </div>
              </td>
              <td className="table-number">{pct(asset.dy)}</td>
              <td className="table-number">{formatDecimal(asset.pl, 2, '-')}</td>
              <td className="table-number">{formatDecimal(asset.pvp, 2, '-')}</td>
              <td className="table-number">{brl(incomesByTicker[asset.ticker] || 0)}</td>
              <td className={`table-number ${Number(asset.variation_day || 0) >= 0 ? 'up' : 'down'}`}>
                {pct(asset.variation_day, true)}
              </td>
              <td className={`table-number ${Number(asset.variation_7d || 0) >= 0 ? 'up' : 'down'}`}>
                {pct(asset.variation_7d, true)}
              </td>
              <td className={`table-number ${Number(asset.variation_30d || 0) >= 0 ? 'up' : 'down'}`}>
                {pct(asset.variation_30d, true)}
              </td>
            </tr>
          ))}
          {sortedAssets.length === 0 && (
            <tr>
              <td colSpan={11}>
                <StatePanel
                  compact
                  eyebrow="Sem ativos"
                  title="Nenhum ativo cadastrado ainda"
                  description="Lance uma transacao ou troque a carteira selecionada para preencher esta tabela."
                />
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

export default AssetsTable
