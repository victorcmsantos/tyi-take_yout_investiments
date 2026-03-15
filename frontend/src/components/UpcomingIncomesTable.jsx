import { Link } from 'react-router-dom'
import { formatQuantity } from '../formatters'
import StatePanel from './StatePanel'

const money = (value, currency = 'BRL') => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  const code = String(currency || 'BRL').trim().toUpperCase() || 'BRL'
  try {
    return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: code }).format(num)
  } catch (_) {
    return `${code} ${num.toFixed(6)}`
  }
}
const dateBr = (value) => {
  const text = String(value || '').trim()
  if (!text) return '-'
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    const [y, m, d] = text.split('-')
    return `${d}/${m}/${y}`
  }
  return text
}

function UpcomingIncomesTable({ upcomingItems }) {
  return (
    <>
      <h2 style={{ marginTop: 24 }}>Agenda de proventos futuros</h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Data com (ex)</th>
              <th>Pagamento</th>
              <th>Valor por cota</th>
              <th>Qtd em carteira</th>
              <th>Estimado</th>
              <th>Fonte</th>
            </tr>
          </thead>
          <tbody>
            {upcomingItems.map((item, idx) => (
              <tr key={`upcoming-home-${item.ticker}-${item.ex_date}-${idx}`}>
                <td className="table-code"><Link to={`/ativo/${item.ticker}`}>{item.ticker}</Link></td>
                <td className="table-number">{dateBr(item.ex_date)}</td>
                <td className="table-number">{dateBr(item.payment_date)}</td>
                <td className="table-number">{money(item.amount_per_share, item.currency)}</td>
                <td className="table-number">{formatQuantity(item.shares, { maxDigits: 4, fallback: '0' })}</td>
                <td className="table-number">{money(item.estimated_total, item.currency)}</td>
                <td className="table-text-soft">{String(item.source || '').trim() || '-'}</td>
              </tr>
            ))}
            {upcomingItems.length === 0 && (
              <tr>
                <td colSpan={7}>
                  <StatePanel
                    compact
                    eyebrow="Agenda futura"
                    title="Nenhum provento futuro encontrado"
                    description="Isso pode indicar carteira sem eventos futuros publicados ou dados ainda nao sincronizados."
                  />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  )
}

export default UpcomingIncomesTable
