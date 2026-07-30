// Valor de ganho/perda com indicador nao-cromatico (seta ▲/▼) alem da cor,
// para nao depender so de verde/vermelho (acessibilidade / daltonismo).
// A cor vem das classes .up/.down; o estilo .trend/.trend-arrow esta no styles.css.
function Trend({ value, children, className = '' }) {
  const positive = Number(value || 0) >= 0
  const tone = positive ? 'up' : 'down'
  return (
    <span className={`trend ${tone}${className ? ` ${className}` : ''}`}>
      <span className="trend-arrow" aria-hidden="true">{positive ? '▲' : '▼'}</span>
      {' '}
      {children}
    </span>
  )
}

export default Trend
