export function toFiniteNumber(value, fallback = null) {
  const num = Number(value)
  return Number.isFinite(num) ? num : fallback
}

export function formatCurrencyBRL(value, fallback = '-') {
  const num = toFiniteNumber(value, null)
  if (num == null) return fallback
  return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(num)
}

export function formatPercent(value, digits = 2, { signed = false, fallback = '-' } = {}) {
  const num = toFiniteNumber(value, null)
  if (num == null) return fallback
  const signal = signed ? (num >= 0 ? '+' : '') : ''
  return `${signal}${num.toFixed(digits)}%`
}

export function formatDecimal(value, digits = 2, fallback = '-') {
  const num = toFiniteNumber(value, null)
  if (num == null) return fallback
  return num.toFixed(digits)
}

export function formatQuantity(value, { minDigits = 0, maxDigits = 4, trim = true, fallback = '-' } = {}) {
  const num = toFiniteNumber(value, null)
  if (num == null) return fallback
  const safeMax = Math.max(Number(maxDigits) || 0, Number(minDigits) || 0)
  const text = new Intl.NumberFormat('pt-BR', {
    minimumFractionDigits: Math.max(Number(minDigits) || 0, 0),
    maximumFractionDigits: safeMax,
  }).format(num)
  if (!trim) return text
  // Remove zeros finais apenas quando houver parte decimal.
  return text.replace(/,(\d*?[1-9])0+$/, ',$1').replace(/,$/, '')
}

// Espelha _is_us_stock_ticker do backend: acoes US sao so letras (dot removido),
// <= 6 chars, e nao FII (...11) nem cripto (...USDT / ...-USD).
export function isUsStockTicker(ticker) {
  const t = String(ticker || '').trim().toUpperCase()
  if (!t) return false
  if (t.endsWith('11')) return false
  if (t.endsWith('USDT') || t.endsWith('-USD')) return false
  const clean = t.replace(/\./g, '')
  return /^[A-Z]+$/.test(clean) && clean.length <= 6
}

// Moeda de lancamento do ativo: acoes US sao em dolar; o resto em real.
export function assetPriceCurrency(ticker) {
  return isUsStockTicker(ticker) ? 'US$' : 'R$'
}

export function formatCompactBrl(value, fallback = '-') {
  const num = toFiniteNumber(value, null)
  if (num == null) return fallback
  const abs = Math.abs(num)
  if (abs >= 1_000_000_000) return `R$ ${(num / 1_000_000_000).toFixed(2)} bi`
  if (abs >= 1_000_000) return `R$ ${(num / 1_000_000).toFixed(2)} mi`
  if (abs >= 1_000) return `R$ ${(num / 1_000).toFixed(1)} mil`
  return formatCurrencyBRL(num, fallback)
}
