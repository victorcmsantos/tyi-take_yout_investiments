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

export function formatCompactBrl(value, fallback = '-') {
  const num = toFiniteNumber(value, null)
  if (num == null) return fallback
  const abs = Math.abs(num)
  if (abs >= 1_000_000_000) return `R$ ${(num / 1_000_000_000).toFixed(2)} bi`
  if (abs >= 1_000_000) return `R$ ${(num / 1_000_000).toFixed(2)} mi`
  if (abs >= 1_000) return `R$ ${(num / 1_000).toFixed(1)} mil`
  return formatCurrencyBRL(num, fallback)
}
