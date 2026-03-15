function normalizeApiDate(value) {
  const text = String(value || '').trim()
  if (!text) return ''
  return text.includes(' ') ? text.replace(' ', 'T') : text
}

export function parseApiDate(value) {
  const text = normalizeApiDate(value)
  if (!text) return null
  if (/Z$|[+-]\d{2}:\d{2}$/.test(text)) {
    const parsed = Date.parse(text)
    return Number.isNaN(parsed) ? null : parsed
  }
  const parsed = Date.parse(`${text}Z`)
  return Number.isNaN(parsed) ? null : parsed
}

export function formatDateTimeLocal(value, fallback = '') {
  const timestamp = parseApiDate(value)
  if (timestamp === null) return fallback || String(value || '').trim()
  const formatter = new Intl.DateTimeFormat('pt-BR', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
  return formatter.format(new Date(timestamp))
}

export function currentBrowserTimeZone() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || 'local'
  } catch (_) {
    return 'local'
  }
}

export function formatAgeFromNow(value, fallback = '-') {
  const timestamp = parseApiDate(value)
  if (timestamp === null) return fallback
  const diffMs = Math.max(Date.now() - timestamp, 0)
  const diffSeconds = Math.floor(diffMs / 1000)
  if (diffSeconds < 60) return `${diffSeconds}s`
  const diffMinutes = Math.floor(diffSeconds / 60)
  if (diffMinutes < 60) return `${diffMinutes} min`
  const diffHours = Math.floor(diffMinutes / 60)
  if (diffHours < 24) return `${diffHours}h ${diffMinutes % 60}min`
  const diffDays = Math.floor(diffHours / 24)
  return `${diffDays}d ${diffHours % 24}h`
}

export function dateTimeBr(value) {
  return formatDateTimeLocal(value, '-')
}
