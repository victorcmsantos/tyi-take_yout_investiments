export async function apiGet(path, params = {}) {
  const url = new URL(path, window.location.origin)
  Object.entries(params).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach((item) => url.searchParams.append(key, String(item)))
      return
    }
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value))
    }
  })

  const response = await fetch(url.toString())
  const payload = await response.json()
  if (!response.ok || !payload.ok) {
    throw new Error(payload?.error || 'Falha na requisicao da API')
  }
  return payload.data
}
