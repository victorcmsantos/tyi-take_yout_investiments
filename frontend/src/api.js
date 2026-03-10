async function requestJson(path, { method = 'GET', params = {}, body } = {}) {
  const url = new URL(path, window.location.origin)
  Object.entries(params || {}).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach((item) => url.searchParams.append(key, String(item)))
      return
    }
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value))
    }
  })

  const response = await fetch(url.toString(), {
    method,
    credentials: 'same-origin',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  })
  const payload = await response.json().catch(() => ({}))
  if (!response.ok || !payload.ok) {
    const error = new Error(payload?.error || 'Falha na requisicao da API')
    error.status = response.status
    throw error
  }
  return payload.data
}

const GET_CACHE = new Map()

function buildUrl(path, params = {}) {
  const url = new URL(path, window.location.origin)
  Object.entries(params || {}).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach((item) => url.searchParams.append(key, String(item)))
      return
    }
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value))
    }
  })
  return url.toString()
}

export async function apiGet(path, params = {}) {
  return requestJson(path, { method: 'GET', params })
}

export async function apiGetCached(path, params = {}, options = {}) {
  const ttlMsRaw = Number(options?.ttlMs)
  const ttlMs = Number.isFinite(ttlMsRaw) && ttlMsRaw > 0 ? ttlMsRaw : 15000
  const staleWhileRevalidate = options?.staleWhileRevalidate !== false
  const cacheKey = `GET:${buildUrl(path, params)}`
  const now = Date.now()
  const cached = GET_CACHE.get(cacheKey)

  if (cached && Number(cached.expiresAt || 0) > now) {
    return cached.data
  }

  if (cached && staleWhileRevalidate && cached.data !== undefined) {
    if (!cached.refreshPromise) {
      cached.refreshPromise = apiGet(path, params)
        .then((fresh) => {
          GET_CACHE.set(cacheKey, {
            data: fresh,
            expiresAt: Date.now() + ttlMs,
            refreshPromise: null,
          })
          return fresh
        })
        .catch(() => null)
    }
    return cached.data
  }

  const fresh = await apiGet(path, params)
  GET_CACHE.set(cacheKey, {
    data: fresh,
    expiresAt: Date.now() + ttlMs,
    refreshPromise: null,
  })
  return fresh
}

export function clearApiCache(pathPrefix = '') {
  const prefix = String(pathPrefix || '').trim()
  if (!prefix) {
    GET_CACHE.clear()
    return
  }
  Array.from(GET_CACHE.keys()).forEach((key) => {
    if (key.includes(prefix)) GET_CACHE.delete(key)
  })
}

export async function apiPost(path, body = {}, params = {}) {
  return requestJson(path, { method: 'POST', params, body })
}

export async function apiPatch(path, body = {}, params = {}) {
  return requestJson(path, { method: 'PATCH', params, body })
}

export async function apiDelete(path, body = {}, params = {}) {
  return requestJson(path, { method: 'DELETE', params, body })
}

export async function apiPostForm(path, formData, params = {}) {
  const url = new URL(path, window.location.origin)
  Object.entries(params || {}).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach((item) => url.searchParams.append(key, String(item)))
      return
    }
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value))
    }
  })

  const response = await fetch(url.toString(), {
    method: 'POST',
    credentials: 'same-origin',
    body: formData,
  })
  const payload = await response.json().catch(() => ({}))
  if (!response.ok || !payload.ok) {
    const error = new Error(payload?.error || 'Falha na requisicao da API')
    error.status = response.status
    throw error
  }
  return payload.data
}
