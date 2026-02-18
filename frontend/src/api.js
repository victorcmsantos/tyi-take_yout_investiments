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
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  })
  const payload = await response.json()
  if (!response.ok || !payload.ok) {
    throw new Error(payload?.error || 'Falha na requisicao da API')
  }
  return payload.data
}

export async function apiGet(path, params = {}) {
  return requestJson(path, { method: 'GET', params })
}

export async function apiPost(path, body = {}, params = {}) {
  return requestJson(path, { method: 'POST', params, body })
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
    body: formData,
  })
  const payload = await response.json()
  if (!response.ok || !payload.ok) {
    throw new Error(payload?.error || 'Falha na requisicao da API')
  }
  return payload.data
}
