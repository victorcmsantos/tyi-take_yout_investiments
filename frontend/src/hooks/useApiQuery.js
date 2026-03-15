import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { apiGet, apiGetCached } from '../api'

export function useApiQuery(path, {
  params = {},
  enabled = true,
  cached = false,
  cacheOptions = {},
  initialData = null,
  clearDataOnDisable = false,
} = {}) {
  const [data, setData] = useState(initialData)
  const [loading, setLoading] = useState(Boolean(enabled))
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')
  const [reloadToken, setReloadToken] = useState(0)
  const initialDataRef = useRef(initialData)
  const hasLoadedRef = useRef(initialData !== null && initialData !== undefined)
  const paramsKey = useMemo(() => JSON.stringify(params || {}), [params])
  const cacheOptionsKey = useMemo(() => JSON.stringify(cacheOptions || {}), [cacheOptions])

  useEffect(() => {
    if (!enabled) {
      setLoading(false)
      setRefreshing(false)
      setError('')
      if (clearDataOnDisable) {
        setData(initialDataRef.current)
        hasLoadedRef.current = initialDataRef.current !== null && initialDataRef.current !== undefined
      }
      return undefined
    }

    let active = true
    const shouldRefreshInPlace = hasLoadedRef.current
    setLoading(!shouldRefreshInPlace)
    setRefreshing(shouldRefreshInPlace)
    setError('')

    ;(async () => {
      try {
        const nextData = cached
          ? await apiGetCached(path, params, cacheOptions)
          : await apiGet(path, params)
        if (!active) return
        setData(nextData)
        hasLoadedRef.current = true
      } catch (err) {
        if (!active) return
        setError(err?.message || 'Falha ao carregar dados.')
      } finally {
        if (active) {
          setLoading(false)
          setRefreshing(false)
        }
      }
    })()

    return () => {
      active = false
    }
  }, [path, paramsKey, cacheOptionsKey, enabled, cached, reloadToken, clearDataOnDisable])

  const refetch = useCallback(() => {
    setReloadToken((current) => current + 1)
  }, [])

  return {
    data,
    setData,
    loading,
    refreshing,
    error,
    refetch,
  }
}
