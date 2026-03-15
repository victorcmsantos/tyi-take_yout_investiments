import { useEffect, useState } from 'react'
import { apiGet, apiGetCached, apiPost, clearApiCache } from '../api'

const EMPTY_UPCOMING = { items: [], summary: { estimated_totals: {} } }

async function loadSyncHealth() {
  const response = await fetch('/api/health', { credentials: 'same-origin' })
  const payload = await response.json().catch(() => ({}))
  if (payload?.ok && payload?.data) {
    return { data: payload.data, error: '' }
  }
  return { data: null, error: 'Saude indisponivel' }
}

export function useHomeDashboardData(selectedPortfolioIds) {
  const [assets, setAssets] = useState([])
  const [sectors, setSectors] = useState([])
  const [incomesByTicker, setIncomesByTicker] = useState({})
  const [incomesTotal, setIncomesTotal] = useState(0)
  const [upcomingIncomes, setUpcomingIncomes] = useState(EMPTY_UPCOMING)
  const [syncHealth, setSyncHealth] = useState(null)
  const [syncHealthError, setSyncHealthError] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [refreshingStaleAssets, setRefreshingStaleAssets] = useState(false)
  const portfolioKey = JSON.stringify(selectedPortfolioIds || [])

  useEffect(() => {
    let active = true
    setLoading(true)
    setError('')
    setSyncHealth(null)
    setSyncHealthError('')
    setUpcomingIncomes(EMPTY_UPCOMING)

    ;(async () => {
      try {
        const [assetsData, sectorsData, incomesData] = await Promise.all([
          apiGetCached('/api/assets', {}, { ttlMs: 15000, staleWhileRevalidate: true }),
          apiGetCached('/api/sectors', {}, { ttlMs: 20000, staleWhileRevalidate: true }),
          apiGetCached('/api/incomes', { portfolio_id: selectedPortfolioIds }, { ttlMs: 12000, staleWhileRevalidate: true }),
        ])
        if (!active) return

        const nextAssets = Array.isArray(assetsData) ? assetsData : []
        const nextSectors = Array.isArray(sectorsData) ? sectorsData : []
        const nextIncomes = Array.isArray(incomesData) ? incomesData : []
        const byTicker = nextIncomes.reduce((acc, income) => {
          const ticker = String(income?.ticker || '').toUpperCase()
          if (!ticker) return acc
          acc[ticker] = (acc[ticker] || 0) + Number(income?.amount || 0)
          return acc
        }, {})

        setAssets(nextAssets)
        setSectors(nextSectors)
        setIncomesByTicker(byTicker)
        setIncomesTotal(Object.values(byTicker).reduce((acc, value) => acc + Number(value || 0), 0))
        setLoading(false)

        const [upcomingResult, healthResult] = await Promise.allSettled([
          apiGetCached(
            '/api/incomes/upcoming',
            { portfolio_id: selectedPortfolioIds, limit: 24 },
            { ttlMs: 30000, staleWhileRevalidate: true },
          ),
          loadSyncHealth(),
        ])
        if (!active) return

        if (upcomingResult.status === 'fulfilled') {
          setUpcomingIncomes(upcomingResult.value || EMPTY_UPCOMING)
        } else {
          setUpcomingIncomes(EMPTY_UPCOMING)
        }

        if (healthResult.status === 'fulfilled') {
          setSyncHealth(healthResult.value.data)
          setSyncHealthError(healthResult.value.error)
        } else {
          setSyncHealth(null)
          setSyncHealthError('Saude indisponivel')
        }
      } catch (err) {
        if (!active) return
        setError(err?.message || 'Falha ao carregar dashboard.')
        setLoading(false)
      }
    })()

    return () => {
      active = false
    }
  }, [portfolioKey])

  const refreshOnlyStaleAssets = async () => {
    const staleTickers = assets
      .filter((asset) => asset?.market_data?.is_stale)
      .map((asset) => String(asset?.ticker || '').trim().toUpperCase())
      .filter(Boolean)

    if (refreshingStaleAssets || staleTickers.length === 0) {
      return { updatedCount: 0, failedCount: 0 }
    }

    setRefreshingStaleAssets(true)
    try {
      const result = await apiPost('/api/sync/market-data/stale', { attempts: 2 })
      clearApiCache('/api/assets')
      const refreshedAssets = await apiGet('/api/assets')
      const nextAssets = Array.isArray(refreshedAssets) ? refreshedAssets : []
      setAssets(nextAssets)
      return {
        updatedCount: Number(result?.updated_count || 0),
        failedCount: Number(result?.failed_count || 0),
      }
    } finally {
      setRefreshingStaleAssets(false)
    }
  }

  return {
    assets,
    sectors,
    incomesByTicker,
    incomesTotal,
    upcomingIncomes,
    syncHealth,
    syncHealthError,
    loading,
    error,
    refreshingStaleAssets,
    refreshOnlyStaleAssets,
  }
}
