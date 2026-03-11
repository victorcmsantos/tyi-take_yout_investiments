import { useEffect, useState } from 'react'

export function usePersistedState(key, initialValue) {
  const [state, setState] = useState(() => {
    try {
      const raw = localStorage.getItem(key)
      if (!raw) return initialValue
      return JSON.parse(raw)
    } catch (_err) {
      return initialValue
    }
  })

  useEffect(() => {
    try {
      localStorage.setItem(key, JSON.stringify(state))
    } catch (_err) {
      // ignore quota errors
    }
  }, [key, state])

  return [state, setState]
}
