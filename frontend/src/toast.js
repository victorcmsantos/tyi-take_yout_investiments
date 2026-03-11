export const APP_TOAST_EVENT = 'tyi-app-toast'

export function emitAppToast(payload) {
  const detail = {
    severity: String(payload?.severity || 'info'),
    message: String(payload?.message || '').trim(),
    durationMs: Number(payload?.durationMs) || 4200,
  }
  if (!detail.message) return
  window.dispatchEvent(new CustomEvent(APP_TOAST_EVENT, { detail }))
}
