import { useEffect, useMemo, useState } from 'react'
import { apiGet, apiPatch } from '../api'

function toNumberOrNull(value) {
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function ScannerMetricsLabPage() {
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const [catalog, setCatalog] = useState([])
  const [selectedMetricKey, setSelectedMetricKey] = useState('')
  const [search, setSearch] = useState('')
  const [parameterDraft, setParameterDraft] = useState({})

  const loadCatalog = async () => {
    setLoading(true)
    setError('')
    try {
      const payload = await apiGet('/api/scanner/metrics/catalog')
      const items = Array.isArray(payload) ? payload : []
      setCatalog(items)
      setSelectedMetricKey((current) => current || (items[0]?.key || ''))
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadCatalog()
  }, [])

  const visibleMetrics = useMemo(() => {
    const query = String(search || '').trim().toLowerCase()
    if (!query) return catalog
    return (catalog || []).filter((metric) => {
      const haystack = [
        metric?.label || '',
        metric?.key || '',
        metric?.details || '',
        metric?.formula || '',
      ].join(' ').toLowerCase()
      return haystack.includes(query)
    })
  }, [catalog, search])

  const selectedMetric = useMemo(
    () => visibleMetrics.find((metric) => metric.key === selectedMetricKey)
      || catalog.find((metric) => metric.key === selectedMetricKey)
      || null,
    [visibleMetrics, catalog, selectedMetricKey]
  )

  useEffect(() => {
    if (!selectedMetric) return
    const next = {}
    ;(selectedMetric.parameters || []).forEach((param) => {
      next[param.key] = String(param.value ?? '')
    })
    setParameterDraft(next)
  }, [selectedMetric?.key])

  useEffect(() => {
    if (!visibleMetrics.length) return
    if (!visibleMetrics.some((metric) => metric.key === selectedMetricKey)) {
      setSelectedMetricKey(visibleMetrics[0].key)
    }
  }, [visibleMetrics, selectedMetricKey])

  const onSaveMetric = async () => {
    if (!selectedMetric) return
    const parameters = {}
    for (const param of selectedMetric.parameters || []) {
      const raw = parameterDraft[param.key]
      const numeric = toNumberOrNull(raw)
      if (numeric == null) {
        setError(`Parametro invalido: ${param.label || param.key}`)
        return
      }
      parameters[param.key] = numeric
    }

    setSaving(true)
    setError('')
    setMessage('')
    try {
      const payload = await apiPatch(`/api/scanner/metrics/catalog/${encodeURIComponent(selectedMetric.key)}`, {
        parameters,
      })
      const updatedCatalog = Array.isArray(payload?.catalog) ? payload.catalog : catalog
      setCatalog(updatedCatalog)
      setSelectedMetricKey(String(payload?.metric_key || selectedMetric.key))
      const summary = payload?.scan_summary || {}
      setMessage(
        `Salvo. Processados ${Number(summary.tickers_processed || 0)} de ${Number(summary.tickers_loaded || 0)}; sinais ${Number(summary.signals_triggered || 0)}.`
      )
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  if (loading) return <p>Carregando metrics lab...</p>

  return (
    <section className="metrics-editor-page">
      <div className="hero-line">
        <div>
          <h1>Metrics Lab</h1>
          <p className="subtitle">Ajuste os parametros das metricas do Market Scanner e rode novo scan.</p>
        </div>
        <div className="hero-actions">
          <button type="button" className="btn-primary" onClick={loadCatalog} disabled={saving}>
            Atualizar catalogo
          </button>
        </div>
      </div>

      {!!message && <p className="notice-ok">{message}</p>}
      {!!error && <p className="notice-warn">{error}</p>}

      <div className="metrics-editor-layout">
        <aside className="metrics-editor-sidebar">
          <h3>Metricas ({visibleMetrics.length})</h3>
          <label className="auth-field scanner-lab-search">
            <span>Pesquisar</span>
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="RSI, momentum, breakout..."
            />
          </label>
          <div className="metrics-editor-menu">
            {(visibleMetrics || []).map((metric) => (
              <button
                key={metric.key}
                type="button"
                className={`metrics-editor-menu-item ${selectedMetricKey === metric.key ? 'active' : ''}`}
                onClick={() => setSelectedMetricKey(metric.key)}
              >
                <strong>{metric.label || metric.key}</strong>
                <small>{metric.key}</small>
              </button>
            ))}
          </div>
        </aside>

        <article className="metrics-editor-content card detail-card">
          {!selectedMetric ? (
            <p>Nenhuma metrica encontrada para o filtro atual.</p>
          ) : (
            <>
              <div className="hero-line">
                <div>
                  <h3>{selectedMetric.label || selectedMetric.key}</h3>
                  <p className="subtitle">{selectedMetric.details || 'Sem descricao.'}</p>
                </div>
                <span className="meta-chip">{selectedMetric.key}</span>
              </div>

              <div className="metrics-editor-help" style={{ marginTop: 12 }}>
                <p><strong>Formula:</strong> {selectedMetric.formula || '-'}</p>
              </div>

              {!Array.isArray(selectedMetric.parameters) || selectedMetric.parameters.length === 0 ? (
                <p style={{ marginTop: 12 }}>Esta metrica nao possui parametros editaveis.</p>
              ) : (
                <div className="scanner-lab-param-grid">
                  {selectedMetric.parameters.map((param) => (
                    <label className="auth-field scanner-lab-param-row" key={param.key}>
                      <span>{param.label || param.key}</span>
                      <input
                        type="number"
                        min={param.min}
                        max={param.max}
                        step={param.step}
                        value={parameterDraft[param.key] ?? ''}
                        onChange={(event) => setParameterDraft((current) => ({ ...current, [param.key]: event.target.value }))}
                      />
                      {!!param.description && <small>{param.description}</small>}
                    </label>
                  ))}
                </div>
              )}

              <div className="hero-actions" style={{ marginTop: 12 }}>
                <button
                  type="button"
                  className="btn-primary"
                  onClick={onSaveMetric}
                  disabled={saving || !selectedMetric || !selectedMetric.parameters?.length}
                >
                  {saving ? 'Salvando e recalculando...' : 'Salvar e rodar em todos os tickers'}
                </button>
              </div>
            </>
          )}
        </article>
      </div>
    </section>
  )
}

export default ScannerMetricsLabPage
