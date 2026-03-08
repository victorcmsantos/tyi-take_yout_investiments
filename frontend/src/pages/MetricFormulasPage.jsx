import { useEffect, useMemo, useState } from 'react'
import { apiGet, apiPost } from '../api'

function MetricFormulasPage() {
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const [catalog, setCatalog] = useState({ metrics: [], allowed_variables: [], allowed_functions: [] })
  const [selectedMetricKey, setSelectedMetricKey] = useState('')
  const [formulaDraft, setFormulaDraft] = useState('')

  const selectedMetric = useMemo(
    () => (catalog.metrics || []).find((item) => item.key === selectedMetricKey) || null,
    [catalog.metrics, selectedMetricKey]
  )

  const loadCatalog = async () => {
    setLoading(true)
    setError('')
    try {
      const payload = await apiGet('/api/admin/metric-formulas')
      const nextCatalog = payload || { metrics: [], allowed_variables: [], allowed_functions: [] }
      setCatalog(nextCatalog)
      const firstKey = nextCatalog.metrics?.[0]?.key || ''
      setSelectedMetricKey((current) => current || firstKey)
      const selected = (nextCatalog.metrics || []).find((item) => item.key === (selectedMetricKey || firstKey))
      setFormulaDraft(String(selected?.formula || 'value'))
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadCatalog()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    if (!selectedMetric) return
    setFormulaDraft(String(selectedMetric.formula || 'value'))
  }, [selectedMetric?.key])

  const onSaveFormula = async () => {
    if (!selectedMetricKey) return
    setSaving(true)
    setError('')
    setMessage('')
    try {
      const payload = await apiPost(`/api/admin/metric-formulas/${selectedMetricKey}`, {
        formula: formulaDraft,
      })
      const nextCatalog = payload?.catalog || { metrics: [], allowed_variables: [], allowed_functions: [] }
      setCatalog(nextCatalog)
      const selected = (nextCatalog.metrics || []).find((item) => item.key === selectedMetricKey)
      setFormulaDraft(String(selected?.formula || 'value'))
      const updatedCount = Number(payload?.result?.recalculate?.updated_count || 0)
      setMessage(`${payload?.message || 'Formula salva.'} Tickers atualizados: ${updatedCount}.`)
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  if (loading) return <p>Carregando metricas...</p>
  if (error && !selectedMetric) return <p className="error">{error}</p>

  const hasUnsavedChanges = selectedMetric ? String(formulaDraft || '').trim() !== String(selectedMetric.formula || '').trim() : false

  return (
    <section className="metrics-editor-page">
      <div className="hero-line">
        <div>
          <h1>Editor de metricas</h1>
          <p className="subtitle">Edite a formula de cada metrica e aplique em todos os tickers com um clique.</p>
        </div>
      </div>

      {!!message && <p className="notice-ok">{message}</p>}
      {!!error && <p className="notice-warn">{error}</p>}

      <div className="metrics-editor-layout">
        <aside className="metrics-editor-sidebar">
          <h3>Metricas</h3>
          <div className="metrics-editor-menu">
            {(catalog.metrics || []).map((metric) => (
              <button
                key={metric.key}
                type="button"
                className={`metrics-editor-menu-item ${selectedMetricKey === metric.key ? 'active' : ''}`}
                onClick={() => setSelectedMetricKey(metric.key)}
              >
                <strong>{metric.title}</strong>
                <small>{metric.key}</small>
              </button>
            ))}
          </div>
        </aside>

        <article className="metrics-editor-content card detail-card">
          {!selectedMetric ? (
            <p>Selecione uma metrica no menu lateral.</p>
          ) : (
            <>
              <div className="hero-line">
                <div>
                  <h3>{selectedMetric.title}</h3>
                  <p className="subtitle">{selectedMetric.description}</p>
                </div>
                <span className="meta-chip">{selectedMetric.key}</span>
              </div>

              <label className="auth-field" style={{ marginTop: 12 }}>
                <span>Formula</span>
                <textarea
                  value={formulaDraft}
                  onChange={(event) => setFormulaDraft(event.target.value)}
                  rows={5}
                  placeholder="Ex: value * 1.02"
                  className="metrics-editor-textarea"
                />
              </label>

              <div className="metrics-editor-help">
                <p><strong>Detalhes:</strong> use `value` para o valor base da metrica selecionada.</p>
                <p><strong>Variaveis:</strong> {(catalog.allowed_variables || []).join(', ')}</p>
                <p><strong>Funcoes:</strong> {(catalog.allowed_functions || []).join(', ')}</p>
                <p><strong>Exemplo:</strong> `value * 0.98` ou `round(max(value, 0), 2)`</p>
              </div>

              <div className="hero-actions">
                <button
                  type="button"
                  className="btn-primary"
                  onClick={onSaveFormula}
                  disabled={saving || !selectedMetricKey || !String(formulaDraft || '').trim()}
                >
                  {saving ? 'Salvando e aplicando...' : 'Salvar e aplicar em todos os tickers'}
                </button>
                {hasUnsavedChanges && <span className="subtitle">Alteracoes pendentes</span>}
              </div>
            </>
          )}
        </article>
      </div>
    </section>
  )
}

export default MetricFormulasPage
