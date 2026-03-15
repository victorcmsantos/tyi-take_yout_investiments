function DashboardCustomizerPanel({
  cardItems,
  hiddenCardKeys,
  onMoveCard,
  onToggleCardVisibility,
  sections,
  onToggleSection,
  onReset,
}) {
  return (
    <aside className="card dashboard-customizer">
      <div className="dashboard-customizer-head">
        <div>
          <small className="dashboard-customizer-eyebrow">Personalizacao</small>
          <h2>Monte sua dashboard</h2>
          <p>Reordene os cards principais e esconda blocos que nao fazem sentido para sua leitura diaria.</p>
        </div>
        <button type="button" className="btn-secondary" onClick={onReset}>
          Restaurar padrao
        </button>
      </div>

      <div className="dashboard-customizer-grid">
        <section className="dashboard-customizer-section">
          <h3>Cards de resumo</h3>
          <div className="dashboard-pref-list">
            {cardItems.map((item, idx) => {
              const hidden = hiddenCardKeys.includes(item.key)
              return (
                <div key={item.key} className={`dashboard-pref-row${hidden ? ' muted' : ''}`}>
                  <div>
                    <strong>{item.title}</strong>
                    <small>{hidden ? 'Oculto' : 'Visivel'}</small>
                  </div>
                  <div className="dashboard-pref-actions">
                    <button type="button" className="icon-btn" onClick={() => onMoveCard(item.key, -1)} disabled={idx === 0}>
                      Subir
                    </button>
                    <button type="button" className="icon-btn" onClick={() => onMoveCard(item.key, 1)} disabled={idx === cardItems.length - 1}>
                      Descer
                    </button>
                    <button type="button" className="icon-btn" onClick={() => onToggleCardVisibility(item.key)}>
                      {hidden ? 'Mostrar' : 'Ocultar'}
                    </button>
                  </div>
                </div>
              )
            })}
          </div>
        </section>

        <section className="dashboard-customizer-section">
          <h3>Blocos da pagina</h3>
          <div className="dashboard-pref-list">
            {sections.map((section) => (
              <div key={section.key} className={`dashboard-pref-row${section.enabled ? '' : ' muted'}`}>
                <div>
                  <strong>{section.label}</strong>
                  <small>{section.enabled ? 'Exibido' : 'Oculto'}</small>
                </div>
                <div className="dashboard-pref-actions">
                  <button type="button" className="icon-btn" onClick={() => onToggleSection(section.key)}>
                    {section.enabled ? 'Ocultar' : 'Mostrar'}
                  </button>
                </div>
              </div>
            ))}
          </div>
        </section>
      </div>
    </aside>
  )
}

export default DashboardCustomizerPanel
