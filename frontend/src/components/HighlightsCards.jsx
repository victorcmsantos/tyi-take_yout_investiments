import StatePanel from './StatePanel'

function HighlightsCards({ cards, onResetHiddenCards }) {
  if (!cards.length) {
    return (
      <StatePanel
        eyebrow="Resumo"
        title="Todos os cards principais estao ocultos"
        description="Reabra a personalizacao da dashboard para escolher quais indicadores devem aparecer aqui."
        actionLabel="Restaurar cards"
        onAction={onResetHiddenCards}
      />
    )
  }

  return (
      <div className="cards">
        {cards.map((card) => (
          <article key={card.key} className={`card dashboard-card dashboard-card-${card.key}`}>
            <small className="dashboard-card-kicker">Resumo</small>
            <h3>{card.title}</h3>
            <p className={card.valueClassName || ''}>{card.value}</p>
            {card.caption ? <small>{card.caption}</small> : null}
          {Array.isArray(card.metaLines) && card.metaLines.length > 0 ? (
            <div className="card-health-lines">
              {card.metaLines.map((line) => <small key={`${card.key}-${line}`}>{line}</small>)}
            </div>
          ) : null}
          {card.actionLabel ? (
            <button type="button" className="btn-primary health-details-trigger" onClick={card.onAction}>
              {card.actionLabel}
            </button>
          ) : null}
        </article>
      ))}
    </div>
  )
}

export default HighlightsCards
