function StatePanel({
  eyebrow,
  title,
  description,
  actionLabel,
  onAction,
  secondaryActionLabel,
  onSecondaryAction,
  busy = false,
  compact = false,
  className = '',
}) {
  return (
    <div className={`state-panel${compact ? ' compact' : ''}${busy ? ' busy' : ''}${className ? ` ${className}` : ''}`}>
      <div className="state-panel-orb" aria-hidden="true">
        <span />
        <span />
      </div>
      <div className="state-panel-copy">
        {eyebrow ? <small className="state-panel-eyebrow">{eyebrow}</small> : null}
        <h3>{title}</h3>
        {description ? <p>{description}</p> : null}
      </div>
      {(actionLabel || secondaryActionLabel) ? (
        <div className="state-panel-actions">
          {actionLabel ? (
            <button type="button" className="btn-primary" onClick={onAction}>
              {actionLabel}
            </button>
          ) : null}
          {secondaryActionLabel ? (
            <button type="button" className="btn-secondary" onClick={onSecondaryAction}>
              {secondaryActionLabel}
            </button>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}

export default StatePanel
