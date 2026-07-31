// Heurística de leitura tática de uma posição, combinando a leitura do OpenClaw
// (humor/ação sugerida) com preço atual vs. preço médio e o PnL em aberto.
// Extraído de pages/AssetPage.jsx para manter a view enxuta e a regra testável.

function normalizeSearchText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
}

function inferMarketMoodLabel(value) {
  const text = normalizeSearchText(value)
  if (!text) {
    return { label: 'Sem leitura', tone: 'neutral', score: 0 }
  }

  let score = 0
  const positiveMarkers = [
    'positivo',
    'otimista',
    'confianca',
    'resiliente',
    'solido',
    'barato',
    'atrativo',
    'desconto',
    'bom momento',
    'favoravel',
    'crescimento',
    'melhora',
    'forte',
  ]
  const negativeMarkers = [
    'negativo',
    'cautela',
    'pressionado',
    'caro',
    'esticado',
    'risco',
    'incerteza',
    'fraco',
    'desaceleracao',
    'desaceleração',
    'volatil',
    'volatilidade',
    'piora',
    'desafiador',
  ]

  positiveMarkers.forEach((marker) => {
    if (text.includes(marker)) score += 1
  })
  negativeMarkers.forEach((marker) => {
    if (text.includes(marker)) score -= 1
  })

  if (score >= 2) {
    return { label: 'Mercado com viés positivo', tone: 'positive', score }
  }
  if (score <= -2) {
    return { label: 'Mercado com viés cauteloso', tone: 'negative', score }
  }
  return { label: 'Mercado sem direção forte', tone: 'neutral', score }
}

export function normalizeStructuredMarketMood(value) {
  const text = normalizeSearchText(value)
  if (!text) return ''
  if (text.includes('positivo') || text.includes('favoravel') || text.includes('otimista')) return 'positive'
  if (text.includes('cauteloso') || text.includes('negativo') || text.includes('pessimista')) return 'negative'
  if (text.includes('neutro')) return 'neutral'
  return ''
}

function marketMoodPresentation(structuredMood, marketView) {
  const normalized = normalizeStructuredMarketMood(structuredMood)
  if (normalized === 'positive') {
    return { label: 'Mercado com viés positivo', tone: 'positive', score: 2 }
  }
  if (normalized === 'negative') {
    return { label: 'Mercado com viés cauteloso', tone: 'negative', score: -2 }
  }
  if (normalized === 'neutral') {
    return { label: 'Mercado sem direção forte', tone: 'neutral', score: 0 }
  }
  return inferMarketMoodLabel(marketView)
}

function normalizeStructuredAction(value) {
  const text = normalizeSearchText(value)
  if (!text) return ''
  if (text.includes('comprar_mais') || text.includes('comprar mais') || text === 'compra') return 'buy_more'
  if (text.includes('segurar') || text.includes('manter')) return 'hold'
  if (text.includes('reduzir') || text.includes('vender')) return 'reduce'
  if (text.includes('observar') || text.includes('aguardar') || text.includes('monitorar')) return 'watch'
  return ''
}

export function structuredActionLabel(value) {
  const normalized = normalizeStructuredAction(value)
  if (normalized === 'buy_more') return 'Comprar mais'
  if (normalized === 'hold') return 'Segurar'
  if (normalized === 'reduce') return 'Vender ou reduzir'
  if (normalized === 'watch') return 'Observar'
  return ''
}

export function buildPositionDecision({ asset, position, enrichmentPayload }) {
  const currentPrice = Number(asset?.price || 0)
  const avgPrice = Number(position?.avg_price || 0)
  const shares = Number(position?.shares || 0)
  const openPnlPct = Number(position?.open_pnl_pct || 0)
  const marketView = String(enrichmentPayload?.visao_do_mercado || '').trim()
  const structuredMood = String(enrichmentPayload?.humor_do_mercado || '').trim()
  const openClawAction = String(enrichmentPayload?.acao_sugerida || '').trim()
  const openClawActionWhy = String(enrichmentPayload?.justificativa_da_acao || '').trim()
  const mood = marketMoodPresentation(structuredMood, marketView)
  const priceGapPct = avgPrice > 0 ? ((currentPrice - avgPrice) / avgPrice) * 100 : null
  const reasons = []

  if (structuredMood) {
    reasons.push(`OpenClaw classificou o humor como ${structuredMood}`)
  } else if (marketView) {
    reasons.push(mood.label)
  } else {
    reasons.push('OpenClaw ainda não trouxe leitura de mercado')
  }

  if (openClawAction) {
    reasons.push(`OpenClaw sugeriu ${structuredActionLabel(openClawAction) || openClawAction}`)
  }

  if (avgPrice > 0 && priceGapPct !== null) {
    const direction = priceGapPct >= 0 ? 'acima' : 'abaixo'
    reasons.push(`Preço atual ${Math.abs(priceGapPct).toFixed(2)}% ${direction} do seu preço médio`)
  }

  if (shares > 0) {
    const pnlDirection = openPnlPct >= 0 ? 'acima' : 'abaixo'
    reasons.push(`Posição ${Math.abs(openPnlPct).toFixed(2)}% ${pnlDirection} do zero`)
  }

  if (shares <= 0 || avgPrice <= 0) {
    if (mood.tone === 'positive') {
      return {
        mood,
        action: 'Observar compra',
        actionTone: 'up',
        openClawActionLabel: structuredActionLabel(openClawAction),
        openClawActionWhy,
        reasons,
        priceGapPct,
        summary: 'O mercado parece construtivo, mas você ainda não tem preço médio relevante nessa posição.',
      }
    }
    if (mood.tone === 'negative') {
      return {
        mood,
        action: 'Aguardar',
        actionTone: 'down',
        openClawActionLabel: structuredActionLabel(openClawAction),
        openClawActionWhy,
        reasons,
        priceGapPct,
        summary: 'A leitura atual não sugere pressa para montar ou aumentar posição.',
      }
    }
    return {
      mood,
      action: 'Monitorar',
      actionTone: '',
      openClawActionLabel: structuredActionLabel(openClawAction),
      openClawActionWhy,
      reasons,
      priceGapPct,
      summary: 'Sem uma posição formada, faz mais sentido monitorar antes de agir.',
    }
  }

  if (mood.tone === 'positive') {
    if (priceGapPct <= -7) {
      return {
        mood,
        action: 'Comprar mais',
        actionTone: 'up',
        openClawActionLabel: structuredActionLabel(openClawAction),
        openClawActionWhy,
        reasons,
        priceGapPct,
        summary: 'O mercado segue favorável e o preço está abaixo do seu custo médio.',
      }
    }
    return {
      mood,
      action: 'Segurar',
      actionTone: '',
      openClawActionLabel: structuredActionLabel(openClawAction),
      openClawActionWhy,
      reasons,
      priceGapPct,
      summary: 'A leitura segue boa, mas o preço já não oferece desconto claro contra o seu custo médio.',
    }
  }

  if (mood.tone === 'negative') {
    if (priceGapPct >= 8 || openPnlPct >= 10) {
      return {
        mood,
        action: 'Vender ou reduzir',
        actionTone: 'down',
        openClawActionLabel: structuredActionLabel(openClawAction),
        openClawActionWhy,
        reasons,
        priceGapPct,
        summary: 'O humor do mercado piorou e a posição ainda tem gordura para realizar ou reduzir risco.',
      }
    }
    return {
      mood,
      action: 'Segurar',
      actionTone: '',
      openClawActionLabel: structuredActionLabel(openClawAction),
      openClawActionWhy,
      reasons,
      priceGapPct,
      summary: 'A leitura está mais cautelosa, mas o preço não abre uma saída tão confortável agora.',
    }
  }

  if (priceGapPct <= -10) {
    return {
      mood,
      action: 'Segurar',
      actionTone: '',
      openClawActionLabel: structuredActionLabel(openClawAction),
      openClawActionWhy,
      reasons,
      priceGapPct,
      summary: 'O preço caiu abaixo do seu médio, mas sem melhora clara no humor do mercado ainda faz sentido evitar aumentar no escuro.',
    }
  }

  if (priceGapPct >= 12 && openPnlPct > 0) {
    return {
      mood,
      action: 'Segurar',
      actionTone: '',
      openClawActionLabel: structuredActionLabel(openClawAction),
      openClawActionWhy,
      reasons,
      priceGapPct,
      summary: 'A posição está andando bem, mas sem sinal forte do mercado a leitura segue de manutenção.',
    }
  }

  return {
    mood,
    action: 'Segurar',
    actionTone: '',
    openClawActionLabel: structuredActionLabel(openClawAction),
    openClawActionWhy,
    reasons,
    priceGapPct,
    summary: 'Não há sinal forte o bastante para aumentar ou reduzir agora.',
  }
}
