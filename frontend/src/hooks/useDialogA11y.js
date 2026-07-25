import { useEffect, useRef } from 'react'

// Acessibilidade de dialog custom: ao abrir foca o primeiro campo, fecha no Esc
// e prende o foco (Tab/Shift+Tab ciclam dentro do dialog). Retorna a ref que deve
// ir no elemento role="dialog" (que precisa de tabIndex={-1}). O efeito depende so
// de `open` (onClose vem via ref) para nao roubar foco a cada re-render.
export function useDialogA11y(open, onClose) {
  const ref = useRef(null)
  const onCloseRef = useRef(onClose)
  onCloseRef.current = onClose

  useEffect(() => {
    if (!open) return undefined
    const node = ref.current
    if (!node) return undefined
    const previouslyFocused = document.activeElement

    const focusables = () => Array.from(
      node.querySelectorAll(
        'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
      ),
    ).filter((el) => el.offsetParent !== null)

    const first = focusables()[0]
    if (first) first.focus()
    else node.focus()

    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        event.stopPropagation()
        onCloseRef.current?.()
        return
      }
      if (event.key !== 'Tab') return
      const items = focusables()
      if (items.length === 0) return
      const firstEl = items[0]
      const lastEl = items[items.length - 1]
      if (event.shiftKey && document.activeElement === firstEl) {
        event.preventDefault()
        lastEl.focus()
      } else if (!event.shiftKey && document.activeElement === lastEl) {
        event.preventDefault()
        firstEl.focus()
      }
    }

    node.addEventListener('keydown', onKeyDown)
    return () => {
      node.removeEventListener('keydown', onKeyDown)
      if (previouslyFocused && typeof previouslyFocused.focus === 'function') {
        previouslyFocused.focus()
      }
    }
  }, [open])

  return ref
}
