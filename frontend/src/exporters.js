function csvCell(value) {
  const text = String(value ?? '')
  return `"${text.replace(/"/g, '""')}"`
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

function normalizeFilenameSegment(value) {
  return String(value || 'export')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
}

export function downloadTextFile(filename, content, mimeType = 'text/plain;charset=utf-8') {
  const blob = new Blob([content], { type: mimeType })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  document.body.appendChild(anchor)
  anchor.click()
  anchor.remove()
  URL.revokeObjectURL(url)
}

export function exportRowsAsCsv(filename, headers, rows) {
  const csvLines = [
    headers.map(csvCell).join(','),
    ...rows.map((row) => row.map(csvCell).join(',')),
  ]
  downloadTextFile(filename, csvLines.join('\n'), 'text/csv;charset=utf-8')
}

export function buildExportFilename(prefix, label, extension) {
  return `${normalizeFilenameSegment(prefix)}-${normalizeFilenameSegment(label)}.${extension}`
}

export function exportTableAsPdf({
  documentTitle,
  sectionTitle,
  subtitle = '',
  summary = [],
  headers = [],
  rows = [],
}) {
  const printWindow = window.open('', '_blank')
  if (!printWindow) return false

  const safeTitle = escapeHtml(documentTitle || sectionTitle || 'Exportacao')
  const safeSectionTitle = escapeHtml(sectionTitle || documentTitle || 'Exportacao')
  const safeSubtitle = escapeHtml(subtitle)
  const summaryHtml = summary.length > 0
    ? `
      <section class="summary-grid">
        ${summary.map((item) => `
          <article class="summary-card">
            <span>${escapeHtml(item.label)}</span>
            <strong>${escapeHtml(item.value)}</strong>
          </article>
        `).join('')}
      </section>
    `
    : ''
  const tableHeadHtml = headers.map((header) => `<th>${escapeHtml(header)}</th>`).join('')
  const tableRowsHtml = rows.length > 0
    ? rows.map((row) => `
        <tr>${row.map((cell) => `<td>${escapeHtml(cell)}</td>`).join('')}</tr>
      `).join('')
    : `<tr><td colspan="${Math.max(headers.length, 1)}">Sem dados para exportar.</td></tr>`

  printWindow.document.open()
  printWindow.document.write(`
    <!doctype html>
    <html lang="pt-BR">
      <head>
        <meta charset="utf-8" />
        <title>${safeTitle}</title>
        <style>
          :root {
            color-scheme: light;
            font-family: Arial, sans-serif;
            color: #162334;
            background: #ffffff;
          }

          * {
            box-sizing: border-box;
          }

          body {
            margin: 0;
            padding: 24px;
          }

          h1 {
            margin: 0 0 6px;
            font-size: 24px;
          }

          p {
            margin: 0 0 18px;
            color: #506176;
          }

          .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
            margin-bottom: 18px;
          }

          .summary-card {
            border: 1px solid #d6e1ec;
            border-radius: 12px;
            padding: 12px;
            background: #f8fbff;
          }

          .summary-card span {
            display: block;
            margin-bottom: 6px;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #5a6b7f;
          }

          .summary-card strong {
            font-size: 18px;
          }

          table {
            width: 100%;
            border-collapse: collapse;
            table-layout: auto;
          }

          th,
          td {
            border: 1px solid #d6e1ec;
            padding: 8px 10px;
            text-align: left;
            font-size: 12px;
            vertical-align: top;
          }

          th {
            background: #eef5fb;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.04em;
          }

          @media print {
            body {
              padding: 0;
            }
          }
        </style>
      </head>
      <body>
        <h1>${safeSectionTitle}</h1>
        ${safeSubtitle ? `<p>${safeSubtitle}</p>` : ''}
        ${summaryHtml}
        <table>
          <thead>
            <tr>${tableHeadHtml}</tr>
          </thead>
          <tbody>
            ${tableRowsHtml}
          </tbody>
        </table>
      </body>
    </html>
  `)
  printWindow.document.close()

  const triggerPrint = () => {
    printWindow.focus()
    printWindow.print()
  }

  if (printWindow.document.readyState === 'complete') {
    window.setTimeout(triggerPrint, 150)
  } else {
    printWindow.addEventListener('load', () => window.setTimeout(triggerPrint, 150), { once: true })
  }

  return true
}
