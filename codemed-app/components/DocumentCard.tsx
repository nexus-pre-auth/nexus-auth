'use client'

interface Document {
  id: string
  type: string
  title: string
  payer?: string
  drug?: string
  pdf_url?: string
  created_at: string
}

interface DocumentCardProps {
  document: Document
}

const TYPE_LABELS: Record<string, { label: string; color: string }> = {
  appeal_letter: { label: 'Appeal Letter', color: 'var(--green)' },
  pa_package: { label: 'PA Package', color: 'var(--purple)' },
  roi_report: { label: 'ROI Report', color: 'var(--gold)' },
}

export default function DocumentCard({ document: doc }: DocumentCardProps) {
  const typeInfo = TYPE_LABELS[doc.type] || { label: doc.type, color: 'var(--gray-light)' }
  const date = new Date(doc.created_at).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })

  return (
    <div className="card" style={{ padding: '20px 24px' }}>
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1 min-w-0">
          {/* Type badge */}
          <span
            className="inline-flex items-center gap-1.5 text-xs font-mono font-semibold px-2.5 py-1 rounded-full mb-3"
            style={{
              background: `${typeInfo.color}15`,
              border: `1px solid ${typeInfo.color}30`,
              color: typeInfo.color,
            }}
          >
            <svg width="8" height="8" viewBox="0 0 8 8" fill={typeInfo.color}>
              <circle cx="4" cy="4" r="4" />
            </svg>
            {typeInfo.label}
          </span>

          {/* Title */}
          <h3 className="font-semibold text-sm mb-2 truncate" style={{ color: 'var(--white)' }}>
            {doc.title}
          </h3>

          {/* Meta */}
          <div className="flex flex-wrap gap-x-4 gap-y-1">
            {doc.payer && (
              <span className="text-xs" style={{ color: 'var(--gray-light)' }}>
                <span style={{ color: 'var(--gray)' }}>Payer: </span>
                <strong>{doc.payer}</strong>
              </span>
            )}
            {doc.drug && (
              <span className="text-xs" style={{ color: 'var(--gray-light)' }}>
                <span style={{ color: 'var(--gray)' }}>Drug: </span>
                <strong>{doc.drug}</strong>
              </span>
            )}
            <span className="text-xs" style={{ color: 'var(--gray)' }}>
              {date}
            </span>
          </div>
        </div>

        {/* Open PDF */}
        {doc.pdf_url ? (
          <a
            href={doc.pdf_url}
            target="_blank"
            rel="noopener noreferrer"
            className="btn-ghost text-xs flex-shrink-0"
            style={{ padding: '6px 14px' }}
          >
            Open PDF →
          </a>
        ) : (
          <span
            className="text-xs flex-shrink-0 px-3 py-1.5 rounded-lg"
            style={{
              background: 'var(--surface)',
              border: '1px solid var(--border)',
              color: 'var(--gray)',
            }}
          >
            No PDF
          </span>
        )}
      </div>
    </div>
  )
}
