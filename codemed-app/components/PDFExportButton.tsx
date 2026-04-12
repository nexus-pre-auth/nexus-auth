'use client'

import { useState } from 'react'

interface PDFExportButtonProps {
  content: string
  payer?: string
  drug?: string
  sessionId?: string
}

export default function PDFExportButton({ content, payer, drug, sessionId }: PDFExportButtonProps) {
  const [loading, setLoading] = useState(false)
  const [url, setUrl] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function handleGenerate() {
    setLoading(true)
    setError(null)

    try {
      const res = await fetch('/api/documents/pdf', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          content,
          payer,
          drug,
          sessionId,
          title: `Appeal Letter — ${payer || 'Unknown Payer'}`,
        }),
      })

      const data = await res.json()

      if (data.url) {
        setUrl(data.url)
      } else {
        setError(data.error || 'Failed to generate PDF')
      }
    } catch {
      setError('Network error. Please try again.')
    }

    setLoading(false)
  }

  if (url) {
    return (
      <div
        className="flex items-center gap-3 rounded-xl px-4 py-3 fade-in"
        style={{
          background: 'var(--green-glow)',
          border: '1px solid var(--green-border)',
          maxWidth: '85%',
          marginTop: 8,
        }}
      >
        <svg width="18" height="18" fill="none" viewBox="0 0 24 24" stroke="var(--green)" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <div className="flex-1">
          <div className="text-sm font-semibold" style={{ color: 'var(--green)' }}>
            PDF Generated
          </div>
          <div className="text-xs" style={{ color: 'var(--gray-light)' }}>Saved to your documents</div>
        </div>
        <a
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          className="btn-primary text-xs"
          style={{ padding: '6px 14px' }}
        >
          Open PDF →
        </a>
      </div>
    )
  }

  return (
    <div className="mt-2 fade-in" style={{ maxWidth: '85%' }}>
      {error && (
        <div
          className="text-xs mb-2 px-3 py-2 rounded-lg"
          style={{
            background: 'rgba(252,90,90,0.08)',
            border: '1px solid rgba(252,90,90,0.2)',
            color: 'var(--red)',
          }}
        >
          {error}
        </div>
      )}
      <button
        onClick={handleGenerate}
        disabled={loading}
        className="flex items-center gap-2 rounded-xl px-4 py-3 text-sm font-semibold transition-all"
        style={{
          background: 'rgba(0,212,160,0.06)',
          border: '1px solid var(--green-border)',
          color: 'var(--green)',
          cursor: loading ? 'not-allowed' : 'pointer',
          opacity: loading ? 0.7 : 1,
        }}
      >
        {loading ? (
          <>
            <svg className="animate-spin" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
              <circle cx="12" cy="12" r="10" strokeOpacity={0.3} />
              <path d="M12 2a10 10 0 0 1 10 10" />
            </svg>
            Generating PDF…
          </>
        ) : (
          <>
            <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 10v6m0 0l-3-3m3 3l3-3M3 17V7a2 2 0 012-2h6l2 2h6a2 2 0 012 2v8a2 2 0 01-2 2H5a2 2 0 01-2-2z" />
            </svg>
            Export as PDF
          </>
        )}
      </button>
    </div>
  )
}
