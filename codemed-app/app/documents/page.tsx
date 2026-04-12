import { redirect } from 'next/navigation'
import { createClient } from '@/lib/supabase-server'
import { createServiceClient } from '@/lib/supabase-server'
import DocumentCard from '@/components/DocumentCard'
import Link from 'next/link'

export default async function DocumentsPage() {
  const supabase = createClient()
  const { data: { user } } = await supabase.auth.getUser()

  if (!user) {
    redirect('/auth')
  }

  const serviceSupabase = createServiceClient()

  const { data: documents } = await serviceSupabase
    .from('documents')
    .select('id, type, title, payer, drug, pdf_url, created_at')
    .eq('user_id', user.id)
    .order('created_at', { ascending: false })

  return (
    <div
      style={{
        minHeight: '100vh',
        background: 'var(--bg)',
        position: 'relative',
      }}
    >
      {/* Background */}
      <div className="bg-grid" />
      <div className="orb-tl" />
      <div className="ghost-text">CODEMED</div>
      <span className="corner-watermark tl">ICD-10-CM · CPT-4</span>
      <span className="corner-watermark br">HIPAA · SOC 2</span>

      {/* Header */}
      <header className="glass-header relative z-10" style={{ padding: '14px 24px' }}>
        <div
          style={{
            maxWidth: 960,
            margin: '0 auto',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
            <Link href="/chat" style={{ display: 'flex', alignItems: 'center', gap: 8, textDecoration: 'none' }}>
              <div
                style={{
                  width: 26,
                  height: 26,
                  borderRadius: 7,
                  background: 'linear-gradient(135deg, #00D4A0, #00B386)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontWeight: 900,
                  fontSize: 13,
                  color: '#0A0F1A',
                }}
              >
                C
              </div>
              <span style={{ fontWeight: 800, color: 'var(--white)', fontSize: 15 }}>CodeMed</span>
            </Link>

            <div style={{ height: 16, width: 1, background: 'var(--border)' }} />
            <h1 style={{ fontSize: 15, fontWeight: 600, color: 'var(--gray-light)' }}>
              Documents
            </h1>
          </div>

          <Link href="/chat" className="btn-ghost text-sm" style={{ padding: '6px 14px' }}>
            ← Back to Chat
          </Link>
        </div>
      </header>

      {/* Content */}
      <main
        style={{
          maxWidth: 960,
          margin: '0 auto',
          padding: '40px 24px',
          position: 'relative',
          zIndex: 1,
        }}
      >
        <div style={{ marginBottom: 32 }}>
          <h2 style={{ fontSize: 28, fontWeight: 800, color: 'var(--white)', marginBottom: 8 }}>
            Saved Documents
          </h2>
          <p style={{ color: 'var(--gray-light)', fontSize: 15 }}>
            Appeal letters, PA packages, and ROI reports generated in your sessions.
          </p>
        </div>

        {!documents || documents.length === 0 ? (
          <div
            className="card text-center"
            style={{ padding: '60px 40px' }}
          >
            <div
              style={{
                width: 56,
                height: 56,
                borderRadius: 14,
                background: 'var(--surface)',
                border: '1px solid var(--border)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                margin: '0 auto 20px',
              }}
            >
              <svg width="24" height="24" fill="none" viewBox="0 0 24 24" stroke="var(--gray)" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
            </div>
            <h3 style={{ fontSize: 18, fontWeight: 700, color: 'var(--white)', marginBottom: 8 }}>
              No documents yet
            </h3>
            <p style={{ color: 'var(--gray-light)', fontSize: 14, maxWidth: 360, margin: '0 auto 24px' }}>
              Generate an appeal letter or PA package in the chat to save it here.
            </p>
            <Link href="/chat" className="btn-primary" style={{ display: 'inline-flex' }}>
              Go to Chat →
            </Link>
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {documents.map((doc) => (
              <DocumentCard key={doc.id} document={doc} />
            ))}
          </div>
        )}
      </main>
    </div>
  )
}
