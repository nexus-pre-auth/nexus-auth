import { redirect } from 'next/navigation'
import { createClient } from '@/lib/supabase-server'
import KnowledgeAdmin from '@/components/KnowledgeAdmin'
import Link from 'next/link'

export default async function AdminPage() {
  const supabase = createClient()
  const { data: { user } } = await supabase.auth.getUser()

  if (!user) {
    redirect('/auth')
  }

  // Check admin access
  const adminEmails = (process.env.ADMIN_EMAILS || '').split(',').map((e) => e.trim())
  if (!adminEmails.includes(user.email || '')) {
    redirect('/chat')
  }

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
      <span className="corner-watermark tl">ADMIN · RESTRICTED</span>

      {/* Header */}
      <header className="glass-header relative z-10" style={{ padding: '14px 24px' }}>
        <div
          style={{
            maxWidth: 1100,
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
              Knowledge Base Admin
            </h1>
            <span
              className="badge badge-red"
              style={{ fontSize: 10 }}
            >
              Admin Only
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <span style={{ fontSize: 13, color: 'var(--gray)' }}>{user.email}</span>
            <Link href="/chat" className="btn-ghost text-sm" style={{ padding: '6px 14px' }}>
              ← Chat
            </Link>
          </div>
        </div>
      </header>

      {/* Content */}
      <main
        style={{
          maxWidth: 1100,
          margin: '0 auto',
          padding: '40px 24px',
          position: 'relative',
          zIndex: 1,
        }}
      >
        <div style={{ marginBottom: 32 }}>
          <h2 style={{ fontSize: 28, fontWeight: 800, color: 'var(--white)', marginBottom: 8 }}>
            Knowledge Base
          </h2>
          <p style={{ color: 'var(--gray-light)', fontSize: 15 }}>
            Manage the knowledge injected into Claude&apos;s context. Entries are vector-embedded for semantic retrieval.
          </p>
        </div>

        <KnowledgeAdmin />
      </main>
    </div>
  )
}
