import AuthForm from '@/components/AuthForm'

export default function AuthPage() {
  return (
    <div
      style={{
        minHeight: '100vh',
        background: 'var(--bg)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: 20,
        position: 'relative',
      }}
    >
      {/* Background */}
      <div className="bg-grid" />
      <div className="orb-tl" />
      <div className="orb-br" />
      <div className="ghost-text">CODEMED</div>
      <span className="corner-watermark tl">ICD-10-CM · CPT-4</span>
      <span className="corner-watermark br">HIPAA · SOC 2</span>

      {/* Card */}
      <div
        className="card card-accent slide-up"
        style={{ width: '100%', maxWidth: 440, padding: 40, position: 'relative', zIndex: 1 }}
      >
        {/* Logo */}
        <div style={{ textAlign: 'center', marginBottom: 32 }}>
          <div
            style={{
              width: 48,
              height: 48,
              borderRadius: 14,
              background: 'linear-gradient(135deg, #00D4A0, #00B386)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              fontWeight: 900,
              fontSize: 22,
              color: '#0A0F1A',
              margin: '0 auto 16px',
            }}
          >
            C
          </div>
          <h1
            style={{
              fontSize: 24,
              fontWeight: 800,
              color: 'var(--white)',
              letterSpacing: '-0.03em',
              marginBottom: 6,
            }}
          >
            CodeMed
          </h1>
          <p style={{ color: 'var(--gray)', fontSize: 13 }}>
            AI Revenue Cycle Intelligence for Healthcare
          </p>
        </div>

        {/* Auth form */}
        <AuthForm />

        {/* Stats row */}
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(3, 1fr)',
            gap: 12,
            marginTop: 32,
            paddingTop: 24,
            borderTop: '1px solid var(--border)',
          }}
        >
          {[
            { value: '94%', label: 'PA approval rate' },
            { value: '6.2h', label: 'PA turnaround' },
            { value: '$206K', label: 'Monthly protected' },
          ].map((stat) => (
            <div key={stat.label} className="stat-item">
              <span className="stat-value" style={{ fontSize: 16 }}>{stat.value}</span>
              <span className="stat-label">{stat.label}</span>
            </div>
          ))}
        </div>

        <p
          style={{
            textAlign: 'center',
            marginTop: 16,
            fontSize: 11,
            color: 'var(--gray)',
            fontFamily: 'IBM Plex Mono, monospace',
          }}
        >
          HIPAA-compliant · No PHI stored · SOC 2 aligned
        </p>
      </div>
    </div>
  )
}
