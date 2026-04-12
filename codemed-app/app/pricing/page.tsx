'use client'

import { useState } from 'react'
import Link from 'next/link'

const TIERS = [
  {
    key: 'starter',
    label: 'Starter',
    price: '$2,500',
    period: '/mo',
    users: '3 users',
    locations: '1 location',
    description: 'For independent practices ready to modernize their RCM workflow.',
    features: [
      'Unlimited PA queries',
      'Denial analysis + appeal letter generation',
      'Basic payer intelligence',
      'ICD-10 & CPT coding guidance',
      'Email support',
    ],
    cta: 'Start 14-Day Free Trial',
    featured: false,
  },
  {
    key: 'growth',
    label: 'Growth',
    price: '$5,000',
    period: '/mo',
    users: '10 users',
    locations: '3 locations',
    description: 'For multi-provider groups scaling their revenue cycle operations.',
    features: [
      'Everything in Starter',
      'AR acceleration workflows',
      'Advanced payer matching',
      'Monthly ROI report',
      'CARC/RARC analysis dashboard',
      'Priority support',
    ],
    cta: 'Start 14-Day Free Trial',
    featured: true,
  },
  {
    key: 'enterprise',
    label: 'Enterprise',
    price: '$10,000',
    period: '/mo',
    users: 'Unlimited users',
    locations: 'Unlimited locations',
    description: 'For health systems and PE-backed groups demanding full RCM automation.',
    features: [
      'Everything in Growth',
      'Full NexusAuth S1–S11 stack',
      'Custom payer rule configuration',
      'Dedicated account manager',
      'SLA guarantee',
      'EHR integration support (Epic, Modernizing Medicine)',
      'White-glove onboarding',
    ],
    cta: 'Start 14-Day Free Trial',
    featured: false,
  },
]

export default function PricingPage() {
  const [claimVolume, setClaimVolume] = useState(500)
  const [avgClaimValue, setAvgClaimValue] = useState(350)
  const [loading, setLoading] = useState<string | null>(null)

  const recoverable = Math.round(claimVolume * avgClaimValue * 0.23 * 0.68 * 12)
  const formatted = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(recoverable)

  async function handleCheckout(tier: string) {
    setLoading(tier)
    try {
      const res = await fetch('/api/stripe/checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tier }),
      })
      const { url, error } = await res.json()
      if (url) {
        window.location.href = url
      } else {
        console.error(error)
        setLoading(null)
      }
    } catch {
      setLoading(null)
    }
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
      <div className="orb-br" />
      <div className="ghost-text">CODEMED</div>
      <span className="corner-watermark tl">ICD-10-CM · CPT-4</span>
      <span className="corner-watermark br">HIPAA · SOC 2</span>

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
          <Link href="/" style={{ display: 'flex', alignItems: 'center', gap: 8, textDecoration: 'none' }}>
            <div
              style={{
                width: 28,
                height: 28,
                borderRadius: 8,
                background: 'linear-gradient(135deg, #00D4A0, #00B386)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontWeight: 900,
                fontSize: 14,
                color: '#0A0F1A',
              }}
            >
              C
            </div>
            <span style={{ fontWeight: 800, color: 'var(--white)', fontSize: 16 }}>CodeMed</span>
          </Link>

          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <Link href="/auth" className="btn-ghost text-sm" style={{ padding: '7px 18px' }}>
              Sign In
            </Link>
            <Link
              href="/auth"
              className="btn-primary text-sm"
              style={{ padding: '7px 18px', textDecoration: 'none' }}
            >
              Get Started Free
            </Link>
          </div>
        </div>
      </header>

      <main style={{ position: 'relative', zIndex: 1 }}>
        {/* Founding Member Banner */}
        <div
          style={{
            background: 'linear-gradient(135deg, rgba(246,173,60,0.08), rgba(246,173,60,0.04))',
            borderBottom: '1px solid rgba(246,173,60,0.15)',
            padding: '12px 24px',
            textAlign: 'center',
          }}
        >
          <span style={{ color: 'var(--gold)', fontSize: 14, fontWeight: 600 }}>
            🏆 Founding Member Offer — First 5 customers get{' '}
            <strong>40% off locked forever.</strong> Only 2 spots remaining.
          </span>
        </div>

        {/* Hero */}
        <section style={{ padding: '80px 24px 60px', textAlign: 'center' }}>
          <div style={{ maxWidth: 680, margin: '0 auto' }}>
            <div
              className="badge badge-green inline-flex"
              style={{ marginBottom: 20, padding: '6px 14px', fontSize: 12 }}
            >
              <div className="ai-live-dot" />
              AI Revenue Cycle Intelligence
            </div>

            <h1
              style={{
                fontSize: 'clamp(36px, 5vw, 56px)',
                fontWeight: 900,
                color: 'var(--white)',
                lineHeight: 1.1,
                letterSpacing: '-0.03em',
                marginBottom: 20,
              }}
            >
              Stop leaving revenue
              <br />
              <span className="text-gradient">on the table.</span>
            </h1>

            <p
              style={{
                fontSize: 18,
                color: 'var(--gray-light)',
                lineHeight: 1.6,
                marginBottom: 40,
              }}
            >
              CodeMed replaces manual billing work with AI-powered prior auth, denial management, and payer intelligence. Most practices recover $150K–$400K in their first 90 days.
            </p>

            {/* Stats */}
            <div
              style={{
                display: 'flex',
                justifyContent: 'center',
                gap: 0,
                background: 'var(--card)',
                border: '1px solid var(--border)',
                borderRadius: 14,
                padding: '20px 0',
                maxWidth: 480,
                margin: '0 auto',
              }}
            >
              {[
                { value: '94%', label: 'PA approval rate' },
                { value: '6.2h', label: 'PA turnaround' },
                { value: '$206K', label: 'Monthly protected' },
              ].map((stat, i) => (
                <div
                  key={stat.label}
                  className="stat-item"
                  style={{
                    flex: 1,
                    borderRight: i < 2 ? '1px solid var(--border)' : 'none',
                  }}
                >
                  <span className="stat-value" style={{ fontSize: 22 }}>{stat.value}</span>
                  <span className="stat-label">{stat.label}</span>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* Pricing Cards */}
        <section style={{ padding: '0 24px 80px' }}>
          <div
            style={{
              maxWidth: 1100,
              margin: '0 auto',
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
              gap: 24,
            }}
          >
            {TIERS.map((tier) => (
              <div key={tier.key} className={`pricing-card${tier.featured ? ' featured' : ''}`}>
                {tier.featured && (
                  <div
                    style={{
                      position: 'absolute',
                      top: 16,
                      right: 16,
                      background: 'var(--green)',
                      color: '#0A0F1A',
                      fontSize: 11,
                      fontWeight: 700,
                      padding: '3px 10px',
                      borderRadius: 999,
                      letterSpacing: '0.05em',
                    }}
                  >
                    MOST POPULAR
                  </div>
                )}

                {/* Tier header */}
                <div style={{ marginBottom: 24 }}>
                  <div
                    style={{
                      fontSize: 12,
                      fontFamily: 'IBM Plex Mono, monospace',
                      color: tier.featured ? 'var(--green)' : 'var(--gray)',
                      letterSpacing: '0.1em',
                      marginBottom: 8,
                      textTransform: 'uppercase',
                    }}
                  >
                    {tier.label}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: 4, marginBottom: 8 }}>
                    <span
                      style={{
                        fontSize: 36,
                        fontWeight: 900,
                        color: 'var(--white)',
                        letterSpacing: '-0.03em',
                      }}
                    >
                      {tier.price}
                    </span>
                    <span style={{ color: 'var(--gray)', fontSize: 14 }}>{tier.period}</span>
                  </div>
                  <div style={{ display: 'flex', gap: 12, marginBottom: 12 }}>
                    <span
                      className="badge badge-gray"
                      style={{ fontSize: 11 }}
                    >
                      {tier.users}
                    </span>
                    <span
                      className="badge badge-gray"
                      style={{ fontSize: 11 }}
                    >
                      {tier.locations}
                    </span>
                  </div>
                  <p style={{ fontSize: 13, color: 'var(--gray-light)', lineHeight: 1.5 }}>
                    {tier.description}
                  </p>
                </div>

                {/* Features */}
                <div style={{ marginBottom: 28 }}>
                  {tier.features.map((feature) => (
                    <div
                      key={feature}
                      style={{
                        display: 'flex',
                        alignItems: 'flex-start',
                        gap: 10,
                        marginBottom: 10,
                      }}
                    >
                      <svg
                        width="16"
                        height="16"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="var(--green)"
                        strokeWidth={2.5}
                        style={{ flexShrink: 0, marginTop: 1 }}
                      >
                        <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                      </svg>
                      <span style={{ fontSize: 14, color: 'var(--gray-light)', lineHeight: 1.4 }}>
                        {feature}
                      </span>
                    </div>
                  ))}
                </div>

                <button
                  onClick={() => handleCheckout(tier.key)}
                  disabled={loading !== null}
                  className={tier.featured ? 'btn-primary w-full' : 'btn-ghost w-full'}
                  style={{
                    opacity: loading !== null && loading !== tier.key ? 0.5 : 1,
                  }}
                >
                  {loading === tier.key ? 'Redirecting…' : tier.cta}
                </button>

                <p style={{ textAlign: 'center', fontSize: 11, color: 'var(--gray)', marginTop: 10 }}>
                  No credit card required for trial
                </p>
              </div>
            ))}
          </div>
        </section>

        {/* ROI Calculator */}
        <section
          style={{
            padding: '60px 24px 80px',
            borderTop: '1px solid var(--border)',
          }}
        >
          <div style={{ maxWidth: 680, margin: '0 auto' }}>
            <div style={{ textAlign: 'center', marginBottom: 40 }}>
              <h2
                style={{
                  fontSize: 36,
                  fontWeight: 900,
                  color: 'var(--white)',
                  letterSpacing: '-0.03em',
                  marginBottom: 12,
                }}
              >
                Calculate your ROI
              </h2>
              <p style={{ color: 'var(--gray-light)', fontSize: 16 }}>
                See how much revenue CodeMed can recover for your practice.
              </p>
            </div>

            <div
              className="card card-accent"
              style={{ padding: 40 }}
            >
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, marginBottom: 32 }}>
                <div>
                  <label className="form-label">Monthly Claim Volume</label>
                  <div style={{ position: 'relative' }}>
                    <input
                      type="number"
                      className="form-input"
                      value={claimVolume}
                      onChange={(e) => setClaimVolume(Number(e.target.value))}
                      min={1}
                    />
                    <span
                      style={{
                        position: 'absolute',
                        right: 12,
                        top: '50%',
                        transform: 'translateY(-50%)',
                        fontSize: 12,
                        color: 'var(--gray)',
                        fontFamily: 'IBM Plex Mono, monospace',
                      }}
                    >
                      claims
                    </span>
                  </div>
                </div>

                <div>
                  <label className="form-label">Avg Claim Value</label>
                  <div style={{ position: 'relative' }}>
                    <span
                      style={{
                        position: 'absolute',
                        left: 14,
                        top: '50%',
                        transform: 'translateY(-50%)',
                        color: 'var(--gray)',
                        fontSize: 14,
                      }}
                    >
                      $
                    </span>
                    <input
                      type="number"
                      className="form-input"
                      value={avgClaimValue}
                      onChange={(e) => setAvgClaimValue(Number(e.target.value))}
                      min={1}
                      style={{ paddingLeft: 24 }}
                    />
                  </div>
                </div>
              </div>

              {/* Result */}
              <div
                style={{
                  background: 'var(--green-glow)',
                  border: '1px solid var(--green-border)',
                  borderRadius: 12,
                  padding: 28,
                  textAlign: 'center',
                }}
              >
                <p style={{ fontSize: 13, color: 'var(--gray-light)', marginBottom: 8 }}>
                  Based on your numbers, you&apos;re likely leaving
                </p>
                <div
                  style={{
                    fontSize: 44,
                    fontWeight: 900,
                    color: 'var(--green)',
                    letterSpacing: '-0.03em',
                    lineHeight: 1.1,
                    marginBottom: 8,
                  }}
                >
                  {formatted}
                </div>
                <p style={{ fontSize: 14, color: 'var(--gray-light)' }}>
                  on the table annually — CodeMed helps you recover the majority of that.
                </p>
              </div>

              <p
                style={{
                  fontSize: 11,
                  color: 'var(--gray)',
                  marginTop: 16,
                  textAlign: 'center',
                  fontFamily: 'IBM Plex Mono, monospace',
                }}
              >
                Formula: claims × avg_value × 23% denial rate × 68% recovery rate × 12 months
              </p>

              <div style={{ marginTop: 28, textAlign: 'center' }}>
                <button
                  onClick={() => handleCheckout('growth')}
                  className="btn-primary"
                  style={{ padding: '12px 32px', fontSize: 15 }}
                >
                  Start Recovering Revenue →
                </button>
              </div>
            </div>
          </div>
        </section>

        {/* Footer */}
        <footer
          style={{
            borderTop: '1px solid var(--border)',
            padding: '24px',
            textAlign: 'center',
          }}
        >
          <p style={{ fontSize: 13, color: 'var(--gray)' }}>
            © 2025 CodeMed Group ·{' '}
            <a
              href="https://codemedgroup.com"
              target="_blank"
              rel="noopener noreferrer"
              style={{ color: 'var(--gray-light)', textDecoration: 'none' }}
            >
              codemedgroup.com
            </a>{' '}
            · HIPAA Compliant · SOC 2 Aligned
          </p>
        </footer>
      </main>
    </div>
  )
}
