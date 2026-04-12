'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'

interface PaywallModalProps {
  onClose?: () => void
}

export default function PaywallModal({ onClose }: PaywallModalProps) {
  const [loading, setLoading] = useState<string | null>(null)
  const router = useRouter()

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
        console.error('Checkout error:', error)
        setLoading(null)
      }
    } catch {
      setLoading(null)
    }
  }

  return (
    <div className="modal-overlay">
      <div
        className="card card-accent slide-up w-full"
        style={{ maxWidth: 520, padding: 40 }}
      >
        {/* Header */}
        <div className="text-center mb-8">
          <div
            className="inline-flex items-center gap-2 px-3 py-1 rounded-full mb-4 text-xs font-mono font-semibold"
            style={{
              background: 'rgba(246,173,60,0.1)',
              border: '1px solid rgba(246,173,60,0.25)',
              color: 'var(--gold)',
            }}
          >
            🔒 Free preview limit reached
          </div>
          <h2 className="text-2xl font-bold mb-3" style={{ color: 'var(--white)' }}>
            Unlock CodeMed AI
          </h2>
          <p style={{ color: 'var(--gray-light)', fontSize: 15, lineHeight: 1.6 }}>
            You&apos;ve seen what CodeMed can do. Start a 14-day free trial to get unlimited access to prior auth, denial management, and payer intelligence.
          </p>
        </div>

        {/* Stats row */}
        <div
          className="grid grid-cols-3 gap-4 mb-8 py-4 rounded-xl"
          style={{ background: 'var(--surface)', border: '1px solid var(--border)' }}
        >
          {[
            { value: '94%', label: 'PA approval rate' },
            { value: '6.2h', label: 'Avg turnaround' },
            { value: '$206K', label: 'Monthly protected' },
          ].map((stat) => (
            <div key={stat.label} className="stat-item">
              <span className="stat-value">{stat.value}</span>
              <span className="stat-label">{stat.label}</span>
            </div>
          ))}
        </div>

        {/* Plans */}
        <div className="space-y-3 mb-6">
          {[
            { tier: 'starter', label: 'Starter', price: '$2,500/mo', note: 'Up to 3 users · 1 location' },
            { tier: 'growth', label: 'Growth', price: '$5,000/mo', note: 'Up to 10 users · 3 locations', featured: true },
            { tier: 'enterprise', label: 'Enterprise', price: '$10,000/mo', note: 'Unlimited users · Full S1–S11 stack' },
          ].map((plan) => (
            <button
              key={plan.tier}
              onClick={() => handleCheckout(plan.tier)}
              disabled={loading !== null}
              className="w-full flex items-center justify-between rounded-xl px-5 py-4 transition-all"
              style={{
                background: plan.featured ? 'var(--green-glow)' : 'var(--surface)',
                border: `1px solid ${plan.featured ? 'var(--green-border)' : 'var(--border)'}`,
                cursor: loading !== null ? 'not-allowed' : 'pointer',
                opacity: loading !== null && loading !== plan.tier ? 0.5 : 1,
              }}
            >
              <div className="text-left">
                <div className="font-bold flex items-center gap-2" style={{ color: 'var(--white)' }}>
                  {plan.label}
                  {plan.featured && (
                    <span
                      className="text-xs px-2 py-0.5 rounded-full font-semibold"
                      style={{ background: 'var(--green)', color: '#0A0F1A' }}
                    >
                      Popular
                    </span>
                  )}
                </div>
                <div className="text-xs mt-0.5" style={{ color: 'var(--gray)' }}>{plan.note}</div>
              </div>
              <div className="text-right">
                <div className="font-bold" style={{ color: plan.featured ? 'var(--green)' : 'var(--white)' }}>
                  {plan.price}
                </div>
                <div className="text-xs" style={{ color: 'var(--green)' }}>
                  {loading === plan.tier ? 'Loading…' : '14-day free trial →'}
                </div>
              </div>
            </button>
          ))}
        </div>

        <div className="flex items-center gap-4">
          <button
            onClick={() => router.push('/pricing')}
            className="btn-ghost flex-1 text-sm"
          >
            View full pricing
          </button>
          {onClose && (
            <button onClick={onClose} className="btn-ghost flex-1 text-sm">
              Continue for now
            </button>
          )}
        </div>

        <p className="text-center mt-4" style={{ fontSize: 11, color: 'var(--gray)' }}>
          No credit card required for trial · Cancel anytime
        </p>
      </div>
    </div>
  )
}
