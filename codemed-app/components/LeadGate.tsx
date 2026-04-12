'use client'

import { useState } from 'react'

interface LeadGateProps {
  onComplete: (leadId: string, data: { name: string; org: string; role: string }) => void
}

const ROLES = [
  'Revenue Cycle Director',
  'Billing Manager',
  'Practice Administrator',
  'CFO / VP Finance',
  'Physician / Provider',
  'Coding Specialist',
  'Denial Management Specialist',
  'Other',
]

export default function LeadGate({ onComplete }: LeadGateProps) {
  const [name, setName] = useState('')
  const [org, setOrg] = useState('')
  const [role, setRole] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)

    try {
      const res = await fetch('/api/leads', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, org, role }),
      })

      const data = await res.json()

      if (!res.ok) {
        setError(data.error || 'Failed to save. Try again.')
        setLoading(false)
        return
      }

      onComplete(data.id, { name, org, role })
    } catch {
      setError('Network error. Try again.')
    }

    setLoading(false)
  }

  return (
    <div className="modal-overlay">
      <div
        className="card card-accent slide-up w-full"
        style={{ maxWidth: 480, padding: 36 }}
      >
        {/* Header */}
        <div className="mb-6">
          <div
            className="text-xs font-mono mb-3"
            style={{ color: 'var(--green)', letterSpacing: '0.1em' }}
          >
            CODEMED AI
          </div>
          <h2 className="text-2xl font-bold mb-2" style={{ color: 'var(--white)' }}>
            Before we begin
          </h2>
          <p style={{ color: 'var(--gray-light)', fontSize: 14, lineHeight: 1.6 }}>
            Tell us a bit about yourself so CodeMed can tailor its intelligence to your specific practice needs.
          </p>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="form-label">Full Name</label>
            <input
              type="text"
              className="form-input"
              placeholder="Dr. Sarah Chen"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              autoFocus
            />
          </div>

          <div>
            <label className="form-label">Organization</label>
            <input
              type="text"
              className="form-input"
              placeholder="Midwest Cardiology Group"
              value={org}
              onChange={(e) => setOrg(e.target.value)}
              required
            />
          </div>

          <div>
            <label className="form-label">Your Role</label>
            <select
              className="form-input"
              value={role}
              onChange={(e) => setRole(e.target.value)}
              required
            >
              <option value="">Select your role…</option>
              {ROLES.map((r) => (
                <option key={r} value={r}>{r}</option>
              ))}
            </select>
          </div>

          {error && (
            <div
              className="text-sm rounded-lg px-4 py-3"
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
            type="submit"
            className="btn-primary w-full"
            disabled={loading || !name || !org || !role}
            style={{ marginTop: 4 }}
          >
            {loading ? 'Setting up your workspace…' : 'Launch CodeMed AI →'}
          </button>
        </form>

        {/* Bottom disclaimer */}
        <p className="text-center mt-4" style={{ fontSize: 11, color: 'var(--gray)' }}>
          HIPAA-compliant · No PHI stored · SOC 2 aligned
        </p>
      </div>
    </div>
  )
}
