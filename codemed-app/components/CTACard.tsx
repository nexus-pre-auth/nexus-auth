'use client'

interface CTACardProps {
  calendlyUrl?: string
  onShowPricing: () => void
}

export default function CTACard({ calendlyUrl, onShowPricing }: CTACardProps) {
  return (
    <div
      className="card card-accent fade-in mt-2"
      style={{ padding: '20px 24px', maxWidth: '85%' }}
    >
      <p
        className="text-xs font-mono font-semibold mb-3"
        style={{ color: 'var(--green)', letterSpacing: '0.1em' }}
      >
        READY TO SCALE?
      </p>
      <p className="font-semibold mb-1" style={{ color: 'var(--white)', fontSize: 15 }}>
        See your full ROI potential
      </p>
      <p style={{ color: 'var(--gray-light)', fontSize: 13, lineHeight: 1.5, marginBottom: 16 }}>
        Most practices recover $150K–$400K in their first 90 days with CodeMed. Book a personalized ROI walkthrough.
      </p>
      <div className="flex gap-3">
        {calendlyUrl && (
          <a
            href={calendlyUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="btn-primary text-sm"
            style={{ padding: '8px 16px' }}
          >
            Book Demo →
          </a>
        )}
        <button
          onClick={onShowPricing}
          className="btn-ghost text-sm"
          style={{ padding: '8px 16px' }}
        >
          View Plans
        </button>
      </div>
    </div>
  )
}
