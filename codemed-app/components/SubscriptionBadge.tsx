'use client'

interface SubscriptionBadgeProps {
  tier: string
  status?: string
}

export default function SubscriptionBadge({ tier, status }: SubscriptionBadgeProps) {
  const tierLabels: Record<string, string> = {
    starter: 'Starter',
    growth: 'Growth',
    enterprise: 'Enterprise',
    trialing: 'Trial',
  }

  const label = tierLabels[tier] || 'Free'

  const isActive = !status || ['trialing', 'active'].includes(status)

  return (
    <span className={`badge ${isActive ? 'badge-green' : 'badge-gray'}`}>
      {isActive && (
        <svg width="6" height="6" viewBox="0 0 6 6" fill="currentColor">
          <circle cx="3" cy="3" r="3" />
        </svg>
      )}
      {label}
    </span>
  )
}
