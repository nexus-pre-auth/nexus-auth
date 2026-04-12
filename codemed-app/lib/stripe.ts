import Stripe from 'stripe'

export const stripe = new Stripe(process.env.STRIPE_SECRET_KEY!, {
  apiVersion: '2024-06-20',
})

export const PRICE_IDS = {
  starter: process.env.STRIPE_STARTER_PRICE_ID!,
  growth: process.env.STRIPE_GROWTH_PRICE_ID!,
  enterprise: process.env.STRIPE_ENTERPRISE_PRICE_ID!,
}

export const PLAN_NAMES = {
  [process.env.STRIPE_STARTER_PRICE_ID || 'starter']: 'starter',
  [process.env.STRIPE_GROWTH_PRICE_ID || 'growth']: 'growth',
  [process.env.STRIPE_ENTERPRISE_PRICE_ID || 'enterprise']: 'enterprise',
} as Record<string, string>
