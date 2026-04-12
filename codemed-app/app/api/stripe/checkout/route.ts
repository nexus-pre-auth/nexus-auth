import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase-server'
import { createServiceClient } from '@/lib/supabase-server'
import { stripe, PRICE_IDS } from '@/lib/stripe'

export async function POST(req: NextRequest) {
  try {
    const supabase = createClient()
    const { data: { user }, error: authError } = await supabase.auth.getUser()

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { tier } = await req.json()

    if (!tier || !PRICE_IDS[tier as keyof typeof PRICE_IDS]) {
      return NextResponse.json({ error: 'Invalid tier' }, { status: 400 })
    }

    const serviceSupabase = createServiceClient()

    // Get or create Stripe customer
    const { data: existingSub } = await serviceSupabase
      .from('subscriptions')
      .select('stripe_customer_id')
      .eq('user_id', user.id)
      .single()

    let customerId = existingSub?.stripe_customer_id

    if (!customerId) {
      const customer = await stripe.customers.create({
        email: user.email!,
        metadata: { supabase_user_id: user.id },
      })
      customerId = customer.id
    }

    // Check founding member status (first 5 customers)
    const { count } = await serviceSupabase
      .from('founding_members')
      .select('*', { count: 'exact', head: true })

    const isFoundingEligible = (count || 0) < 5

    // Create checkout session
    const sessionParams: Parameters<typeof stripe.checkout.sessions.create>[0] = {
      customer: customerId,
      payment_method_types: ['card'],
      line_items: [
        {
          price: PRICE_IDS[tier as keyof typeof PRICE_IDS],
          quantity: 1,
        },
      ],
      mode: 'subscription',
      subscription_data: {
        trial_period_days: 14,
        metadata: { supabase_user_id: user.id, tier },
      },
      success_url: `${process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3000'}/chat?success=true`,
      cancel_url: `${process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3000'}/pricing`,
      metadata: { supabase_user_id: user.id, tier },
    }

    // Apply founding member coupon if eligible
    if (isFoundingEligible) {
      // Create a 40% forever discount coupon if it doesn't exist
      let couponId: string
      try {
        const existingCoupon = await stripe.coupons.retrieve('FOUNDING40')
        couponId = existingCoupon.id
      } catch {
        const newCoupon = await stripe.coupons.create({
          id: 'FOUNDING40',
          percent_off: 40,
          duration: 'forever',
          name: 'Founding Member — 40% Off Forever',
        })
        couponId = newCoupon.id
      }
      sessionParams.discounts = [{ coupon: couponId }]
    }

    const session = await stripe.checkout.sessions.create(sessionParams)

    return NextResponse.json({ url: session.url })
  } catch (err) {
    console.error('Stripe checkout error:', err)
    return NextResponse.json({ error: 'Failed to create checkout session' }, { status: 500 })
  }
}
