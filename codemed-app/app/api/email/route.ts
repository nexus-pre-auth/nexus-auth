import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase-server'
import { createServiceClient } from '@/lib/supabase-server'

export async function POST(req: NextRequest) {
  try {
    const supabase = createClient()
    const { data: { user } } = await supabase.auth.getUser()

    if (!user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { email, leadId, context } = await req.json()

    if (!email || !leadId) {
      return NextResponse.json({ error: 'Missing email or leadId' }, { status: 400 })
    }

    const serviceSupabase = createServiceClient()

    await serviceSupabase.from('email_captures').insert({
      lead_id: leadId,
      email,
      context: context || null,
    })

    return NextResponse.json({ ok: true })
  } catch (err) {
    console.error('Email capture error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
