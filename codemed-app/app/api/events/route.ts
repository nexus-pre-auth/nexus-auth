import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase-server'
import { createServiceClient } from '@/lib/supabase-server'

export async function POST(req: NextRequest) {
  try {
    const supabase = createClient()
    const { data: { user }, error: authError } = await supabase.auth.getUser()

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { leadId, event, context, metadata } = await req.json()

    if (!leadId || !event) {
      return NextResponse.json({ error: 'Missing leadId or event' }, { status: 400 })
    }

    const serviceSupabase = createServiceClient()

    const { error } = await serviceSupabase.from('events').insert({
      lead_id: leadId,
      event,
      context: context || null,
      metadata: metadata || {},
    })

    if (error) {
      console.error('Event insert error:', error)
      return NextResponse.json({ error: 'Failed to log event' }, { status: 500 })
    }

    return NextResponse.json({ ok: true })
  } catch (err) {
    console.error('Events API error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
