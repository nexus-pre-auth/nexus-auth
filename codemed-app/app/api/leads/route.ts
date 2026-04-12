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

    const serviceSupabase = createServiceClient()

    // Check if lead already exists
    const { data: existing } = await serviceSupabase
      .from('leads')
      .select('id')
      .eq('user_id', user.id)
      .single()

    if (existing) {
      return NextResponse.json({ id: existing.id, exists: true })
    }

    const { name, org, role, email } = await req.json()

    if (!name || !org || !role) {
      return NextResponse.json({ error: 'Missing required fields' }, { status: 400 })
    }

    const { data, error } = await serviceSupabase
      .from('leads')
      .insert({
        user_id: user.id,
        name,
        org,
        role,
        email: email || user.email,
      })
      .select('id')
      .single()

    if (error) {
      console.error('Lead insert error:', error)
      return NextResponse.json({ error: 'Failed to create lead' }, { status: 500 })
    }

    return NextResponse.json({ id: data.id, exists: false })
  } catch (err) {
    console.error('Leads API error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

export async function GET(req: NextRequest) {
  try {
    const supabase = createClient()
    const { data: { user }, error: authError } = await supabase.auth.getUser()

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const serviceSupabase = createServiceClient()
    const { data } = await serviceSupabase
      .from('leads')
      .select('id, name, org, role, email')
      .eq('user_id', user.id)
      .single()

    return NextResponse.json({ lead: data || null })
  } catch (err) {
    console.error('Leads GET error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
