import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase-server'
import { createServiceClient } from '@/lib/supabase-server'

export async function GET(req: NextRequest) {
  try {
    const supabase = createClient()
    const { data: { user }, error: authError } = await supabase.auth.getUser()

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const serviceSupabase = createServiceClient()

    const { data, error } = await serviceSupabase
      .from('documents')
      .select('id, type, title, payer, drug, pdf_url, created_at')
      .eq('user_id', user.id)
      .order('created_at', { ascending: false })

    if (error) {
      return NextResponse.json({ error: 'Failed to fetch documents' }, { status: 500 })
    }

    return NextResponse.json({ documents: data || [] })
  } catch (err) {
    console.error('Documents GET error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

export async function POST(req: NextRequest) {
  try {
    const supabase = createClient()
    const { data: { user }, error: authError } = await supabase.auth.getUser()

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { sessionId, type, title, content, payer, drug, pdfUrl } = await req.json()

    if (!title || !content || !type) {
      return NextResponse.json({ error: 'Missing required fields' }, { status: 400 })
    }

    const serviceSupabase = createServiceClient()

    const { data, error } = await serviceSupabase
      .from('documents')
      .insert({
        user_id: user.id,
        session_id: sessionId || null,
        type,
        title,
        content,
        payer: payer || null,
        drug: drug || null,
        pdf_url: pdfUrl || null,
      })
      .select('id')
      .single()

    if (error) {
      console.error('Document insert error:', error)
      return NextResponse.json({ error: 'Failed to save document' }, { status: 500 })
    }

    return NextResponse.json({ id: data.id })
  } catch (err) {
    console.error('Documents POST error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
