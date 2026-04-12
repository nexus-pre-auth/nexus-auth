import { NextRequest, NextResponse } from 'next/server'
import Anthropic from '@anthropic-ai/sdk'
import { createClient } from '@/lib/supabase-server'
import { createServiceClient } from '@/lib/supabase-server'

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY! })

export async function POST(req: NextRequest) {
  try {
    const supabase = createClient()
    const { data: { user }, error: authError } = await supabase.auth.getUser()

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { sessionId, messages } = await req.json()

    if (!sessionId || !messages || messages.length === 0) {
      return NextResponse.json({ error: 'Missing sessionId or messages' }, { status: 400 })
    }

    const lastSix = messages.slice(-6)
    const transcript = lastSix
      .map((m: { role: string; content: string }) => `${m.role}: ${m.content}`)
      .join('\n')

    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 100,
      messages: [
        {
          role: 'user',
          content: `Summarize this RCM consultation in one sentence (no PHI, focus on topic and outcome):\n\n${transcript}`,
        },
      ],
    })

    const summary =
      response.content[0].type === 'text' ? response.content[0].text : 'RCM consultation session.'

    const serviceSupabase = createServiceClient()

    await serviceSupabase
      .from('chat_sessions')
      .update({ summary, updated_at: new Date().toISOString() })
      .eq('id', sessionId)

    return NextResponse.json({ ok: true, summary })
  } catch (err) {
    console.error('Summary API error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
