import { NextRequest, NextResponse } from 'next/server'
import Anthropic from '@anthropic-ai/sdk'
import { createClient } from '@/lib/supabase-server'
import { createServiceClient } from '@/lib/supabase-server'
import { buildSystemPrompt } from '@/lib/prompts'
import { getMemoryContext, upsertSession } from '@/lib/memory'

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY! })

export async function POST(req: NextRequest) {
  try {
    const supabase = createClient()
    const { data: { user }, error: authError } = await supabase.auth.getUser()

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const serviceSupabase = createServiceClient()

    // Check subscription
    const { data: sub } = await serviceSupabase
      .from('subscriptions')
      .select('tier, status')
      .eq('user_id', user.id)
      .single()

    const tier = sub?.tier || 'trialing'
    const isActive = !sub || ['trialing', 'active'].includes(sub.status || '')

    if (!isActive) {
      return NextResponse.json({ error: 'Subscription expired' }, { status: 402 })
    }

    const { messages, sessionId, context, leadId } = await req.json()

    if (!messages || !Array.isArray(messages)) {
      return NextResponse.json({ error: 'Invalid messages' }, { status: 400 })
    }

    // Get lead info
    let lead = { name: 'there', org: 'your practice', role: 'billing team' }
    if (leadId) {
      const { data: leadData } = await serviceSupabase
        .from('leads')
        .select('name, org, role')
        .eq('id', leadId)
        .single()
      if (leadData) lead = leadData
    }

    // RAG search
    const lastUserMessage = messages.filter((m: { role: string }) => m.role === 'user').pop()
    let relevantKnowledge: Array<{ category: string; title: string; content: string }> = []

    if (lastUserMessage?.content) {
      try {
        const searchRes = await fetch(
          `${process.env.NEXT_PUBLIC_SUPABASE_URL}/functions/v1/search-knowledge`,
          {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
            },
            body: JSON.stringify({ query: lastUserMessage.content }),
          }
        )
        if (searchRes.ok) {
          const { results } = await searchRes.json()
          relevantKnowledge = results || []
        }
      } catch {
        // RAG unavailable — proceed without it
      }
    }

    // Memory context
    const memoryContext = await getMemoryContext(user.id)

    // Build system prompt
    const systemPrompt = buildSystemPrompt({
      lead,
      context,
      tier,
      relevantKnowledge,
      memoryContext,
    })

    // Call Claude
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 800,
      system: systemPrompt,
      messages: messages.map((m: { role: string; content: string }) => ({
        role: m.role as 'user' | 'assistant',
        content: m.content,
      })),
    })

    const text = response.content[0].type === 'text' ? response.content[0].text : ''
    const isAppealLetter = text.trim().startsWith('APPEAL LETTER:')

    // Upsert session
    if (sessionId) {
      await upsertSession(sessionId, user.id, messages, context?.key || null)
    }

    return NextResponse.json({ text, isAppealLetter })
  } catch (err) {
    console.error('Chat API error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
