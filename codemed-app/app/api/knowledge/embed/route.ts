import { NextRequest, NextResponse } from 'next/server'
import { createServiceClient } from '@/lib/supabase-server'
import { generateEmbedding, vectorToString } from '@/lib/embeddings'

export async function POST(req: NextRequest) {
  try {
    const { id, title, content } = await req.json()

    if (!id || !title || !content) {
      return NextResponse.json({ error: 'Missing id, title, or content' }, { status: 400 })
    }

    const inputText = `${title}: ${content}`
    const embedding = await generateEmbedding(inputText)
    const embeddingStr = vectorToString(embedding)

    const serviceSupabase = createServiceClient()

    const { error } = await serviceSupabase
      .from('knowledge_base')
      .update({ embedding: embeddingStr, updated_at: new Date().toISOString() })
      .eq('id', id)

    if (error) {
      console.error('Embedding update error:', error)
      return NextResponse.json({ error: 'Failed to store embedding' }, { status: 500 })
    }

    return NextResponse.json({ ok: true })
  } catch (err) {
    console.error('Embed API error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
