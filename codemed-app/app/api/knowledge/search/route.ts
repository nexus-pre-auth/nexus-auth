import { NextRequest, NextResponse } from 'next/server'
import { createServiceClient } from '@/lib/supabase-server'
import { generateEmbedding, vectorToString } from '@/lib/embeddings'

export async function POST(req: NextRequest) {
  try {
    const { query, threshold = 0.65, count = 5 } = await req.json()

    if (!query) {
      return NextResponse.json({ error: 'Missing query' }, { status: 400 })
    }

    const embedding = await generateEmbedding(query)
    const serviceSupabase = createServiceClient()

    const { data, error } = await serviceSupabase.rpc('match_knowledge', {
      query_embedding: vectorToString(embedding),
      match_threshold: threshold,
      match_count: count,
    })

    if (error) {
      console.error('Vector search error:', error)
      return NextResponse.json({ results: [] })
    }

    return NextResponse.json({ results: data || [] })
  } catch (err) {
    console.error('Knowledge search error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
