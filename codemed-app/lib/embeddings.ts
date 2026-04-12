import Anthropic from '@anthropic-ai/sdk'

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY!,
})

export async function generateEmbedding(text: string): Promise<number[]> {
  // Use OpenAI-compatible embedding endpoint via Anthropic SDK
  // text-embedding-3-small is an OpenAI model — call via fetch directly
  const response = await fetch('https://api.openai.com/v1/embeddings', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${process.env.OPENAI_API_KEY || ''}`,
    },
    body: JSON.stringify({
      model: 'text-embedding-3-small',
      input: text.slice(0, 8000),
    }),
  })

  if (!response.ok) {
    // Fallback: generate a zero vector if embedding service unavailable
    console.error('Embedding API error:', await response.text())
    return new Array(1536).fill(0)
  }

  const data = await response.json()
  return data.data[0].embedding
}

// Format vector for Supabase pgvector
export function vectorToString(embedding: number[]): string {
  return `[${embedding.join(',')}]`
}
