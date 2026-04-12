import { createServiceClient } from './supabase-server'

export async function getMemoryContext(userId: string): Promise<string | null> {
  const supabase = createServiceClient()

  const { data: sessions } = await supabase
    .from('chat_sessions')
    .select('summary, updated_at')
    .eq('user_id', userId)
    .not('summary', 'is', null)
    .order('updated_at', { ascending: false })
    .limit(3)

  if (!sessions || sessions.length === 0) return null

  const summaries = sessions
    .map((s, i) => `Session ${i + 1}: ${s.summary}`)
    .join('\n')

  return summaries
}

export async function upsertSession(
  sessionId: string,
  userId: string,
  messages: Array<{ role: string; content: string }>,
  context?: string | null
) {
  const supabase = createServiceClient()

  await supabase.from('chat_sessions').upsert({
    id: sessionId,
    user_id: userId,
    messages,
    context,
    updated_at: new Date().toISOString(),
  })
}
