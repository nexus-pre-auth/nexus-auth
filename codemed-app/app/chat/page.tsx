import { redirect } from 'next/navigation'
import { createClient } from '@/lib/supabase-server'
import { createServiceClient } from '@/lib/supabase-server'
import ChatShell from '@/components/ChatShell'

export default async function ChatPage() {
  const supabase = createClient()
  const { data: { user } } = await supabase.auth.getUser()

  if (!user) {
    redirect('/auth')
  }

  const serviceSupabase = createServiceClient()

  // Fetch subscription
  const { data: subscription } = await serviceSupabase
    .from('subscriptions')
    .select('tier, status')
    .eq('user_id', user.id)
    .single()

  // Fetch lead
  const { data: lead } = await serviceSupabase
    .from('leads')
    .select('id, name, org, role')
    .eq('user_id', user.id)
    .single()

  return (
    <ChatShell
      user={{ id: user.id, email: user.email! }}
      subscription={subscription || null}
      initialLead={lead || null}
    />
  )
}
