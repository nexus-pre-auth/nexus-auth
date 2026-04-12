'use client'

import { useState, useEffect, useRef, useCallback } from 'react'
import { v4 as uuidv4 } from 'uuid'
import ChipRow from './ChipRow'
import LeadGate from './LeadGate'
import PaywallModal from './PaywallModal'
import CTACard from './CTACard'
import TypingIndicator from './TypingIndicator'
import PDFExportButton from './PDFExportButton'
import SubscriptionBadge from './SubscriptionBadge'
import { CHIP_FOLLOW_UPS } from '@/lib/prompts'

interface Message {
  id: string
  role: 'user' | 'assistant'
  content: string
  isAppealLetter?: boolean
  showPDF?: boolean
}

interface Lead {
  id: string
  name: string
  org: string
  role: string
}

interface ChatShellProps {
  user: { id: string; email: string }
  subscription: { tier: string; status: string } | null
  initialLead: Lead | null
}

export default function ChatShell({ user, subscription, initialLead }: ChatShellProps) {
  const [sessionId] = useState(() => uuidv4())
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [lead, setLead] = useState<Lead | null>(initialLead)
  const [showLeadGate, setShowLeadGate] = useState(!initialLead)
  const [showPaywall, setShowPaywall] = useState(false)
  const [ctaShown, setCtaShown] = useState(false)
  const [selectedChip, setSelectedChip] = useState<string | null>(null)
  const [context, setContext] = useState<{ key: string; label: string } | null>(null)
  const [userMessageCount, setUserMessageCount] = useState(0)

  const messagesEndRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const tier = subscription?.tier || 'trialing'
  const isSubscribed = subscription && ['active', 'trialing'].includes(subscription.status)

  // Welcome message
  useEffect(() => {
    if (lead && messages.length === 0) {
      const firstName = lead.name.split(' ')[0]
      setMessages([
        {
          id: uuidv4(),
          role: 'assistant',
          content: `Welcome back, ${firstName}. I'm CodeMed AI — your senior RCM intelligence engine. I'm calibrated to your practice at **${lead.org}** and ready to work.\n\nWhat's the highest-priority revenue issue on your plate today? Select a focus area or type your question below.`,
        },
      ])
    }
  }, [lead, messages.length])

  // Scroll to bottom
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  // Auto-resize textarea
  function handleInputChange(e: React.ChangeEvent<HTMLTextAreaElement>) {
    setInput(e.target.value)
    e.target.style.height = 'auto'
    e.target.style.height = Math.min(e.target.scrollHeight, 110) + 'px'
  }

  function handleLeadComplete(leadId: string, data: { name: string; org: string; role: string }) {
    const newLead = { id: leadId, ...data }
    setLead(newLead)
    setShowLeadGate(false)

    // Log event
    fetch('/api/events', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leadId, event: 'lead_captured', context: 'initial_gate' }),
    })
  }

  const sendMessage = useCallback(
    async (content: string, chipContext?: { key: string; label: string }) => {
      if (!content.trim() || loading) return

      const newCount = userMessageCount + 1
      setUserMessageCount(newCount)

      // Check paywall: message 4+ requires subscription
      if (newCount > 3 && !isSubscribed) {
        setShowPaywall(true)
        return
      }

      const userMsg: Message = { id: uuidv4(), role: 'user', content }
      const updatedMessages = [...messages, userMsg]
      setMessages(updatedMessages)
      setInput('')
      if (textareaRef.current) {
        textareaRef.current.style.height = 'auto'
      }
      setLoading(true)

      // Show CTA after message 4 (once)
      if (newCount === 4 && !ctaShown) {
        setCtaShown(true)
      }

      try {
        const res = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            messages: updatedMessages.map((m) => ({ role: m.role, content: m.content })),
            sessionId,
            context: chipContext || context,
            leadId: lead?.id,
          }),
        })

        if (res.status === 402) {
          setShowPaywall(true)
          setLoading(false)
          return
        }

        const data = await res.json()

        if (data.text) {
          const assistantMsg: Message = {
            id: uuidv4(),
            role: 'assistant',
            content: data.text,
            isAppealLetter: data.isAppealLetter,
            showPDF: data.isAppealLetter,
          }
          setMessages((prev) => [...prev, assistantMsg])

          // Log event
          if (lead?.id) {
            fetch('/api/events', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                leadId: lead.id,
                event: data.isAppealLetter ? 'appeal_letter_generated' : 'chat_message',
                context: (chipContext || context)?.key || 'general',
                metadata: { messageCount: newCount },
              }),
            })
          }
        }
      } catch {
        setMessages((prev) => [
          ...prev,
          {
            id: uuidv4(),
            role: 'assistant',
            content: 'I encountered a connection issue. Please try again in a moment.',
          },
        ])
      }

      setLoading(false)
    },
    [loading, messages, userMessageCount, isSubscribed, ctaShown, sessionId, lead, context]
  )

  function handleChipSelect(key: string, label: string) {
    const chipContext = { key, label }
    setSelectedChip(key)
    setContext(chipContext)

    const followUp = CHIP_FOLLOW_UPS[key]
    const userContent = `I need help with: ${label}`

    const userMsg: Message = { id: uuidv4(), role: 'user', content: userContent }
    const botMsg: Message = {
      id: uuidv4(),
      role: 'assistant',
      content: followUp || `Got it — let's dig into ${label}. What's the specific challenge you're facing?`,
    }

    setMessages((prev) => [...prev, userMsg, botMsg])
    setUserMessageCount((c) => c + 1)

    // Log chip selection
    if (lead?.id) {
      fetch('/api/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          leadId: lead.id,
          event: 'chip_selected',
          context: key,
        }),
      })
    }
  }

  async function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      await sendMessage(input)
    }
  }

  function renderMessageContent(msg: Message) {
    // Basic markdown-like rendering
    const formatted = msg.content
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      .replace(/\n\n/g, '</p><p>')
      .replace(/\n/g, '<br/>')

    return `<p>${formatted}</p>`
  }

  const showChips = messages.length <= 1 && !selectedChip
  const firstName = lead?.name?.split(' ')[0] || user.email.split('@')[0]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', background: 'var(--bg)' }}>
      {/* Background elements */}
      <div className="bg-grid" />
      <div className="orb-tl" />
      <div className="orb-br" />
      <div className="ghost-text">CODEMED</div>
      <span className="corner-watermark tl">ICD-10-CM · CPT-4</span>
      <span className="corner-watermark tr">CARC · RARC</span>
      <span className="corner-watermark bl">X12-278 · EDI-835</span>
      <span className="corner-watermark br">NPI · HIPAA-5010</span>

      {/* Lead Gate */}
      {showLeadGate && <LeadGate onComplete={handleLeadComplete} />}

      {/* Paywall Modal */}
      {showPaywall && (
        <PaywallModal onClose={() => setShowPaywall(false)} />
      )}

      {/* Header */}
      <header
        className="glass-header relative z-10 flex-shrink-0"
        style={{ padding: '12px 24px' }}
      >
        <div
          style={{
            maxWidth: 800,
            margin: '0 auto',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          {/* Logo */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div
              style={{
                width: 28,
                height: 28,
                borderRadius: 8,
                background: 'linear-gradient(135deg, #00D4A0, #00B386)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontWeight: 900,
                fontSize: 14,
                color: '#0A0F1A',
              }}
            >
              C
            </div>
            <span style={{ fontWeight: 800, color: 'var(--white)', fontSize: 16, letterSpacing: '-0.02em' }}>
              CodeMed
            </span>
          </div>

          {/* Right side */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <span style={{ fontSize: 13, color: 'var(--gray)', display: 'none' }} className="sm:inline">
              {user.email}
            </span>
            <SubscriptionBadge tier={tier} status={subscription?.status} />
            <div className="ai-live-pill">
              <div className="ai-live-dot" />
              AI Live
            </div>
          </div>
        </div>
      </header>

      {/* Messages */}
      <div
        style={{
          flex: 1,
          overflowY: 'auto',
          padding: '24px 24px 8px',
          position: 'relative',
          zIndex: 1,
        }}
      >
        <div style={{ maxWidth: 800, margin: '0 auto' }}>
          {messages.map((msg, idx) => (
            <div key={msg.id} style={{ marginBottom: 16 }}>
              {msg.role === 'user' ? (
                <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                  <div className="bubble-user fade-in">
                    <p style={{ fontSize: 15, lineHeight: 1.5, color: 'var(--white)' }}>
                      {msg.content}
                    </p>
                  </div>
                </div>
              ) : (
                <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start' }}>
                  <div
                    style={{
                      width: 28,
                      height: 28,
                      borderRadius: '50%',
                      background: 'linear-gradient(135deg, #00D4A0, #00B386)',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      fontWeight: 700,
                      fontSize: 12,
                      color: '#0A0F1A',
                      flexShrink: 0,
                      marginTop: 4,
                    }}
                  >
                    C
                  </div>
                  <div>
                    <div
                      className="bubble-bot prose-codemed fade-in"
                      dangerouslySetInnerHTML={{ __html: renderMessageContent(msg) }}
                    />
                    {msg.showPDF && (
                      <PDFExportButton
                        content={msg.content}
                        sessionId={sessionId}
                      />
                    )}
                  </div>
                </div>
              )}

              {/* Show chips after first bot message */}
              {idx === 0 && showChips && msg.role === 'assistant' && (
                <div style={{ marginTop: 16, paddingLeft: 40 }}>
                  <ChipRow onSelect={handleChipSelect} selected={selectedChip ?? undefined} />
                </div>
              )}

              {/* Show CTA card after 4th user message */}
              {msg.role === 'assistant' && userMessageCount === 4 && ctaShown && idx === messages.length - 1 && (
                <div style={{ paddingLeft: 40, marginTop: 8 }}>
                  <CTACard
                    calendlyUrl={process.env.NEXT_PUBLIC_CALENDLY_URL}
                    onShowPricing={() => setShowPaywall(true)}
                  />
                </div>
              )}
            </div>
          ))}

          {loading && (
            <div style={{ marginBottom: 16 }}>
              <TypingIndicator />
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>
      </div>

      {/* Input bar */}
      <div
        style={{
          padding: '12px 24px 20px',
          position: 'relative',
          zIndex: 1,
          flexShrink: 0,
        }}
      >
        <div style={{ maxWidth: 800, margin: '0 auto' }}>
          <div className="input-bar">
            <textarea
              ref={textareaRef}
              className="chat-input"
              placeholder={
                selectedChip
                  ? 'Ask a follow-up question…'
                  : 'Ask anything about prior auth, denials, AR, or payer intelligence…'
              }
              value={input}
              onChange={handleInputChange}
              onKeyDown={handleKeyDown}
              rows={1}
              disabled={loading || showLeadGate}
            />
            <button
              onClick={() => sendMessage(input)}
              disabled={loading || !input.trim() || showLeadGate}
              style={{
                width: 36,
                height: 36,
                borderRadius: 8,
                background: input.trim() && !loading
                  ? 'linear-gradient(135deg, #00D4A0, #00B386)'
                  : 'rgba(255,255,255,0.06)',
                border: 'none',
                cursor: input.trim() && !loading ? 'pointer' : 'not-allowed',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                flexShrink: 0,
                transition: 'all 0.2s',
              }}
            >
              <svg
                width="16"
                height="16"
                fill="none"
                viewBox="0 0 24 24"
                stroke={input.trim() && !loading ? '#0A0F1A' : 'var(--gray)'}
                strokeWidth={2.5}
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
              </svg>
            </button>
          </div>

          <p
            style={{
              fontSize: 11,
              color: 'var(--gray)',
              textAlign: 'center',
              marginTop: 8,
              fontFamily: 'IBM Plex Mono, monospace',
            }}
          >
            Conversations are saved to improve your experience. Do not enter patient names or member IDs.
          </p>
        </div>
      </div>
    </div>
  )
}
