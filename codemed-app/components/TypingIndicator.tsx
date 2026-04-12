'use client'

export default function TypingIndicator() {
  return (
    <div className="flex items-start gap-3 fade-in">
      <div
        className="w-7 h-7 rounded-full flex-shrink-0 flex items-center justify-center text-xs font-bold"
        style={{ background: 'linear-gradient(135deg, #00D4A0, #00B386)', color: '#0A0F1A' }}
      >
        C
      </div>
      <div className="bubble-bot flex items-center gap-2" style={{ padding: '14px 18px' }}>
        <div className="flex items-center gap-1">
          <div className="typing-dot" />
          <div className="typing-dot" />
          <div className="typing-dot" />
        </div>
      </div>
    </div>
  )
}
