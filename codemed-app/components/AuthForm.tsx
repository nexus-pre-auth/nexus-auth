'use client'

import { useState } from 'react'
import { createClient } from '@/lib/supabase'
import { useRouter } from 'next/navigation'

export default function AuthForm() {
  const [mode, setMode] = useState<'signin' | 'signup'>('signin')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState(false)
  const router = useRouter()
  const supabase = createClient()

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)

    if (mode === 'signup') {
      const { error: signUpError } = await supabase.auth.signUp({
        email,
        password,
        options: { emailRedirectTo: `${window.location.origin}/chat` },
      })
      if (signUpError) {
        setError(signUpError.message)
      } else {
        setSuccess(true)
      }
    } else {
      const { error: signInError } = await supabase.auth.signInWithPassword({ email, password })
      if (signInError) {
        setError(signInError.message)
      } else {
        router.push('/chat')
        router.refresh()
      }
    }

    setLoading(false)
  }

  if (success && mode === 'signup') {
    return (
      <div className="text-center">
        <div
          className="w-14 h-14 rounded-full flex items-center justify-center mx-auto mb-4"
          style={{ background: 'var(--green-glow)', border: '1px solid var(--green-border)' }}
        >
          <svg width="24" height="24" fill="none" viewBox="0 0 24 24" stroke="var(--green)" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
          </svg>
        </div>
        <h2 className="text-xl font-bold mb-2" style={{ color: 'var(--white)' }}>Check your email</h2>
        <p style={{ color: 'var(--gray-light)', fontSize: 14 }}>
          We sent a confirmation link to <strong style={{ color: 'var(--white)' }}>{email}</strong>.
          Click it to activate your account.
        </p>
        <button
          className="btn-ghost mt-6"
          onClick={() => { setMode('signin'); setSuccess(false) }}
        >
          Back to Sign In
        </button>
      </div>
    )
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div>
        <label className="form-label">Email</label>
        <input
          type="email"
          className="form-input"
          placeholder="you@practice.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          required
          autoComplete="email"
        />
      </div>

      <div>
        <label className="form-label">Password</label>
        <input
          type="password"
          className="form-input"
          placeholder="••••••••"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
          minLength={8}
          autoComplete={mode === 'signup' ? 'new-password' : 'current-password'}
        />
      </div>

      {error && (
        <div
          className="text-sm rounded-lg px-4 py-3"
          style={{ background: 'rgba(252,90,90,0.08)', border: '1px solid rgba(252,90,90,0.2)', color: 'var(--red)' }}
        >
          {error}
        </div>
      )}

      <button
        type="submit"
        className="btn-primary w-full"
        disabled={loading || !email || !password}
        style={{ marginTop: 8 }}
      >
        {loading ? (
          <>
            <svg className="animate-spin" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
              <circle cx="12" cy="12" r="10" strokeOpacity={0.3} />
              <path d="M12 2a10 10 0 0 1 10 10" />
            </svg>
            {mode === 'signup' ? 'Creating account…' : 'Signing in…'}
          </>
        ) : (
          mode === 'signup' ? 'Create Account' : 'Sign In'
        )}
      </button>

      <p className="text-center text-sm" style={{ color: 'var(--gray)' }}>
        {mode === 'signin' ? "Don't have an account?" : 'Already have an account?'}{' '}
        <button
          type="button"
          className="font-semibold"
          style={{ color: 'var(--green)' }}
          onClick={() => { setMode(mode === 'signin' ? 'signup' : 'signin'); setError(null) }}
        >
          {mode === 'signin' ? 'Sign Up' : 'Sign In'}
        </button>
      </p>
    </form>
  )
}
