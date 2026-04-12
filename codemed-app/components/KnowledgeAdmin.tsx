'use client'

import { useState, useEffect, useRef } from 'react'

interface KnowledgeEntry {
  id: string
  category: string
  title: string
  content: string
  tags: string[]
  source: string
  active: boolean
  created_at: string
  updated_at: string
}

const CATEGORIES = [
  'Payer Policies',
  'Denial Codes',
  'NexusAuth Modules',
  'Clinical Criteria',
  'LCD/NCD Policies',
  'Appeal Templates',
  'ElderlyAI',
  'Custom',
]

export default function KnowledgeAdmin() {
  const [activeTab, setActiveTab] = useState<'library' | 'add' | 'upload' | 'preview'>('library')
  const [entries, setEntries] = useState<KnowledgeEntry[]>([])
  const [loading, setLoading] = useState(false)
  const [search, setSearch] = useState('')
  const [categoryFilter, setCategoryFilter] = useState('all')

  // Add form state
  const [addCategory, setAddCategory] = useState('')
  const [addTitle, setAddTitle] = useState('')
  const [addContent, setAddContent] = useState('')
  const [addTags, setAddTags] = useState('')
  const [addSource, setAddSource] = useState('Manual')
  const [addLoading, setAddLoading] = useState(false)
  const [addSuccess, setAddSuccess] = useState(false)
  const [addError, setAddError] = useState<string | null>(null)

  // Upload state
  const fileRef = useRef<HTMLInputElement>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadStatus, setUploadStatus] = useState<string | null>(null)

  // Copied state for preview
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    fetchEntries()
  }, [search, categoryFilter])

  async function fetchEntries() {
    setLoading(true)
    const params = new URLSearchParams()
    if (search) params.set('search', search)
    if (categoryFilter !== 'all') params.set('category', categoryFilter)

    const res = await fetch(`/api/knowledge?${params}`)
    const data = await res.json()
    setEntries(data.entries || [])
    setLoading(false)
  }

  async function handleToggleActive(entry: KnowledgeEntry) {
    await fetch('/api/knowledge', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: entry.id, active: !entry.active }),
    })
    fetchEntries()
  }

  async function handleDelete(id: string) {
    if (!confirm('Delete this knowledge entry?')) return
    await fetch(`/api/knowledge?id=${id}`, { method: 'DELETE' })
    fetchEntries()
  }

  async function handleAdd(e: React.FormEvent) {
    e.preventDefault()
    setAddLoading(true)
    setAddError(null)

    const res = await fetch('/api/knowledge', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        category: addCategory,
        title: addTitle,
        content: addContent,
        tags: addTags.split(',').map((t) => t.trim()).filter(Boolean),
        source: addSource,
      }),
    })

    const data = await res.json()

    if (!res.ok) {
      setAddError(data.error || 'Failed to add entry')
      setAddLoading(false)
      return
    }

    // Generate embedding
    await fetch('/api/knowledge/embed', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: data.id, title: addTitle, content: addContent }),
    })

    setAddSuccess(true)
    setAddCategory('')
    setAddTitle('')
    setAddContent('')
    setAddTags('')
    setAddSource('Manual')
    setTimeout(() => setAddSuccess(false), 3000)
    setAddLoading(false)
    fetchEntries()
  }

  async function handleUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (!file) return

    setUploading(true)
    setUploadStatus(`Processing ${file.name}…`)

    const text = await file.text()

    // Chunk into ~800 char sections
    const chunks = chunkText(text, 800)
    setUploadStatus(`Uploading ${chunks.length} chunks…`)

    let success = 0
    for (const [i, chunk] of chunks.entries()) {
      const res = await fetch('/api/knowledge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          category: 'Custom',
          title: `${file.name} — Part ${i + 1}`,
          content: chunk,
          source: file.name,
        }),
      })
      const data = await res.json()
      if (data.id) {
        await fetch('/api/knowledge/embed', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id: data.id, title: `${file.name} Part ${i + 1}`, content: chunk }),
        })
        success++
      }
      setUploadStatus(`Embedded ${success}/${chunks.length} chunks…`)
    }

    setUploadStatus(`✓ Uploaded and embedded ${success} chunks from ${file.name}`)
    setUploading(false)
    fetchEntries()
    if (fileRef.current) fileRef.current.value = ''
  }

  function chunkText(text: string, chunkSize: number): string[] {
    const sentences = text.split(/(?<=[.!?])\s+/)
    const chunks: string[] = []
    let current = ''

    for (const sentence of sentences) {
      if ((current + sentence).length > chunkSize && current.length > 0) {
        chunks.push(current.trim())
        current = sentence
      } else {
        current += (current ? ' ' : '') + sentence
      }
    }
    if (current.trim()) chunks.push(current.trim())
    return chunks
  }

  const previewText = entries
    .filter((e) => e.active)
    .reduce((acc: Record<string, string[]>, e) => {
      if (!acc[e.category]) acc[e.category] = []
      acc[e.category].push(`### ${e.title}\n${e.content}`)
      return acc
    }, {})

  const previewString = Object.entries(previewText)
    .map(([cat, items]) => `## ${cat}\n${items.join('\n\n')}`)
    .join('\n\n---\n\n')

  return (
    <div>
      {/* Tabs */}
      <div className="flex gap-2 mb-6">
        {(['library', 'add', 'upload', 'preview'] as const).map((tab) => (
          <button
            key={tab}
            className={`tab-btn ${activeTab === tab ? 'active' : ''}`}
            onClick={() => setActiveTab(tab)}
          >
            {tab.charAt(0).toUpperCase() + tab.slice(1)}
          </button>
        ))}
      </div>

      {/* Library Tab */}
      {activeTab === 'library' && (
        <div>
          <div className="flex gap-3 mb-4">
            <input
              type="text"
              className="form-input flex-1"
              placeholder="Search entries…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
            <select
              className="form-input"
              style={{ width: 200 }}
              value={categoryFilter}
              onChange={(e) => setCategoryFilter(e.target.value)}
            >
              <option value="all">All Categories</option>
              {CATEGORIES.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>

          {loading ? (
            <div className="text-center py-12" style={{ color: 'var(--gray)' }}>Loading…</div>
          ) : entries.length === 0 ? (
            <div className="text-center py-12" style={{ color: 'var(--gray)' }}>
              No entries found. Add some knowledge to get started.
            </div>
          ) : (
            <div className="space-y-3">
              {entries.map((entry) => (
                <div
                  key={entry.id}
                  className="card"
                  style={{
                    padding: '16px 20px',
                    opacity: entry.active ? 1 : 0.5,
                  }}
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1 flex-wrap">
                        <span
                          className="text-xs font-mono px-2 py-0.5 rounded"
                          style={{
                            background: 'var(--surface)',
                            border: '1px solid var(--border)',
                            color: 'var(--gray-light)',
                          }}
                        >
                          {entry.category}
                        </span>
                        {!entry.active && (
                          <span className="badge badge-gray text-xs">Paused</span>
                        )}
                      </div>
                      <h4 className="font-semibold text-sm mb-1" style={{ color: 'var(--white)' }}>
                        {entry.title}
                      </h4>
                      <p
                        className="text-xs line-clamp-2"
                        style={{ color: 'var(--gray-light)', lineHeight: 1.5 }}
                      >
                        {entry.content}
                      </p>
                    </div>
                    <div className="flex items-center gap-2 flex-shrink-0">
                      <button
                        onClick={() => handleToggleActive(entry)}
                        className="text-xs btn-ghost"
                        style={{ padding: '4px 10px' }}
                      >
                        {entry.active ? 'Pause' : 'Activate'}
                      </button>
                      <button
                        onClick={() => handleDelete(entry.id)}
                        className="text-xs rounded-lg px-3 py-1 transition-colors"
                        style={{
                          background: 'rgba(252,90,90,0.08)',
                          border: '1px solid rgba(252,90,90,0.2)',
                          color: 'var(--red)',
                          cursor: 'pointer',
                        }}
                      >
                        Delete
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Add Tab */}
      {activeTab === 'add' && (
        <form onSubmit={handleAdd} className="space-y-4" style={{ maxWidth: 640 }}>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="form-label">Category</label>
              <select
                className="form-input"
                value={addCategory}
                onChange={(e) => setAddCategory(e.target.value)}
                required
              >
                <option value="">Select category…</option>
                {CATEGORIES.map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            <div>
              <label className="form-label">Source</label>
              <input
                type="text"
                className="form-input"
                placeholder="Manual, CMS.gov, etc."
                value={addSource}
                onChange={(e) => setAddSource(e.target.value)}
              />
            </div>
          </div>

          <div>
            <label className="form-label">Title</label>
            <input
              type="text"
              className="form-input"
              placeholder="UHC Prior Auth Criteria — Humira"
              value={addTitle}
              onChange={(e) => setAddTitle(e.target.value)}
              required
            />
          </div>

          <div>
            <label className="form-label">Content</label>
            <textarea
              className="form-input"
              rows={8}
              placeholder="Paste payer policy, LCD criteria, denial code descriptions, appeal templates, or any RCM knowledge here…"
              value={addContent}
              onChange={(e) => setAddContent(e.target.value)}
              required
              style={{ resize: 'vertical', fontFamily: 'inherit' }}
            />
          </div>

          <div>
            <label className="form-label">Tags (comma-separated)</label>
            <input
              type="text"
              className="form-input"
              placeholder="UHC, biologics, RA, prior-auth"
              value={addTags}
              onChange={(e) => setAddTags(e.target.value)}
            />
          </div>

          {addError && (
            <div
              className="text-sm rounded-lg px-4 py-3"
              style={{
                background: 'rgba(252,90,90,0.08)',
                border: '1px solid rgba(252,90,90,0.2)',
                color: 'var(--red)',
              }}
            >
              {addError}
            </div>
          )}

          {addSuccess && (
            <div
              className="text-sm rounded-lg px-4 py-3"
              style={{
                background: 'var(--green-glow)',
                border: '1px solid var(--green-border)',
                color: 'var(--green)',
              }}
            >
              ✓ Knowledge entry saved and embedded successfully.
            </div>
          )}

          <button
            type="submit"
            className="btn-primary"
            disabled={addLoading}
          >
            {addLoading ? 'Saving and embedding…' : 'Save Knowledge Entry'}
          </button>
        </form>
      )}

      {/* Upload Tab */}
      {activeTab === 'upload' && (
        <div style={{ maxWidth: 540 }}>
          <p className="text-sm mb-6" style={{ color: 'var(--gray-light)', lineHeight: 1.6 }}>
            Upload PDF, DOC, or TXT files. CodeMed will chunk the text, generate embeddings, and add each chunk to the knowledge base automatically.
          </p>

          <div
            className="rounded-xl text-center cursor-pointer transition-colors"
            style={{
              border: '2px dashed var(--border)',
              padding: 48,
              background: 'var(--surface)',
            }}
            onClick={() => fileRef.current?.click()}
          >
            <svg
              width="40"
              height="40"
              fill="none"
              viewBox="0 0 24 24"
              stroke="var(--gray)"
              strokeWidth={1.5}
              className="mx-auto mb-3"
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
            </svg>
            <p className="font-semibold mb-1" style={{ color: 'var(--white)' }}>
              Click to upload a file
            </p>
            <p className="text-sm" style={{ color: 'var(--gray)' }}>
              PDF, DOC, TXT — max 10MB
            </p>
          </div>

          <input
            ref={fileRef}
            type="file"
            accept=".txt,.pdf,.doc,.docx"
            className="hidden"
            onChange={handleUpload}
            disabled={uploading}
          />

          {uploadStatus && (
            <div
              className="mt-4 text-sm rounded-lg px-4 py-3"
              style={{
                background: uploadStatus.startsWith('✓') ? 'var(--green-glow)' : 'var(--surface)',
                border: `1px solid ${uploadStatus.startsWith('✓') ? 'var(--green-border)' : 'var(--border)'}`,
                color: uploadStatus.startsWith('✓') ? 'var(--green)' : 'var(--gray-light)',
                fontFamily: 'IBM Plex Mono, monospace',
              }}
            >
              {uploadStatus}
            </div>
          )}
        </div>
      )}

      {/* Preview Tab */}
      {activeTab === 'preview' && (
        <div>
          <div className="flex items-center justify-between mb-4">
            <p className="text-sm" style={{ color: 'var(--gray-light)' }}>
              This is what Claude sees in its system prompt from the knowledge base ({entries.filter(e => e.active).length} active entries).
            </p>
            <button
              className="btn-ghost text-sm"
              style={{ padding: '6px 14px' }}
              onClick={() => {
                navigator.clipboard.writeText(previewString)
                setCopied(true)
                setTimeout(() => setCopied(false), 2000)
              }}
            >
              {copied ? '✓ Copied' : 'Copy'}
            </button>
          </div>
          <pre
            style={{
              background: 'var(--surface)',
              border: '1px solid var(--border)',
              borderRadius: 12,
              padding: 24,
              fontSize: 12,
              lineHeight: 1.7,
              color: 'var(--gray-light)',
              fontFamily: 'IBM Plex Mono, monospace',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              maxHeight: 600,
              overflow: 'auto',
            }}
          >
            {previewString || 'No active knowledge entries.'}
          </pre>
        </div>
      )}
    </div>
  )
}
