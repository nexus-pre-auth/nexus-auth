import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase-server'
import { createServiceClient } from '@/lib/supabase-server'
import { renderToBuffer } from '@react-pdf/renderer'
import { AppealLetterPDF } from '@/lib/pdf'
import React from 'react'

export async function POST(req: NextRequest) {
  try {
    const supabase = createClient()
    const { data: { user }, error: authError } = await supabase.auth.getUser()

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { content, payer, drug, sessionId, title } = await req.json()

    if (!content) {
      return NextResponse.json({ error: 'Missing content' }, { status: 400 })
    }

    const date = new Date().toLocaleDateString('en-US', {
      month: 'long',
      day: 'numeric',
      year: 'numeric',
    })

    // Generate PDF buffer
    const element = React.createElement(AppealLetterPDF, { content, payer, drug, date })
    const pdfBuffer = await renderToBuffer(element)

    // Upload to Supabase Storage
    const serviceSupabase = createServiceClient()
    const fileName = `${user.id}/appeal-${Date.now()}.pdf`

    const { error: uploadError } = await serviceSupabase.storage
      .from('documents')
      .upload(fileName, pdfBuffer, {
        contentType: 'application/pdf',
        upsert: false,
      })

    if (uploadError) {
      console.error('Upload error:', uploadError)
      return NextResponse.json({ error: 'Failed to upload PDF' }, { status: 500 })
    }

    // Get public URL
    const { data: urlData } = serviceSupabase.storage
      .from('documents')
      .getPublicUrl(fileName)

    const pdfUrl = urlData.publicUrl

    // Save document record
    await serviceSupabase.from('documents').insert({
      user_id: user.id,
      session_id: sessionId || null,
      type: 'appeal_letter',
      title: title || `Appeal Letter — ${payer || 'Unknown Payer'}`,
      content,
      payer: payer || null,
      drug: drug || null,
      pdf_url: pdfUrl,
    })

    return NextResponse.json({ url: pdfUrl })
  } catch (err) {
    console.error('PDF generation error:', err)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}
