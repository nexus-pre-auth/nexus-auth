'use client'

import React from 'react'
import {
  Document,
  Page,
  Text,
  View,
  StyleSheet,
  Font,
} from '@react-pdf/renderer'

Font.register({
  family: 'Inter',
  fonts: [
    { src: 'https://fonts.gstatic.com/s/inter/v13/UcCO3FwrK3iLTeHuS_fvQtMwCp50KnMw2boKoduKmMEVuLyfAZ9hiJ-Ek-_EeA.woff', fontWeight: 400 },
    { src: 'https://fonts.gstatic.com/s/inter/v13/UcCO3FwrK3iLTeHuS_fvQtMwCp50KnMw2boKoduKmMEVuGKYAZ9hiJ-Ek-_EeA.woff', fontWeight: 700 },
  ],
})

const styles = StyleSheet.create({
  page: {
    fontFamily: 'Inter',
    fontSize: 11,
    color: '#1a1a2e',
    paddingTop: 60,
    paddingBottom: 60,
    paddingHorizontal: 72,
    lineHeight: 1.6,
  },
  header: {
    borderBottomWidth: 2,
    borderBottomColor: '#00D4A0',
    paddingBottom: 16,
    marginBottom: 24,
  },
  logo: {
    fontSize: 18,
    fontWeight: 700,
    color: '#00D4A0',
    marginBottom: 4,
  },
  tagline: {
    fontSize: 9,
    color: '#6B8299',
    textTransform: 'uppercase',
    letterSpacing: 1,
  },
  title: {
    fontSize: 16,
    fontWeight: 700,
    marginBottom: 8,
    color: '#0A0F1A',
  },
  date: {
    fontSize: 10,
    color: '#6B8299',
    marginBottom: 24,
  },
  section: {
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 12,
    fontWeight: 700,
    marginBottom: 8,
    color: '#0A0F1A',
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  body: {
    fontSize: 11,
    lineHeight: 1.7,
    color: '#333',
  },
  footer: {
    position: 'absolute',
    bottom: 30,
    left: 72,
    right: 72,
    borderTopWidth: 1,
    borderTopColor: '#e5e5e5',
    paddingTop: 10,
    flexDirection: 'row',
    justifyContent: 'space-between',
  },
  footerText: {
    fontSize: 8,
    color: '#9BAFC4',
  },
})

interface AppealLetterPDFProps {
  content: string
  payer?: string
  drug?: string
  date: string
}

export function AppealLetterPDF({ content, payer, drug, date }: AppealLetterPDFProps) {
  const lines = content.replace(/^APPEAL LETTER:\n?/i, '').split('\n')

  return React.createElement(
    Document,
    null,
    React.createElement(
      Page,
      { size: 'LETTER', style: styles.page },
      React.createElement(
        View,
        { style: styles.header },
        React.createElement(Text, { style: styles.logo }, 'CodeMed'),
        React.createElement(Text, { style: styles.tagline }, 'AI Revenue Cycle Intelligence')
      ),
      React.createElement(Text, { style: styles.title }, 'Prior Authorization Appeal Letter'),
      React.createElement(Text, { style: styles.date }, `Generated: ${date}${payer ? ` | Payer: ${payer}` : ''}${drug ? ` | Drug/Service: ${drug}` : ''}`),
      React.createElement(
        View,
        { style: styles.section },
        ...lines.map((line, i) =>
          React.createElement(
            Text,
            { key: i, style: styles.body },
            line || ' '
          )
        )
      ),
      React.createElement(
        View,
        { style: styles.footer },
        React.createElement(Text, { style: styles.footerText }, 'CodeMed Group · codemedgroup.com'),
        React.createElement(Text, { style: styles.footerText }, 'CONFIDENTIAL — For authorized use only')
      )
    )
  )
}
