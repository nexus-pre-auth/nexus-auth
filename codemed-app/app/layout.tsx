import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'CodeMed — AI Revenue Cycle Intelligence for Healthcare',
  description:
    'CodeMed automates prior authorization, denial management, AR acceleration, and payer intelligence using AI. Replace manual billing work with intelligent workflows.',
  keywords: 'prior authorization, denial management, revenue cycle management, healthcare AI, medical billing',
  openGraph: {
    title: 'CodeMed — AI Revenue Cycle Intelligence',
    description: 'AI-powered RCM automation for medical practices and health systems.',
    url: 'https://codemedgroup.com',
    siteName: 'CodeMed',
    type: 'website',
  },
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=IBM+Plex+Mono:wght@400;500&display=swap"
          rel="stylesheet"
        />
      </head>
      <body>{children}</body>
    </html>
  )
}
