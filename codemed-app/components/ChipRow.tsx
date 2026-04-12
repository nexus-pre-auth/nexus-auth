'use client'

import { CHIP_LABELS } from '@/lib/prompts'

interface ChipRowProps {
  onSelect: (key: string, label: string) => void
  disabled?: boolean
  selected?: string | null
}

export default function ChipRow({ onSelect, disabled, selected }: ChipRowProps) {
  return (
    <div className="flex flex-wrap gap-2">
      {CHIP_LABELS.map((chip) => (
        <button
          key={chip.key}
          className="chip"
          disabled={disabled || selected !== undefined}
          onClick={() => onSelect(chip.key, chip.label)}
          style={
            selected === chip.key
              ? {
                  borderColor: 'var(--green-border)',
                  color: 'var(--green)',
                  background: 'var(--green-glow)',
                }
              : undefined
          }
        >
          <span>{chip.icon}</span>
          {chip.label}
        </button>
      ))}
    </div>
  )
}
