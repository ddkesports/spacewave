import { describe, expect, it } from 'vitest'

import { frontmatterRecord, readFrontmatter } from './frontmatter.js'

const lf = [
  '---',
  'title: Hello',
  'date: 2026-10-01',
  'order: 3',
  'draft: false',
  'tags: [a, b]',
  '---',
  'Body text.',
  '',
].join('\n')

describe('readFrontmatter', () => {
  it('reads the fields and body of an LF file', () => {
    const parsed = readFrontmatter(lf)
    expect(frontmatterRecord(parsed)).toEqual({
      title: 'Hello',
      date: '2026-10-01',
      order: 3,
      draft: false,
      tags: ['a', 'b'],
    })
    expect(parsed.body).toBe('Body text.\n')
  })

  it('reads a CRLF file as a Windows checkout writes it', () => {
    const crlf = lf.replace(/\n/g, '\r\n')
    const parsed = readFrontmatter(crlf)
    expect(frontmatterRecord(parsed)).toEqual(
      frontmatterRecord(readFrontmatter(lf)),
    )
    expect(parsed.body).toBe('Body text.\r\n')

    // Offsets still point at the key and value in the CRLF text.
    for (const field of parsed.fields) {
      expect(
        crlf.slice(field.keyStart, field.keyStart + field.key.length),
      ).toBe(field.key)
      expect(crlf.slice(field.valueStart, field.valueEnd)).not.toContain('\r')
    }
    const date = parsed.fields.find((field) => field.key === 'date')
    expect(crlf.slice(date!.valueStart, date!.valueEnd)).toBe('2026-10-01')
  })

  it('keeps a file without a block as body', () => {
    expect(readFrontmatter('no block\n')).toEqual({
      fields: [],
      body: 'no block\n',
    })
  })
})
