import { mkdtempSync, readFileSync, symlinkSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRequire } from 'node:module'

const ssePath = process.argv[2] ?? join(import.meta.dirname, 'fixtures/frame-shapes.sse')
const temp = mkdtempSync(join(tmpdir(), 'mina-frame-shapes-'))
try {
  symlinkSync(join(import.meta.dirname, '../apps/web/node_modules'), join(temp, 'node_modules'), 'dir')
  const require = createRequire(join(temp, 'runner.cjs'))
  const core = require('@xinjiyuan97/chat-core')
  const applyEvent = core.applyEvent
  if (typeof applyEvent !== 'function') throw new Error('chat-core applyEvent export missing')

  const events = []
  for (const line of readFileSync(ssePath, 'utf8').split('\n')) {
    const trimmed = line.trim()
    if (!trimmed.startsWith('data:')) continue
    const payload = trimmed.slice(5).trim()
    if (!payload || payload === '[DONE]') continue
    events.push(JSON.parse(payload))
  }

  let message = { id: 'replay', role: 'assistant', parts: [], status: 'streaming' }
  for (const event of events) message = applyEvent(message, event)
  const permission = message.parts.find((part) => part.type === 'permission')
  if (!permission) throw new Error('permission part missing')
  const requestId = permission.request?.id
  const serverRequest = events.find((event) => event.type === 'permission-request')?.request?.id
  if (requestId !== serverRequest) throw new Error(`permission id mismatch: ${requestId} !== ${serverRequest}`)
  for (const part of message.parts) {
    if (!part || typeof part.type !== 'string') throw new Error('invalid part shape')
    if (part.type === 'permission' && (!part.request || typeof part.request.id !== 'string')) {
      throw new Error('invalid permission part shape')
    }
  }
  console.log(`解析出 ${events.length} 个事件（${ssePath}）`)
  console.log(`最终 parts: ${message.parts.map((part) => part.type).join(', ')}`)
  console.log(`permission.request.id: ${requestId}`)
  console.log('结果: PASS（无异常；permission request.id 与服务端一致）')
} finally {
  rmSync(temp, { recursive: true, force: true })
}
