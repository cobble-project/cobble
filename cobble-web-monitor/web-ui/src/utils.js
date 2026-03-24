export function encodeUtf8ToBase64(value) {
  try {
    const bytes = new TextEncoder().encode(value)
    let binary = ''
    for (const byte of bytes) {
      binary += String.fromCharCode(byte)
    }
    return btoa(binary)
  } catch {
    return ''
  }
}

export async function copyText(text) {
  if (text === null || text === undefined) {
    throw new Error('Nothing to copy')
  }
  const value = String(text)
  if (navigator?.clipboard?.writeText) {
    await navigator.clipboard.writeText(value)
    return
  }
  const textarea = document.createElement('textarea')
  textarea.value = value
  textarea.style.position = 'fixed'
  textarea.style.left = '-9999px'
  document.body.appendChild(textarea)
  textarea.select()
  const ok = document.execCommand('copy')
  document.body.removeChild(textarea)
  if (!ok) {
    throw new Error('Clipboard copy failed')
  }
}

export function shortB64(value) {
  if (!value) {
    return ''
  }
  if (value.length <= 36) {
    return value
  }
  return `${value.slice(0, 16)}...${value.slice(-12)}`
}

export function formatTimestamp(date = new Date()) {
  const pad = (num) => String(num).padStart(2, '0')
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
}

export function safeLocalStorageSet(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value))
  } catch {
    // Ignore persistence errors
  }
}

export function safeLocalStorageGet(key, fallback) {
  try {
    const raw = localStorage.getItem(key)
    if (!raw) {
      return fallback
    }
    return JSON.parse(raw)
  } catch {
    return fallback
  }
}
