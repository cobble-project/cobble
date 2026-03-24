const jsonHeaders = {
  'Content-Type': 'application/json',
}

async function request(path, options = {}) {
  const resp = await fetch(path, options)
  const data = await resp.json().catch(() => ({}))
  if (!resp.ok) {
    const message = data?.error || `HTTP ${resp.status}`
    throw new Error(message)
  }
  return data
}

export async function fetchMeta() {
  return request('/api/v1/meta')
}

export async function fetchSnapshots() {
  return request('/api/v1/snapshots')
}

export async function switchToCurrentMode() {
  return request('/api/v1/mode', {
    method: 'POST',
    headers: jsonHeaders,
    body: JSON.stringify({ mode: 'current' }),
  })
}

export async function switchToSnapshotMode(snapshotId) {
  return request('/api/v1/mode', {
    method: 'POST',
    headers: jsonHeaders,
    body: JSON.stringify({ mode: 'snapshot', snapshot_id: snapshotId }),
  })
}

export async function inspectLookup({ bucket, keys, keysB64, lookupItems }) {
  const query = new URLSearchParams()
  query.set('mode', 'lookup')
  if (bucket !== null && bucket !== undefined) {
    query.set('bucket', String(bucket))
  }
  if (Array.isArray(lookupItems) && lookupItems.length > 0) {
    query.set('lookup_items', JSON.stringify(lookupItems))
  } else if (Array.isArray(keysB64) && keysB64.length > 0) {
    query.set('keys_b64', keysB64.join(','))
  } else {
    query.set('keys', (keys || []).join(','))
  }
  return request(`/api/v1/inspect?${query.toString()}`)
}

export async function inspectScan({ bucket, prefix, prefixB64, startAfter, startAfterB64, limit }) {
  const query = new URLSearchParams()
  query.set('mode', 'scan')
  query.set('bucket', String(bucket))
  if (prefixB64 !== null && prefixB64 !== undefined) {
    query.set('prefix_b64', prefixB64)
  } else {
    query.set('prefix', prefix)
  }
  query.set('limit', String(limit))
  if (startAfterB64) {
    query.set('start_after_b64', startAfterB64)
  } else if (startAfter) {
    query.set('start_after', startAfter)
  }
  return request(`/api/v1/inspect?${query.toString()}`)
}
