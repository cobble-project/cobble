<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { fetchMeta, inspectLookup, inspectScan } from '../api'
import AutoRefreshControl from '../components/AutoRefreshControl.vue'
import { copyText, formatTimestamp, safeLocalStorageGet, safeLocalStorageSet, shortB64 } from '../utils'

const props = defineProps({
  mode: {
    type: String,
    required: true,
  },
})

const emit = defineEmits(['open-lookup'])

const cacheKey = 'cobble-web-monitor-inspect-cache-v6'
const persisted = safeLocalStorageGet(cacheKey, {
  scanBucket: 0,
  lookupRows: [],
  addRowBucket: 0,
  addRowKeyB64: '',
  scanPrefix: '',
  scanPrefixEncoding: 'utf8',
  scanLimit: 20,
  scanStartAfterB64: '',
  scanLastResult: null,
  autoRefreshEnabled: true,
  autoRefreshIntervalSeconds: 10,
  visibleColumnsText: '',
})

const scanBucket = ref(Number.isFinite(persisted.scanBucket) ? persisted.scanBucket : 0)
const lookupRows = ref(Array.isArray(persisted.lookupRows) ? persisted.lookupRows : [])
const addRowBucket = ref(Number.isFinite(persisted.addRowBucket) ? persisted.addRowBucket : 0)
const addRowKeyB64 = ref(typeof persisted.addRowKeyB64 === 'string' ? persisted.addRowKeyB64 : '')
const scanPrefix = ref(typeof persisted.scanPrefix === 'string' ? persisted.scanPrefix : '')
const scanPrefixEncoding = ref(
  persisted.scanPrefixEncoding === 'base64' ? 'base64' : 'utf8',
)
const scanLimit = ref(Number(persisted.scanLimit || 20))
const scanStartAfterB64 = ref(typeof persisted.scanStartAfterB64 === 'string' ? persisted.scanStartAfterB64 : '')
const visibleColumnsText = ref(typeof persisted.visibleColumnsText === 'string' ? persisted.visibleColumnsText : '')

const loading = ref(false)
const error = ref('')
const copyHint = ref('')
const readMode = ref('unknown')
const configuredSnapshotId = ref(null)
const currentSnapshotId = ref(null)
const lastUpdatedAt = ref('')

const lookupResult = ref([])
const lookupResultRowIndices = ref([])
const scanResult = ref(persisted.scanLastResult || null)

const autoRefresh = ref(Boolean(persisted.autoRefreshEnabled))
const refreshIntervalSeconds = ref(Number(persisted.autoRefreshIntervalSeconds || 10))
let refreshTimer = null

const highlightedRows = ref({})
const floatingMenu = ref({ open: false, x: 0, y: 0, actions: [] })

const modeLabel = computed(() => (props.mode === 'lookup' ? 'lookup' : 'scan'))

const visibleColumnIndices = computed(() => {
  const raw = visibleColumnsText.value
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part.length > 0)
  if (raw.length === 0) {
    return null
  }
  const values = []
  for (const part of raw) {
    const idx = Number(part)
    if (!Number.isInteger(idx) || idx < 0) {
      continue
    }
    values.push(idx)
  }
  return values.length > 0 ? values : null
})

const selectedColumnSet = computed(() => {
  const indices = visibleColumnIndices.value
  if (!indices) {
    return null
  }
  return new Set(indices)
})

const renderedColumnIndices = computed(() => {
  const set = selectedColumnSet.value
  const sourceRows = props.mode === 'lookup'
    ? lookupResult.value.map((item) => item.value || [])
    : (scanResult.value?.items || []).map((item) => item.columns || [])
  const maxColumns = sourceRows.reduce((max, row) => Math.max(max, row.length), 0)
  if (set) {
    return Array.from(set).filter((idx) => idx < maxColumns)
  }
  return Array.from({ length: maxColumns }, (_, idx) => idx)
})

function closeMenus() {
  floatingMenu.value = { open: false, x: 0, y: 0, actions: [] }
}

function openFloatingMenu(event, actions) {
  const rect = event.currentTarget.getBoundingClientRect()
  const menuWidth = 208
  let x = rect.left
  if (x + menuWidth > window.innerWidth - 8) {
    x = window.innerWidth - menuWidth - 8
  }
  x = Math.max(8, x)
  const estimatedHeight = Math.max(actions.length, 1) * 34 + 12
  let y = rect.bottom + 6
  if (y + estimatedHeight > window.innerHeight - 8) {
    y = Math.max(8, rect.top - estimatedHeight - 6)
  }
  floatingMenu.value = { open: true, x, y, actions }
}

async function handleMenuAction(action) {
  closeMenus()
  try {
    await action.onClick()
  } catch (err) {
    error.value = err.message || 'Action failed'
  }
}

function persistState() {
  safeLocalStorageSet(cacheKey, {
    scanBucket: scanBucket.value,
    lookupRows: lookupRows.value,
    addRowBucket: addRowBucket.value,
    addRowKeyB64: addRowKeyB64.value,
    scanPrefix: scanPrefix.value,
    scanPrefixEncoding: scanPrefixEncoding.value,
    scanLimit: scanLimit.value,
    scanStartAfterB64: scanStartAfterB64.value,
    scanLastResult: scanResult.value,
    autoRefreshEnabled: autoRefresh.value,
    autoRefreshIntervalSeconds: refreshIntervalSeconds.value,
    visibleColumnsText: visibleColumnsText.value,
  })
}

function rowKeyLookup(item) {
  return `${item.bucket}:${item.key_b64}`
}

function rowKeyScan(item) {
  return item.key_b64
}

function rowFingerprint(columns) {
  return JSON.stringify((columns || []).map((column) => (column ? column.b64 : null)))
}

function flashRow(rowKey) {
  highlightedRows.value = { ...highlightedRows.value, [rowKey]: true }
  setTimeout(() => {
    const next = { ...highlightedRows.value }
    delete next[rowKey]
    highlightedRows.value = next
  }, 1400)
}

function rowClass(rowKey) {
  return highlightedRows.value[rowKey] ? 'row-flash' : ''
}

function normalizeBucket(value, fallback = 0) {
  const bucket = Number(value)
  if (!Number.isInteger(bucket) || bucket < 0) {
    return fallback
  }
  return bucket
}

function isValidBase64(value) {
  if (!value || typeof value !== 'string') {
    return false
  }
  try {
    atob(value)
    return true
  } catch {
    return false
  }
}

function buildLookupPayload() {
  const items = []
  const rowIndices = []
  lookupRows.value.forEach((row, idx) => {
    const keyB64 = (row.keyB64 || '').trim()
    if (!isValidBase64(keyB64)) {
      return
    }
    items.push({
      bucket: normalizeBucket(row.bucket, 0),
      key_b64: keyB64,
    })
    rowIndices.push(idx)
  })
  return { items, rowIndices }
}

function addLookupRowFromInput() {
  error.value = ''
  const keyB64 = addRowKeyB64.value.trim()
  if (!isValidBase64(keyB64)) {
    error.value = 'Add row requires valid key base64.'
    return
  }
  lookupRows.value.push({
    bucket: normalizeBucket(addRowBucket.value, 0),
    keyB64,
  })
  addRowKeyB64.value = ''
  persistState()
}

function removeLookupRowByResultIndex(resultIndex) {
  const sourceIndex = lookupResultRowIndices.value[resultIndex]
  if (sourceIndex === undefined) {
    return
  }
  lookupRows.value.splice(sourceIndex, 1)
  lookupResult.value.splice(resultIndex, 1)
  lookupResultRowIndices.value.splice(resultIndex, 1)
  lookupResultRowIndices.value = lookupResultRowIndices.value.map((idx) =>
    idx > sourceIndex ? idx - 1 : idx,
  )
  persistState()
}

function setLookupRowFromScan(item) {
  const keyB64 = item.key_b64
  const bucket = normalizeBucket(scanBucket.value, 0)
  const exists = lookupRows.value.some(
    (row) => row.keyB64 === keyB64 && normalizeBucket(row.bucket, 0) === bucket,
  )
  if (!exists) {
    lookupRows.value.push({ bucket, keyB64 })
    persistState()
  }
  emit('open-lookup')
}

async function copyAndHint(text, okMessage) {
  await copyText(text)
  copyHint.value = okMessage
  setTimeout(() => {
    if (copyHint.value === okMessage) {
      copyHint.value = ''
    }
  }, 1200)
}

async function copyLookupKey(item) {
  await copyAndHint(item.key_b64, 'Copied key base64')
}

async function copyLookupKeyUtf8(item) {
  const value = item.key_utf8 ?? '[non-utf8]'
  await copyAndHint(value, 'Copied key utf8')
}

async function copyScanKey(item) {
  await copyAndHint(item.key_b64, 'Copied key base64')
}

async function copyScanKeyUtf8(item) {
  const value = item.key_utf8 ?? '[non-utf8]'
  await copyAndHint(value, 'Copied key utf8')
}

async function copyColumn(column) {
  if (!column) {
    await copyAndHint('null', 'Copied null')
    return
  }
  await copyAndHint(column.b64, 'Copied column base64')
}

async function copyColumnUtf8(column) {
  if (!column) {
    await copyAndHint('null', 'Copied null')
    return
  }
  const value = column.utf8 ?? '[non-utf8]'
  await copyAndHint(value, 'Copied column utf8')
}

function displayColumnBase64(column) {
  if (!column) {
    return 'null'
  }
  return column.b64
}

function openLookupKeyMenu(event, item, resultIdx) {
  openFloatingMenu(event, [
    {
      label: 'Copy key b64',
      onClick: () => copyLookupKey(item),
    },
    {
      label: 'Copy key utf8',
      onClick: () => copyLookupKeyUtf8(item),
    },
    {
      label: 'Remove row',
      danger: true,
      onClick: () => removeLookupRowByResultIndex(resultIdx),
    },
  ])
}

function openLookupColumnMenu(event, item, colIdx) {
  openFloatingMenu(event, [
    {
      label: `Copy col[${colIdx}] b64`,
      onClick: () => copyColumn(item.value?.[colIdx] ?? null),
    },
    {
      label: `Copy col[${colIdx}] utf8`,
      onClick: () => copyColumnUtf8(item.value?.[colIdx] ?? null),
    },
  ])
}

function openScanKeyMenu(event, item) {
  openFloatingMenu(event, [
    {
      label: 'Copy key b64',
      onClick: () => copyScanKey(item),
    },
    {
      label: 'Copy key utf8',
      onClick: () => copyScanKeyUtf8(item),
    },
    {
      label: 'Add to lookup',
      onClick: () => setLookupRowFromScan(item),
    },
  ])
}

function openScanColumnMenu(event, item, colIdx) {
  openFloatingMenu(event, [
    {
      label: `Copy col[${colIdx}] b64`,
      onClick: () => copyColumn(item.columns?.[colIdx] ?? null),
    },
    {
      label: `Copy col[${colIdx}] utf8`,
      onClick: () => copyColumnUtf8(item.columns?.[colIdx] ?? null),
    },
  ])
}

function applyLookupResult(newItems) {
  const previous = new Map(
    lookupResult.value.map((item) => [rowKeyLookup(item), rowFingerprint(item.value || [])]),
  )
  lookupResult.value = newItems
  newItems.forEach((item) => {
    const key = rowKeyLookup(item)
    const nextFingerprint = rowFingerprint(item.value || [])
    if (previous.has(key) && previous.get(key) !== nextFingerprint) {
      flashRow(key)
    }
  })
}

function applyScanResult(newScan) {
  const oldItems = scanResult.value?.items || []
  const previous = new Map(oldItems.map((item) => [rowKeyScan(item), rowFingerprint(item.columns || [])]))
  scanResult.value = newScan
  const nextItems = newScan?.items || []
  nextItems.forEach((item) => {
    const key = rowKeyScan(item)
    const nextFingerprint = rowFingerprint(item.columns || [])
    if (previous.has(key) && previous.get(key) !== nextFingerprint) {
      flashRow(key)
    }
  })
}

async function loadMetaOnly() {
  const meta = await fetchMeta()
  readMode.value = meta.read_mode
  configuredSnapshotId.value = meta.configured_snapshot_id
  currentSnapshotId.value = meta.current_global_snapshot_id
}

async function runLookup() {
  const payload = buildLookupPayload()
  lookupResultRowIndices.value = payload.rowIndices
  if (payload.items.length === 0) {
    lookupResult.value = []
    error.value = 'Please add at least one valid base64 lookup row.'
    return
  }
  const resp = await inspectLookup({ lookupItems: payload.items })
  applyLookupResult(resp.lookup || [])
  lastUpdatedAt.value = formatTimestamp()
}

async function runScan({ resetCursor }) {
  if (resetCursor) {
    scanStartAfterB64.value = ''
  }

  const resp = await inspectScan({
    bucket: normalizeBucket(scanBucket.value, 0),
    prefix: scanPrefixEncoding.value === 'utf8' ? scanPrefix.value : '',
    prefixB64: scanPrefixEncoding.value === 'base64' ? scanPrefix.value : undefined,
    startAfterB64: scanStartAfterB64.value || undefined,
    limit: scanLimit.value,
  })
  applyScanResult(resp.scan || null)
  lastUpdatedAt.value = formatTimestamp()
}

async function runInspect({ resetCursor = false } = {}) {
  loading.value = true
  error.value = ''
  try {
    await loadMetaOnly()
    if (props.mode === 'lookup') {
      await runLookup()
    } else {
      await runScan({ resetCursor })
    }
    persistState()
  } catch (err) {
    error.value = err.message || 'Refresh failed'
  } finally {
    loading.value = false
  }
}

function nextPage() {
  if (scanResult.value?.has_more && scanResult.value?.next_start_after_b64) {
    scanStartAfterB64.value = scanResult.value.next_start_after_b64
    runInspect({ resetCursor: false })
  }
}

function scheduleRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
  if (!autoRefresh.value) {
    persistState()
    return
  }
  const intervalMs = Number(refreshIntervalSeconds.value || 10) * 1000
  refreshTimer = setInterval(() => {
    runInspect({ resetCursor: false })
  }, intervalMs)
  persistState()
}

onMounted(() => {
  runInspect({ resetCursor: false })
  scheduleRefresh()
  window.addEventListener('click', closeMenus)
  window.addEventListener('scroll', closeMenus, true)
  window.addEventListener('resize', closeMenus)
})

onBeforeUnmount(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
  }
  window.removeEventListener('click', closeMenus)
  window.removeEventListener('scroll', closeMenus, true)
  window.removeEventListener('resize', closeMenus)
})

watch([autoRefresh, refreshIntervalSeconds], scheduleRefresh)
watch([lookupRows, addRowBucket, addRowKeyB64, scanPrefix, scanPrefixEncoding, scanLimit, scanBucket, visibleColumnsText], persistState, {
  deep: true,
})
watch(refreshIntervalSeconds, (value) => {
  const allowed = new Set([5, 10, 30, 60, 300, 600])
  if (!allowed.has(value)) {
    refreshIntervalSeconds.value = 10
  }
})
watch(
  () => props.mode,
  () => {
    closeMenus()
    runInspect({ resetCursor: false })
  },
)
watch(
  () => scanResult.value,
  () => {
    persistState()
  },
  { deep: true },
)
</script>

<template>
  <section class="space-y-4">
    <div class="card space-y-3">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 class="text-lg font-semibold text-stone-800">Inspect · {{ modeLabel }}</h3>
          <p class="info-text mt-1">
            Snapshot:
            <span class="font-medium text-stone-700">{{ readMode === 'snapshot' ? configuredSnapshotId : currentSnapshotId }}</span>
            <span> (mode={{ readMode }})</span>
            <span> · updated {{ lastUpdatedAt || '-' }}</span>
            <span v-if="error" class="text-red-600"> · error {{ error }}</span>
          </p>
        </div>

        <button class="btn" :disabled="loading" @click="runInspect({ resetCursor: false })">Refresh</button>
      </div>

      <div class="flex flex-wrap items-center gap-3">
        <AutoRefreshControl
          :enabled="autoRefresh"
          :interval-seconds="refreshIntervalSeconds"
          @update:enabled="autoRefresh = $event"
          @update:interval-seconds="refreshIntervalSeconds = $event"
        />

        <label class="text-sm text-stone-600">
          Visible columns (comma separated)
          <input v-model="visibleColumnsText" class="input ml-2 w-60" placeholder="e.g. 0,2,3" />
        </label>
      </div>

      <div v-if="mode === 'lookup'" class="space-y-2">
        <div class="grid gap-2 md:grid-cols-[120px_1fr_120px]">
          <input v-model.number="addRowBucket" type="number" min="0" class="input" placeholder="bucket" />
          <input v-model="addRowKeyB64" class="input" placeholder="key base64" />
          <button class="btn-secondary" @click="addLookupRowFromInput">Add row</button>
        </div>
      </div>

      <div v-else class="grid grid-cols-[120px_120px_1fr_92px] items-end gap-3">
        <label class="text-sm text-stone-600">
          Bucket
          <input v-model.number="scanBucket" type="number" min="0" class="input mt-1 w-full" />
        </label>

        <label class="text-sm text-stone-600">
          Prefix type
          <select v-model="scanPrefixEncoding" class="select mt-1 w-full">
            <option value="utf8">utf8</option>
            <option value="base64">base64</option>
          </select>
        </label>

        <label class="text-sm text-stone-600">
          Prefix (empty means full scan)
          <input
            v-model="scanPrefix"
            class="input mt-1 w-full"
            :placeholder="scanPrefixEncoding === 'base64' ? 'prefix as base64' : 'prefix as utf8'"
          />
        </label>

        <label class="text-sm text-stone-600">
          Limit
          <input v-model.number="scanLimit" type="number" min="1" max="1000" class="input mt-1 w-full" />
        </label>
      </div>

      <p v-if="copyHint" class="text-sm text-green-700">{{ copyHint }}</p>
    </div>

    <div v-if="mode === 'lookup'" class="card overflow-x-auto">
      <table class="min-w-full divide-y divide-stone-200 text-sm">
        <thead>
          <tr class="text-left text-stone-500">
            <th class="px-2 py-2">Bucket</th>
            <th class="px-2 py-2">Key</th>
            <th v-for="colIdx in renderedColumnIndices" :key="`lookup-col-head-${colIdx}`" class="px-2 py-2">col[{{ colIdx }}]</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-stone-200">
          <tr
            v-for="(item, resultIdx) in lookupResult"
            :key="`${item.bucket}-${item.key_b64}-${resultIdx}`"
            :class="rowClass(rowKeyLookup(item))"
          >
            <td class="px-2 py-2">{{ item.bucket }}</td>
            <td class="px-2 py-2">
              <div class="inline-flex items-center gap-1">
                <span class="font-mono" :title="item.key_b64">{{ shortB64(item.key_b64) }}</span>
                <button class="btn-secondary px-2 py-1" @click.stop="openLookupKeyMenu($event, item, resultIdx)">⋯</button>
              </div>
            </td>
            <td v-for="colIdx in renderedColumnIndices" :key="`${item.key_b64}-col-${colIdx}`" class="px-2 py-2">
              <div class="inline-flex items-center gap-1">
                <span class="font-mono text-xs">{{ displayColumnBase64(item.value?.[colIdx] ?? null) }}</span>
                <button class="btn-secondary px-2 py-1" @click.stop="openLookupColumnMenu($event, item, colIdx)">⋯</button>
              </div>
            </td>
          </tr>
          <tr v-if="!loading && lookupResult.length === 0">
            <td :colspan="2 + renderedColumnIndices.length" class="px-2 py-4 text-center text-stone-500">No lookup result.</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-else class="card space-y-3">
      <div class="flex items-center justify-between text-sm">
        <span class="text-stone-600">has_more: {{ scanResult?.has_more ? 'true' : 'false' }}</span>
        <button class="btn-secondary" :disabled="!scanResult?.has_more || loading" @click="nextPage">Next page</button>
      </div>

      <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-stone-200 text-sm">
          <thead>
            <tr class="text-left text-stone-500">
              <th class="px-2 py-2">Key</th>
              <th v-for="colIdx in renderedColumnIndices" :key="`scan-col-head-${colIdx}`" class="px-2 py-2">col[{{ colIdx }}]</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-stone-200">
            <tr
              v-for="item in scanResult?.items || []"
              :key="item.key_b64"
              :class="rowClass(rowKeyScan(item))"
            >
              <td class="px-2 py-2">
                <div class="inline-flex items-center gap-1">
                  <span class="font-mono" :title="item.key_b64">{{ shortB64(item.key_b64) }}</span>
                  <button class="btn-secondary px-2 py-1" @click.stop="openScanKeyMenu($event, item)">⋯</button>
                </div>
              </td>
              <td v-for="colIdx in renderedColumnIndices" :key="`${item.key_b64}-col-${colIdx}`" class="px-2 py-2">
                <div class="inline-flex items-center gap-1">
                  <span class="font-mono text-xs">{{ displayColumnBase64(item.columns?.[colIdx] ?? null) }}</span>
                  <button class="btn-secondary px-2 py-1" @click.stop="openScanColumnMenu($event, item, colIdx)">⋯</button>
                </div>
              </td>
            </tr>
            <tr v-if="!loading && (!scanResult || (scanResult.items || []).length === 0)">
              <td :colspan="1 + renderedColumnIndices.length" class="px-2 py-4 text-center text-stone-500">No scan result.</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <teleport to="body">
      <div
        v-if="floatingMenu.open"
        class="fixed z-[1000] w-52 rounded-md border border-stone-200 bg-white p-1 shadow-lg"
        :style="{ left: `${floatingMenu.x}px`, top: `${floatingMenu.y}px` }"
      >
        <button
          v-for="(action, idx) in floatingMenu.actions"
          :key="`menu-action-${idx}`"
          class="w-full rounded px-2 py-1 text-left text-sm hover:bg-stone-100"
          :class="action.danger ? 'text-red-600' : 'text-stone-700'"
          @click.stop="handleMenuAction(action)"
        >
          {{ action.label }}
        </button>
      </div>
    </teleport>
  </section>
</template>
