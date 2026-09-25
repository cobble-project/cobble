<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { fetchTables, inspectTable } from '../api'
import { formatTimestamp, safeLocalStorageGet, safeLocalStorageSet } from '../utils'

const props = defineProps({ mode: { type: String, required: true } })
const saved = safeLocalStorageGet('cobble-web-monitor-table-inspect-v1', {})
const tables = ref([])
const snapshotId = ref(null)
const tableName = ref(saved.tableName || '')
const selectedFields = ref(Array.isArray(saved.selectedFields) ? saved.selectedFields : [])
const keyInputs = ref(saved.keyInputs && typeof saved.keyInputs === 'object' ? saved.keyInputs : {})
const bucket = ref(Number.isInteger(saved.bucket) ? saved.bucket : 0)
const limit = ref(Number.isInteger(saved.limit) ? saved.limit : 20)
const autoRefresh = ref(saved.autoRefresh !== false)
const busy = ref(false)
const error = ref('')
const lastUpdated = ref('')
const lookup = ref(null)
const lookupAttempted = ref(false)
const scan = ref(null)
const cursor = ref(null)
const columnMenu = ref(null)
const draftFields = ref([])
const resultFields = ref([])
let timer = null
let generation = 0
let disposed = false
let fieldsTable = tableName.value
let firstLoad = true

const selectedTable = computed(() => tables.value.find((table) => table.name === tableName.value) || null)
const fields = computed(() => selectedTable.value?.schema?.fields || [])
const keyFields = computed(() => {
  const schema = selectedTable.value?.schema
  if (!schema) return []
  return schema.primary_key.map((id) => schema.fields.find((field) => field.id === id)).filter(Boolean)
})

function persist() {
  safeLocalStorageSet('cobble-web-monitor-table-inspect-v1', {
    tableName: tableName.value,
    selectedFields: selectedFields.value,
    keyInputs: keyInputs.value,
    bucket: bucket.value,
    limit: limit.value,
    autoRefresh: autoRefresh.value,
  })
}

function displayType(type) {
  if (!type) return ''
  if (type.kind === 'decimal') return `decimal(${type.precision}, ${type.scale})`
  if (type.kind === 'list') return `list<${displayType(type.element_type)}>`
  return `${type.kind}${type.nullable ? '?' : ''}`
}

function displayValue(value) {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'object' && 'base64' in value) return `base64: ${value.base64}`
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function inputHint(type) {
  if (type.kind === 'binary') return 'Base64 text'
  if (type.kind === 'timestamp') return '{"seconds":"0","nanos":0}'
  if (type.kind === 'date') return 'Days since epoch'
  if (type.kind === 'time') return 'Nanoseconds since midnight'
  if (type.kind === 'decimal') return 'Exact decimal, e.g. 1250.50'
  if (type.kind === 'int64') return 'Exact integer'
  return type.kind === 'string' ? 'Text' : 'Value'
}

function typedKey() {
  return keyFields.value.map((field) => {
    const raw = String(keyInputs.value[field.name] ?? '')
    if (field.logical_type.kind === 'boolean') {
      if (raw !== 'true' && raw !== 'false') throw new Error(`${field.name} must be true or false`)
      return raw === 'true'
    }
    if (field.logical_type.kind === 'timestamp') {
      try { return JSON.parse(raw) } catch { throw new Error(`${field.name} needs a timestamp object`) }
    }
    return raw
  })
}

function selectedNames() {
  return fields.value.filter((field) => selectedFields.value.includes(field.name)).map((field) => field.name)
}

async function loadTables() {
  const response = await fetchTables()
  const previousSnapshot = snapshotId.value
  snapshotId.value = response.snapshot_id
  tables.value = response.tables || []
  if (!tables.value.some((table) => table.name === tableName.value)) {
    tableName.value = tables.value[0]?.name || ''
  }
  const tableChanged = fieldsTable !== tableName.value
  if (tableChanged) {
    selectedFields.value = fields.value.map((field) => field.name)
    keyInputs.value = {}
    fieldsTable = tableName.value
  }
  const available = new Set(fields.value.map((field) => field.name))
  const previousFields = selectedFields.value
  selectedFields.value = selectedFields.value.filter((name) => available.has(name))
  if ((firstLoad && !Array.isArray(saved.selectedFields)) || (previousFields.length > 0 && selectedFields.value.length === 0)) {
    selectedFields.value = fields.value.map((field) => field.name)
  }
  firstLoad = false
  if (previousSnapshot !== null && previousSnapshot !== snapshotId.value) {
    cursor.value = null
    scan.value = null
    lookup.value = null
  }
  persist()
}

async function run({ next = false, quietEmptyKey = false } = {}) {
  if (busy.value || disposed) return
  const requestGeneration = generation
  busy.value = true
  error.value = ''
  try {
    await loadTables()
    if (requestGeneration !== generation) return
    if (!tableName.value) {
      lookup.value = null
      scan.value = null
      return
    }
    const names = selectedNames()
    if (names.length === 0) throw new Error('Select at least one column')
    if (props.mode === 'lookup') {
      if (quietEmptyKey && keyFields.value.some((field) => !String(keyInputs.value[field.name] ?? '').trim())) return
      lookupAttempted.value = true
      const response = await inspectTable({ table: tableName.value, mode: 'lookup', key: typedKey(), fields: names, snapshotId: snapshotId.value })
      if (requestGeneration !== generation) return
      lookup.value = response.lookup
      resultFields.value = response.fields
    } else {
      if (!Number.isInteger(bucket.value) || bucket.value < 0 || bucket.value > 65535) throw new Error('Enter a valid bucket')
      if (!Number.isInteger(limit.value) || limit.value < 1) throw new Error('Limit must be greater than 0')
      const response = await inspectTable({
        table: tableName.value, mode: 'scan', bucket: bucket.value,
        startAfter: next ? cursor.value : null, fields: names, limit: limit.value,
        snapshotId: snapshotId.value,
      })
      if (requestGeneration !== generation) return
      scan.value = response.scan
      resultFields.value = response.fields
      cursor.value = next ? cursor.value : null
    }
    lastUpdated.value = formatTimestamp()
  } catch (err) {
    if (requestGeneration === generation) error.value = err.message || 'Could not inspect table'
  } finally {
    busy.value = false
    if (requestGeneration !== generation) queueMicrotask(() => run({ quietEmptyKey: true }))
  }
}

function nextPage() {
  if (!scan.value?.has_more || !scan.value.next_start_after) return
  cursor.value = scan.value.next_start_after
  run({ next: true })
}

function resetResults() {
  generation += 1
  cursor.value = null
  scan.value = null
  lookup.value = null
  lookupAttempted.value = false
  resultFields.value = []
  persist()
}

function openColumns(event) {
  if (event.target.open) draftFields.value = [...selectedFields.value]
}

function applyColumns() {
  selectedFields.value = [...draftFields.value]
  columnMenu.value.open = false
  cursor.value = null
  persist()
  run({ quietEmptyKey: true })
}

function onTableChange() {
  fieldsTable = tableName.value
  selectedFields.value = fields.value.map((field) => field.name)
  keyInputs.value = {}
  resetResults()
}

function setTimer() {
  if (timer) clearInterval(timer)
  timer = autoRefresh.value ? setInterval(() => run({ next: Boolean(cursor.value), quietEmptyKey: true }), 10000) : null
  persist()
}

onMounted(() => {
  run({ quietEmptyKey: true })
  setTimer()
})
onBeforeUnmount(() => { disposed = true; generation += 1; if (timer) clearInterval(timer) })
watch(autoRefresh, setTimer)
watch(() => props.mode, () => { resetResults(); run({ quietEmptyKey: true }) })
watch([tableName, selectedFields, bucket, limit], persist, { deep: true })
watch(keyInputs, () => { lookup.value = null; lookupAttempted.value = false; persist() }, { deep: true })
</script>

<template>
  <div class="space-y-4">
    <div class="card space-y-4">
      <div class="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 class="text-lg font-semibold text-stone-800">Table · {{ mode }}</h3>
          <p class="info-text mt-1">Snapshot {{ snapshotId ?? '-' }} · updated {{ lastUpdated || '-' }}</p>
        </div>
        <div class="flex items-center gap-3">
          <label class="text-sm text-stone-600"><input v-model="autoRefresh" type="checkbox" class="mr-1 accent-coffee-700" />Auto refresh</label>
          <button class="btn" :disabled="busy" @click="run({ next: Boolean(cursor) })">Refresh</button>
        </div>
      </div>
      <p v-if="error" role="alert" class="text-sm text-red-600">{{ error }}</p>
      <p v-if="tables.length === 0 && !busy" class="text-sm text-stone-500">No tables in this snapshot.</p>

      <label v-if="tables.length" for="table-select" class="block text-sm text-stone-600">
        Table
        <select id="table-select" v-model="tableName" :disabled="busy" class="select mt-1 block w-full max-w-md" @change="onTableChange">
          <option v-for="table in tables" :key="table.name" :value="table.name">{{ table.name }}</option>
        </select>
      </label>

      <details v-if="selectedTable" ref="columnMenu" class="relative w-fit" @toggle="openColumns" @keydown.esc="columnMenu.open = false">
        <summary class="btn-secondary cursor-pointer">Visible columns ({{ selectedFields.length }})</summary>
        <div class="absolute left-0 z-10 mt-2 min-w-56 rounded-lg border border-stone-200 bg-white p-3 shadow-lg">
          <fieldset class="max-h-64 space-y-2 overflow-y-auto">
            <legend class="sr-only">Visible columns</legend>
            <label v-for="field in fields" :key="field.id" class="block text-sm text-stone-700">
              <input v-model="draftFields" type="checkbox" :disabled="busy" :value="field.name" class="mr-2 accent-coffee-700" />{{ field.name }}
            </label>
          </fieldset>
          <button class="btn mt-3 w-full" :disabled="busy || draftFields.length === 0" @click="applyColumns">Apply</button>
        </div>
      </details>

      <div v-if="selectedTable && mode === 'lookup'" class="grid gap-3 md:grid-cols-2">
        <label v-for="field in keyFields" :key="field.id" class="block text-sm text-stone-600">
          {{ field.name }} <span class="text-stone-400">({{ displayType(field.logical_type) }})</span>
          <input v-model="keyInputs[field.name]" :disabled="busy" class="input mt-1 block w-full" :placeholder="inputHint(field.logical_type)" @keyup.enter="run()" />
        </label>
        <div class="self-end"><button class="btn" :disabled="busy" @click="run()">Look up</button></div>
      </div>
      <div v-if="selectedTable && mode === 'scan'" class="flex flex-wrap items-end gap-3">
        <label for="table-bucket" class="text-sm text-stone-600">Bucket
          <input id="table-bucket" v-model.number="bucket" :disabled="busy" type="number" min="0" class="input mt-1 block w-32" @change="resetResults" />
        </label>
        <label for="table-limit" class="text-sm text-stone-600">Rows per page
          <input id="table-limit" v-model.number="limit" :disabled="busy" type="number" min="1" max="1000" class="input mt-1 block w-32" @change="resetResults" />
        </label>
        <button class="btn" :disabled="busy" @click="resetResults(); run()">Scan</button>
      </div>
    </div>

    <div v-if="selectedTable" class="card space-y-3 overflow-x-auto">
      <div v-if="mode === 'scan'" class="flex items-center justify-between text-sm">
        <span class="text-stone-600">{{ scan?.items?.length ?? 0 }} rows on this page</span>
        <button class="btn-secondary" :disabled="busy || !scan?.has_more" @click="nextPage">Next page</button>
      </div>
      <table class="min-w-full divide-y divide-stone-200 text-sm">
        <thead><tr class="text-left text-stone-500"><th v-for="name in resultFields" :key="name" class="px-2 py-2">{{ name }}<span class="block text-xs font-normal text-stone-400">{{ displayType(fields.find((field) => field.name === name)?.logical_type) }}</span></th></tr></thead>
        <tbody class="divide-y divide-stone-200">
          <tr v-for="(row, index) in (mode === 'lookup' ? (lookup ? [lookup] : []) : (scan?.items || []))" :key="index">
            <td v-for="(value, cellIndex) in row.values" :key="cellIndex" class="px-2 py-2 font-mono text-xs" :class="value === null ? 'text-stone-400 italic' : ''" :title="displayValue(value)">{{ displayValue(value) }}</td>
          </tr>
          <tr v-if="!busy && (mode === 'lookup' ? !lookup : !scan?.items?.length)">
            <td :colspan="Math.max(resultFields.length, 1)" class="px-2 py-4 text-center text-stone-500">{{ mode === 'lookup' ? (lookupAttempted ? 'No matching row.' : 'Enter a key to look up a row.') : 'No rows on this page.' }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
