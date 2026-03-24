<script setup>
import { computed, onMounted, ref } from 'vue'
import { fetchMeta, fetchSnapshots, switchToCurrentMode, switchToSnapshotMode } from '../api'
import { formatTimestamp } from '../utils'

const loading = ref(false)
const switching = ref(false)
const error = ref('')
const snapshots = ref([])
const meta = ref(null)
const lastUpdatedAt = ref('')

const selectedMode = computed(() => meta.value?.read_mode || 'unknown')
const selectedSnapshotId = computed(() => meta.value?.configured_snapshot_id ?? null)
const currentSnapshotId = computed(() => meta.value?.current_global_snapshot_id ?? null)

async function loadData() {
  loading.value = true
  error.value = ''
  try {
    const [metaResp, snapshotsResp] = await Promise.all([fetchMeta(), fetchSnapshots()])
    meta.value = metaResp
    snapshots.value = snapshotsResp.snapshots || []
    lastUpdatedAt.value = formatTimestamp()
  } catch (err) {
    error.value = err.message || 'Failed to load snapshots'
  } finally {
    loading.value = false
  }
}

async function switchCurrent() {
  switching.value = true
  error.value = ''
  try {
    await switchToCurrentMode()
    await loadData()
  } catch (err) {
    error.value = err.message || 'Failed to switch mode'
  } finally {
    switching.value = false
  }
}

async function switchSnapshot(id) {
  switching.value = true
  error.value = ''
  try {
    await switchToSnapshotMode(id)
    await loadData()
  } catch (err) {
    error.value = err.message || 'Failed to switch mode'
  } finally {
    switching.value = false
  }
}

async function toggleTracking(target) {
  if (switching.value) {
    return
  }
  if (target === 'current') {
    await switchCurrent()
    return
  }
  await switchSnapshot(target)
}

onMounted(loadData)
</script>

<template>
  <section class="space-y-4">
    <div class="card">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 class="text-lg font-semibold text-stone-800">Snapshot tracking</h2>
          <p class="info-text mt-1">
            Tracking mode: <span class="font-medium text-stone-700">{{ selectedMode }}</span>
            <span v-if="selectedMode === 'snapshot' && selectedSnapshotId !== null"> · snapshot {{ selectedSnapshotId }}</span>
            <span> · updated {{ lastUpdatedAt || '-' }}</span>
          </p>
        </div>
        <button class="btn" :disabled="loading || switching" @click="loadData">Refresh</button>
      </div>
      <p v-if="error" class="mt-3 text-sm text-red-600">{{ error }}</p>
    </div>

    <div class="card overflow-x-auto">
      <table class="min-w-full divide-y divide-stone-200 text-sm">
        <thead>
          <tr class="text-left text-stone-500">
            <th class="px-2 py-2">Snapshot ID</th>
            <th class="px-2 py-2">Buckets</th>
            <th class="px-2 py-2">Shard snapshots</th>
            <th class="px-2 py-2">Current</th>
            <th class="px-2 py-2">Tracking</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-stone-200">
          <tr>
            <td class="px-2 py-2">(current mode)</td>
            <td class="px-2 py-2">-</td>
            <td class="px-2 py-2">-</td>
            <td class="px-2 py-2"><span class="text-stone-700">{{ currentSnapshotId ?? '-' }}</span></td>
            <td class="px-2 py-2">
              <label class="inline-flex items-center gap-2 text-sm text-stone-700">
                <input
                  type="checkbox"
                  class="h-4 w-4 accent-coffee-700"
                  :checked="selectedMode === 'current'"
                  :disabled="switching"
                  @change="toggleTracking('current')"
                />
                tracking
              </label>
            </td>
          </tr>
          <tr v-for="snapshot in snapshots" :key="snapshot.id">
            <td class="px-2 py-2 font-mono">{{ snapshot.id }}</td>
            <td class="px-2 py-2">{{ snapshot.total_buckets }}</td>
            <td class="px-2 py-2">{{ snapshot.shard_snapshot_count }}</td>
            <td class="px-2 py-2">
              <span v-if="snapshot.is_current" class="tag">yes</span>
              <span v-else class="text-stone-400">no</span>
            </td>
            <td class="px-2 py-2">
              <label class="inline-flex items-center gap-2 text-sm text-stone-700">
                <input
                  type="checkbox"
                  class="h-4 w-4 accent-coffee-700"
                  :checked="selectedMode === 'snapshot' && selectedSnapshotId === snapshot.id"
                  :disabled="switching"
                  @change="toggleTracking(snapshot.id)"
                />
                tracking
              </label>
            </td>
          </tr>
          <tr v-if="!loading && snapshots.length === 0">
            <td colspan="5" class="px-2 py-4 text-center text-stone-500">No snapshots found.</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>
