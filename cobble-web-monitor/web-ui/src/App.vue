<script setup>
import { computed, ref } from 'vue'
import SideNav from './components/SideNav.vue'
import SnapshotsPage from './pages/SnapshotsPage.vue'
import InspectPage from './pages/InspectPage.vue'

const activeMain = ref('snapshots')
const inspectExpanded = ref(true)
const activeInspectSub = ref('lookup')

const pageTitle = computed(() => {
  if (activeMain.value === 'snapshots') {
    return 'Snapshot list'
  }
  return activeInspectSub.value === 'lookup' ? 'Inspect · Lookup' : 'Inspect · Scan'
})

function selectMain(main) {
  activeMain.value = main
}

function toggleInspect() {
  inspectExpanded.value = !inspectExpanded.value
  if (inspectExpanded.value) {
    activeMain.value = 'inspect'
  } else if (activeMain.value === 'inspect') {
    activeMain.value = 'snapshots'
  }
}

function selectInspectSub(sub) {
  activeMain.value = 'inspect'
  activeInspectSub.value = sub
  inspectExpanded.value = true
}

function openLookupFromScan() {
  selectInspectSub('lookup')
}

const buildVersion = import.meta.env.VITE_COBBLE_VERSION || 'unknown'
const buildCommit = import.meta.env.VITE_COBBLE_COMMIT || 'unknown'
</script>

<template>
  <div class="min-h-screen bg-stone-200 text-stone-900">
    <div class="mx-auto flex min-h-screen max-w-[1400px]">
      <SideNav
        :active-main="activeMain"
        :inspect-expanded="inspectExpanded"
        :active-inspect-sub="activeInspectSub"
        @select-main="selectMain"
        @toggle-inspect="toggleInspect"
        @select-inspect-sub="selectInspectSub"
      />

      <main class="flex-1 bg-white px-6 py-6">
        <header class="mb-5 border-b border-stone-200 pb-4">
          <h2 class="text-xl font-semibold text-stone-800">{{ pageTitle }}</h2>
          <p class="info-text mt-1">cobble-ui version {{ buildVersion }} · commit {{ buildCommit }}</p>
        </header>

        <SnapshotsPage v-if="activeMain === 'snapshots'" />

        <InspectPage
          v-else-if="activeInspectSub === 'lookup'"
          mode="lookup"
          @open-lookup="openLookupFromScan"
        />

        <InspectPage
          v-else
          mode="scan"
          @open-lookup="openLookupFromScan"
        />
      </main>
    </div>
  </div>
</template>
