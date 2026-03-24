<script setup>
const props = defineProps({
  activeMain: { type: String, required: true },
  inspectExpanded: { type: Boolean, required: true },
  activeInspectSub: { type: String, required: true },
})

const emit = defineEmits(['select-main', 'toggle-inspect', 'select-inspect-sub'])
</script>

<template>
  <aside class="sidebar">
    <div class="sidebar-header">
      <h1 class="text-base font-semibold text-white">Cobble Monitor</h1>
    </div>

    <nav class="space-y-2">
      <button class="sidebar-btn" :class="activeMain === 'snapshots' ? 'sidebar-btn-active' : ''" @click="emit('select-main', 'snapshots')">
        Snapshot list
      </button>

      <button class="sidebar-btn" :class="activeMain === 'inspect' ? 'sidebar-btn-active' : ''" @click="emit('toggle-inspect')">
        Inspect
        <span class="ml-auto text-xs">{{ inspectExpanded ? '▾' : '▸' }}</span>
      </button>

      <div v-if="inspectExpanded" class="ml-3 space-y-1">
        <button
          class="sidebar-sub-btn"
          :class="activeMain === 'inspect' && activeInspectSub === 'lookup' ? 'sidebar-sub-btn-active' : ''"
          @click="emit('select-inspect-sub', 'lookup')"
        >
          Lookup
        </button>
        <button
          class="sidebar-sub-btn"
          :class="activeMain === 'inspect' && activeInspectSub === 'scan' ? 'sidebar-sub-btn-active' : ''"
          @click="emit('select-inspect-sub', 'scan')"
        >
          Scan
        </button>
      </div>
    </nav>
  </aside>
</template>
