<script setup>
const props = defineProps({
  enabled: { type: Boolean, required: true },
  intervalSeconds: { type: Number, required: true },
})

const emit = defineEmits(['update:enabled', 'update:intervalSeconds'])

const options = [5, 10, 30, 60, 300, 600]

function labelFor(value) {
  if (value < 60) {
    return `${value}s`
  }
  return `${Math.floor(value / 60)}min`
}
</script>

<template>
  <div class="flex flex-wrap items-center gap-3">
    <label class="inline-flex items-center gap-2 text-sm text-stone-600">
      <input
        type="checkbox"
        class="h-4 w-4 accent-coffee-700"
        :checked="enabled"
        @change="emit('update:enabled', $event.target.checked)"
      />
      Auto refresh
    </label>

    <label class="text-sm text-stone-600">
      Interval
      <select
        class="select ml-2"
        :disabled="!enabled"
        :value="intervalSeconds"
        @change="emit('update:intervalSeconds', Number($event.target.value))"
      >
        <option v-for="value in options" :key="value" :value="value">
          {{ labelFor(value) }}
        </option>
      </select>
    </label>
  </div>
</template>
