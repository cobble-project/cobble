import assert from 'node:assert/strict'
import { test } from 'node:test'
import { displayTableValue } from './utils.js'

test('table values distinguish binary from structs by schema', () => {
  assert.equal(displayTableValue({ base64: 'AAEA' }, { kind: 'binary' }), 'base64: AAEA')
  for (const value of [{ base64: 'human-field', other: 17 }, { base64: 'human-field' }]) {
    assert.equal(displayTableValue(value, { kind: 'struct' }), JSON.stringify(value))
  }
  assert.equal(displayTableValue(null, { kind: 'binary' }), 'null')
  assert.equal(displayTableValue([{ base64: 'nested' }], { kind: 'list' }), '[{"base64":"nested"}]')
})
