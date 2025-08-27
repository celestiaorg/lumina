import { assert, test } from 'vitest';
import { ExtendedHeaderGenerator } from "../pkg/celestia_types.js";

test('consecutive headers previous header hash equals hash', () => {
  const gen = new ExtendedHeaderGenerator(BigInt(5));

  let header5 = gen.next();
  let header6 = gen.next();

  assert.equal(header5.height(), BigInt(5));
  assert.equal(header6.height(), BigInt(6));

  assert.equal(header5.hash(), header6.previousHeaderHash());
})
