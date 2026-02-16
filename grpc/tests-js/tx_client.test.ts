import { assert, afterEach, beforeEach, test } from "vitest";
import { server } from "@vitest/browser/context"
import { secp256k1 } from "@noble/curves/secp256k1";

import { AppVersion, Blob, GrpcClient, Namespace, SignDoc, protoEncodeSignDoc } from "../pkg/celestia_grpc";
import { FileLock } from "./utils/file_lock";

// tests in the file are ran sequentially, but all browsers run in parallel, so
// we need to mutex the grpc client
const fLock = new FileLock("./node_modules/celestia-grpc-vitest-client.lock");
beforeEach(async () => fLock.lock())
afterEach(async () => fLock.unlock())

function arrayFromHex(hex: string): Uint8Array {
  // Uint8Array.fromHex should be on all latest browsers, but typecheck uses node
  // match all 2 subsequent chars and parse them as numbers
  return Uint8Array.from(hex.match(/../g)!.map((byte) => parseInt(byte, 16)));
}

test("submit blob", async () => {
  const privKey = await server.commands.readFile("./ci/credentials/node-1.plaintext-key").then(arrayFromHex);
  const pubKey = secp256k1.getPublicKey(privKey);

  const signer = (signDoc: SignDoc): Uint8Array => {
    const bytes = protoEncodeSignDoc(signDoc);
    return secp256k1.sign(bytes, privKey, { prehash: true });
  };

  const client = GrpcClient.withUrl("http://127.0.0.1:18080").withPubkeyAndSigner(pubKey, signer).build();

  const ns = Namespace.newV0(new Uint8Array([97, 98, 99]));
  const data = new Uint8Array([100, 97, 116, 97]);
  const blob = new Blob(ns, data, AppVersion.latest());

  const txInfo = await client.submitBlobs([blob]);

  assert(txInfo.height > 0);
})
