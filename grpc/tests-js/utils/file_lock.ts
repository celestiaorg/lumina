import { server } from "@vitest/browser/context"

// we only have those 3 "syscalls" available, so it ain't much, but it's honest work
// didn't dive into custom commands tho https://vitest.dev/guide/browser/commands
const { readFile, writeFile, removeFile } = server.commands;

const sleep = async (ms: number) => {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const randInt = (lower: number, upper: number) => {
  return lower + (Math.round(Math.random() * upper) - lower);
}

// A cross process lock, operating on files
export class FileLock {
  path: string;
  nonce?: number;

  constructor(path: string) {
    this.path = path
  }

  async lock() {
    while (true) {
      const lockNonce = await this.readLock();

      if (!lockNonce) {
        // no one holds the lock, try to grab it
        await this.writeLock();
        // multiple locks can race here, thus sleep some so everybody finishes writing
        // we will see if we won in next iteration
        await sleep(75);
        continue;
      }

      if (lockNonce == this.nonce) {
        break;
      } else {
        // try again later
        await sleep(randInt(33, 100));
      }
    }
  }

  async unlock() {
    removeFile(this.path);
  }

  async readLock(): Promise<number | null> {
    try {
      const content = await readFile(this.path);
      const [timestamp, nonce] = content.split(" ");

      if (Date.now() - parseInt(timestamp) > 30 * 1000) {
        // ignore locks older than 30s, as a form of GC
        return null;
      }

      return parseInt(nonce);
    } catch (_) {
      return null;
    }
  }

  async writeLock() {
    this.nonce = randInt(0, 1e10);
    const content = Date.now().toString() + " " + this.nonce.toString();
    await writeFile(this.path, content);
  }
}
