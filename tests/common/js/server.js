import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import Hypercore from 'hypercore';
import Hyperbee from 'hyperbee';
import RAM from 'random-access-memory';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR_NAME = 'data'
const PATH_TO_DATA_DIR = join(__dirname, DATA_DIR_NAME);

async function basic_in_ram() {
  const start = 0
  const stop = 25

  const storage_dir = join(PATH_TO_DATA_DIR, DATA_DIR_NAME);
  const core = new Hypercore(storage_dir)
  const db = new Hyperbee(core)
  await db.ready()
  return db
}
async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
const p = (...x) => [console.log(...x), x][1];

const b64Encode = (buf) => Buffer.from(buf).toString('base64');

(async () => {
  const db = await basic_in_ram();

  const {id, key, discoveryKey, writable } = db;
  p(`Discovery Key:
${b64Encode(discoveryKey)}

id:
${id}

key:
${b64Encode(key)}

writable:
${writable}
`);
  p(`waiting...`);
  while (true) {
    await sleep(1e3*10);
  }
  console.log('done')
})()
