import { dirname, join } from 'path';
import { fileURLToPath } from 'url';


import { rm, cp } from 'node:fs/promises';
import Hypercore from 'hypercore';
import Hyperbee from 'hyperbee';


const __dirname = dirname(fileURLToPath(import.meta.url));
const NODE_TEST_DATA_DIR_NAME = 'test_data'
const RS_TEST_DATA_DIR_NAME = 'test_data'
const PATH_TO_NODE_TEST_DATA = join(__dirname, NODE_TEST_DATA_DIR_NAME);
const PATH_TO_RS_TEST_DATA = join(__dirname, '..', RS_TEST_DATA_DIR_NAME);

const rmTestData = async (dataName) => {
  await rm(join(PATH_TO_RS_TEST_DATA, dataName), {recursive: true, force: true})
  await rm(join(PATH_TO_NODE_TEST_DATA, dataName), {recursive: true, force: true})
}

const copyTestData = async (dataName) => {
  await cp(join(PATH_TO_NODE_TEST_DATA, dataName), join(PATH_TO_RS_TEST_DATA, dataName), {recursive: true})

}

const N = 25
const DATA_DIR_NAME = 'basic';
const PATH_TO_DATA_DIR = join(PATH_TO_NODE_TEST_DATA, DATA_DIR_NAME);


(async ()=> {

  await rmTestData(DATA_DIR_NAME);
  const core = new Hypercore(PATH_TO_DATA_DIR)
  const db = new Hyperbee(core)
  await db.ready()

  for (let i = 0; i < N; i += 1) {
    const x = String(i);
    await db.put(x, x);
  }
  for (let i = 0; i < N; i += 1) {
    const x = String(i);
    const res = await db.get(x);
    if (res.value.toString() !== x) {
      console.error(`Could not get ${x} instead go ${res.value.toString()}`)
    }
  }
  await copyTestData(DATA_DIR_NAME);
})()
