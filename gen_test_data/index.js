import { dirname, join } from 'path';
import { fileURLToPath } from 'url';


import { rm, cp } from 'node:fs/promises';
import Hypercore from 'hypercore';
import Hyperbee from 'hyperbee';

const DATA_DIR_NAME = 'basic';

const __dirname = dirname(fileURLToPath(import.meta.url));
const RS_TEST_DATA_DIR_NAME = 'test_data'
const PATH_TO_NODE_TEST_DATA = __dirname;
const PATH_TO_RS_TEST_DATA = join(__dirname, '..', RS_TEST_DATA_DIR_NAME);

const rmTestData = async (dataName) => {
  await rm(join(PATH_TO_RS_TEST_DATA, dataName), {recursive: true, force: true})
  await rm(join(PATH_TO_NODE_TEST_DATA, dataName), {recursive: true, force: true})
}

const copyTestData = async (dataName) => {
  await cp(join(PATH_TO_NODE_TEST_DATA, dataName), join(PATH_TO_RS_TEST_DATA, dataName), {recursive: true})

}

(async ()=> {

  await rmTestData(DATA_DIR_NAME);
  const core = new Hypercore(DATA_DIR_NAME)
  const db = new Hyperbee(core)
  await db.ready()

  await db.put('hello', 'world');
  await db.put('foo', 'bar');

  for (let i = 0; i < 10; i += 1) {
    const x = String(i);
    await db.put(x, x);
  }
  await copyTestData(DATA_DIR_NAME);
})()
