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

async function basic() {
  const start = 0
  const stop = 25
  const DATA_DIR_NAME = 'basic';
  const PATH_TO_DATA_DIR = join(PATH_TO_NODE_TEST_DATA, DATA_DIR_NAME);


  await rmTestData(DATA_DIR_NAME);
  const core = new Hypercore(PATH_TO_DATA_DIR)
  const db = new Hyperbee(core)
  await db.ready()

  for (let i = start; i < stop; i += 1) {
    const key = String(i);
    const value = String(stop - i)
    await db.put(key, value);
  }
  for (let i = start; i < stop; i += 1) {
    const key = String(i);
    const value = String(stop - i)
    const res = await db.get(key);
    if (res.value.toString() !== value) {
      console.error(`Could not get ${key} instead go ${res.value.toString()}`)
    }
  }
  await copyTestData(DATA_DIR_NAME);
}

async function with_replaced_values() {
  const start = 0
  const stop = 25
  const DATA_DIR_NAME = 'with_replaced_values';
  const PATH_TO_DATA_DIR = join(PATH_TO_NODE_TEST_DATA, DATA_DIR_NAME);


  await rmTestData(DATA_DIR_NAME);
  const core = new Hypercore(PATH_TO_DATA_DIR)
  const db = new Hyperbee(core)
  await db.ready()

  for (let i = start; i < stop; i += 1) {
    const x = String(i);
    await db.put(x, x);
  }

  for (let i = start; i < stop; i += 1) {
    const x = String(i);
    await db.put(x, String(i*2));
  }

  await copyTestData(DATA_DIR_NAME);
}

async function more_height() {
  const start = 0
  const stop = 5**5;
  const DATA_DIR_NAME = 'more_height';
  const PATH_TO_DATA_DIR = join(PATH_TO_NODE_TEST_DATA, DATA_DIR_NAME);


  await rmTestData(DATA_DIR_NAME);
  const core = new Hypercore(PATH_TO_DATA_DIR)
  const db = new Hyperbee(core)
  await db.ready()

  for (let i = start; i < stop; i += 1) {
    const key = String(i);
    const value = String(stop - i)
    await db.put(key, value);
  }
  for (let i = start; i < stop; i += 1) {
    const key = String(i);
    const value = String(stop - i)
    const res = await db.get(key);
    if (res.value.toString() !== value) {
      console.error(`Could not get ${key} instead go ${res.value.toString()}`)
    }
  }
  await copyTestData(DATA_DIR_NAME);
}

async function alphabet() {
  const letters = 'abcdefghijklmnopqrstuvwxyz';
  const start = 0
  const stop = letters.length;
  const DATA_DIR_NAME = 'alphabet';
  const PATH_TO_DATA_DIR = join(PATH_TO_NODE_TEST_DATA, DATA_DIR_NAME);


  await rmTestData(DATA_DIR_NAME);
  const core = new Hypercore(PATH_TO_DATA_DIR)
  const db = new Hyperbee(core)
  await db.ready()

  for (let i = start; i < stop; i += 1) {
    const key = letters[i];
    const value = key;
    await db.put(key, value);
  }
  await copyTestData(DATA_DIR_NAME);
}


async function run() {
  await basic();
  await with_replaced_values();
  await more_height();
  await alphabet();
}

run()
