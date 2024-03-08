import { dirname, join } from 'path';
import { fileURLToPath } from 'url';


import { rm, cp } from 'node:fs/promises';
import Hypercore from 'hypercore';
import Hyperbee from 'hyperbee';


const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR_NAME = 'data'
const PATH_TO_DATA_DIR = join(__dirname, DATA_DIR_NAME);

const rmTestData = async (dataName) => {
  await rm(join(PATH_TO_DATA_DIR, dataName), {recursive: true, force: true})
}

async function basic() {
  const start = 0
  const stop = 25
  const DATA_DIR_NAME = 'basic';
  const storage_dir = join(PATH_TO_DATA_DIR, DATA_DIR_NAME);


  await rmTestData(DATA_DIR_NAME);
  const core = new Hypercore(storage_dir)
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
}

async function with_replaced_values() {
  const start = 0
  const stop = 25
  const DATA_DIR_NAME = 'with_replaced_values';
  const storage_dir = join(PATH_TO_DATA_DIR, DATA_DIR_NAME);


  await rmTestData(DATA_DIR_NAME);
  const core = new Hypercore(storage_dir)
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

}

async function more_height() {
  const start = 0
  const stop = 5**5;
  const DATA_DIR_NAME = 'more_height';
  const storage_dir = join(PATH_TO_DATA_DIR, DATA_DIR_NAME);


  await rmTestData(DATA_DIR_NAME);
  const core = new Hypercore(storage_dir)
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
}

async function alphabet() {
  const letters = 'abcdefghijklmnopqrstuvwxyz';
  const start = 0
  const stop = letters.length;
  const DATA_DIR_NAME = 'alphabet';
  const storage_dir = join(PATH_TO_DATA_DIR, DATA_DIR_NAME);


  await rmTestData(DATA_DIR_NAME);
  const core = new Hypercore(storage_dir)
  const db = new Hyperbee(core)
  await db.ready()

  for (let i = start; i < stop; i += 1) {
    const key = letters[i];
    const value = key;
    await db.put(key, value);
  }
}


async function run() {
  await basic();
  await with_replaced_values();
  await more_height();
  await alphabet();
}

run()
