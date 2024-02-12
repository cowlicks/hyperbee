import Hypercore from 'hypercore';
import Hyperbee from 'hyperbee';

const p = (x) => [console.log(x), x][1]

async function main() {
  const core = new Hypercore('../../written_by_rs') // store data in ./directory
  const hb = new Hyperbee(core);
  const x = await hb.get("hello");
  p(x.value.toString())
}

(async ()=> {
  await main();
})()
