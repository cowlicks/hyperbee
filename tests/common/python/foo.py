import asyncio
from target.hyperbee import *
from target.hyperbee import uniffi_set_event_loop


async def main():
    import json
    init_env_logs()
    try_current_rt()
    print(asyncio.get_running_loop())
    uniffi_set_event_loop(asyncio.get_running_loop())
    #hb = await hyperbee_from_storage_dir('./hbdir')
    hb = await hyperbee_from_ram();
    try_current_rt()
    res = await hb.get(b'hello')
    assert(res is None)
        
    try_current_rt()
    hb = await hyperbee_from_storage_dir('./hbdir')
    hb = await hyperbee_from_ram();
    res = await hb.get(b'hello')

    res = await hb.put(b'skipval', None)
    res = await hb.get(b'skipval')
    assert(res.value is None)


if __name__ == '__main__':
    asyncio.run(main())
