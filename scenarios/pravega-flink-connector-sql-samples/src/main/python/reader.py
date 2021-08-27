import asyncio
from datetime import datetime

import pravega_client

manager = pravega_client.StreamManager('localhost:9090')
reader_group = manager.create_reader_group("rg1", "test", "test")
reader = reader_group.create_reader("r1")


async def read():
    slice = await reader.get_segment_slice_async()
    # for event in slice:
    #     print(event.data())
    data = [int.from_bytes(_.data(), 'big') for _ in slice]
    data.sort()
    print(len(data), data[-10:])
    # print(sum(data),(1+10000)*10000/2)

async def count():
    while True:
        slice = await reader.get_segment_slice_async()
        read_count = len(list(slice))
        print(datetime.now().strftime("%H:%M:%S"), ' ', read_count)


try:
    asyncio.run(read())
finally:
    reader.reader_offline()
