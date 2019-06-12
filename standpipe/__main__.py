import logging
import asyncio

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    from . import server
    asyncio.run(server.main())
