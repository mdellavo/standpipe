import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    from standpipe import server
    server.main()
