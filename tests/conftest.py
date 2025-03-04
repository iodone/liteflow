import logging


def pytest_configure(config):
    # Configure logging with thread information
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(threadName)s] %(levelname)s %(filename)s:%(lineno)d: %(message)s",
        force=True,
    )
