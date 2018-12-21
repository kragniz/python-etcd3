import asyncio
import pytest


# @pytest.yield_fixture(scope='session')
# def event_loop(request):
#     """Create an instance of the default event loop for each test case."""
#     # loop = asyncio.get_event_loop_policy().new_event_loop()
#     # yield loop
#     # loop.close()
#     yield asyncio.get_event_loop()
