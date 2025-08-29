import pytest
from pytest_asyncio import fixture
import asyncio
from datetime import datetime
from cryptography.fernet import Fernet, InvalidToken
from event_sourcing_v2 import open_stream, Event, factories
import pydantic_core

import tempfile
import os

@fixture
async def fresh_db(request):
    """
    Provides test isolation by cleaning up shared resources (connections, notifiers)
    after each test function completes.
    """
    yield
    await factories.cleanup_shared_resources()

# Fixture
@fixture
def fresh_config():
    key = Fernet.generate_key()
    # Use in-memory SQLite for tests, which is the new default
    return {"key": key, "url": "sqlite://"}

# Test 1
@pytest.mark.asyncio
async def test_open_close(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        assert stream.stream_id == "test"

# Test 2
@pytest.mark.asyncio
async def test_append_basic(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        event = Event(type="Test", data=b"data", timestamp=datetime.now(), metadata={})
        new_version = await stream.append([event])
        assert new_version == 1

# Test 3
@pytest.mark.asyncio
async def test_append_idempotent(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        event = Event(type="Test", data=b"data", timestamp=datetime.now(), metadata={'id': "unique"})
        await stream.append([event])
        new_version = await stream.append([event])
        assert new_version == 1

# Test 4
@pytest.mark.asyncio
async def test_append_multiple(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        events = [
            Event(type="Test1", data=b"data1", timestamp=datetime.now(), metadata={}),
            Event(type="Test2", data=b"data2", timestamp=datetime.now(), metadata={}),
        ]
        new_version = await stream.append(events)
        assert new_version == 2

# Test 5
@pytest.mark.asyncio
async def test_append_empty(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        new_version = await stream.append([])
        assert new_version == 0

# Test 6
@pytest.mark.asyncio
async def test_append_invalid_event(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        with pytest.raises(TypeError):
            await stream.append([{"type": "InvalidEvent"}])

# Test 7
@pytest.mark.asyncio
async def test_append_invalid_data(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        with pytest.raises(pydantic_core.ValidationError):
            await stream.append([Event(type="Test", data=None, timestamp=datetime.now(), metadata={})])

# Test 8
@pytest.mark.asyncio
async def test_append_invalid_timestamp(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        with pytest.raises(pydantic_core.ValidationError):
            await stream.append([Event(type="Test", data=b"data", timestamp=None, metadata={})])

# Test 9
@pytest.mark.asyncio
async def test_append_invalid_metadata(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        with pytest.raises(pydantic_core.ValidationError):
            await stream.append([Event(type="Test", data=b"data", timestamp=datetime.now(), metadata=None)])

# Test 14
@pytest.mark.asyncio
async def test_append_invalid_event_type_in_list(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        with pytest.raises(TypeError):
            await stream.append([{"type": "InvalidType", "data": b"data", "timestamp": datetime.now(), "metadata": {}}])

# Test 15
@pytest.mark.asyncio
async def test_append_invalid_data_in_list(fresh_config, fresh_db):
    async with open_stream(fresh_config, "test") as stream:
        with pytest.raises(TypeError):
            await stream.append([{"type": "Test", "data": None, "timestamp": datetime.now(), "metadata": {}}])

@pytest.mark.asyncio
async def test_unsupported_scheme(fresh_config, fresh_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        config = fresh_config.copy()
        config['url'] = f"file://{tmpdir}/test.txt"  # Invalid scheme
        with pytest.raises(ValueError, match="Unsupported scheme: file. Only 'sqlite' is supported."):
            async with open_stream(config, "test"):
                pass

@pytest.mark.asyncio
async def test_file_persistence(fresh_config, fresh_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        config = fresh_config.copy()
        config['url'] = f"sqlite:///{db_path}"
        # Session 1: write an event
        async with open_stream(config, "test") as stream:
            event = Event(type="Test", data=b"data", timestamp=datetime.now(), metadata={})
            await stream.append([event])
        
        # Session 2: read and verify
        async with open_stream(config, "test") as stream:
            events = await stream.search(lambda e: True)
            assert len(events) == 1
            assert events[0].type == "Test"
            assert events[0].data == b"data"

@pytest.mark.asyncio
async def test_file_watch(fresh_config, fresh_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        config = fresh_config.copy()
        config['url'] = f"sqlite:///{db_path}"
        async with open_stream(config, "test") as stream:
            # Append initial event to test replay
            initial_event = Event(type="Initial", data=b"initial", timestamp=datetime.now(), metadata={})
            await stream.append([initial_event])
            
            async def handler(state, event):
                return state + 1
            
            count = 0
            async def consume_watch():
                nonlocal count
                async for new_count in stream.read_and_watch(handler, initial_state=0):
                    count = new_count
                    if count == 2:
                        break
            
            try:
                watch_task = asyncio.create_task(consume_watch())
                await asyncio.sleep(0.3)  # Allow replay and first poll
                
                # Append second event concurrently to test live watch
                second_event = Event(type="Second", data=b"second", timestamp=datetime.now(), metadata={})
                append_task = asyncio.create_task(stream.append([second_event]))
                
                await asyncio.wait_for(watch_task, timeout=5.0)
                await append_task  # Ensure append completes
            except asyncio.TimeoutError:
                pytest.fail("Watch loop timed out")
            
            assert count == 2

@pytest.mark.asyncio
async def test_default_stream_is_in_memory_sqlite(fresh_config, fresh_db):
    config = fresh_config.copy()
    del config['url']  # No url, should default to in-memory sqlite
    async with open_stream(config, "test") as stream:
        assert config['url'] == 'sqlite://'
        event = Event(type="Test", data=b"data", timestamp=datetime.now(), metadata={})
        version = await stream.append([event])
        assert version == 1
    # also check that it's not persisted by re-opening
    # We must explicitly clean up to get a new in-memory database, as connections are cached.
    await factories.cleanup_shared_resources()
    async with open_stream(config, "test") as stream:
        assert stream.handle.version == 0

@pytest.mark.asyncio
async def test_optimistic_concurrency_control(fresh_config, fresh_db):
    """Tests that append fails if the expected_version does not match."""
    # Session 1: open a stream and get its version
    async with open_stream(fresh_config, "concurrency_test") as stream1:
        version1 = await stream1.append([Event(type="A", data=b"1", timestamp=datetime.now(), metadata={})])
        assert version1 == 1

        # Session 2: open the same stream
        async with open_stream(fresh_config, "concurrency_test") as stream2:
            assert stream2.version == 1
            # Session 1 appends again, advancing the version
            await stream1.append([Event(type="B", data=b"2", timestamp=datetime.now(), metadata={})])

            # Session 2 tries to append with a stale version, which should fail
            with pytest.raises(ValueError, match="Concurrency conflict"):
                await stream2.append([Event(type="C", data=b"3", timestamp=datetime.now(), metadata={})], expected_version=1)
