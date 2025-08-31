import pytest
from pytest_asyncio import fixture
import asyncio
from datetime import datetime, timezone
from event_sourcing_v2 import stream_factory, Event
import pydantic_core

import tempfile
import os
import time

@fixture
def fresh_config():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        yield {"url": f"sqlite:///{db_path}"}

@fixture
async def open_stream(fresh_config):
    """Provides a clean open_stream function for each test, wrapped in a resource-managing context."""
    async with stream_factory(fresh_config) as open_stream_func:
        yield open_stream_func

# Test 1
@pytest.mark.asyncio
async def test_open_close(open_stream):
    async with open_stream("test") as stream:
        assert stream.stream_id == "test"

# Test 2
@pytest.mark.asyncio
async def test_append_basic(open_stream):
    async with open_stream("test") as stream:
        event = Event(type="Test", data=b"data", timestamp=datetime.now(), metadata={})
        new_version = await stream.write([event])
        assert new_version == 1

# Test 3
@pytest.mark.asyncio
async def test_append_idempotent(open_stream):
    async with open_stream("test") as stream:
        event = Event(id="unique", type="Test", data=b"data", timestamp=datetime.now())
        await stream.write([event])
        new_version = await stream.write([event])
        assert new_version == 1

# Test 4
@pytest.mark.asyncio
async def test_append_multiple(open_stream):
    async with open_stream("test") as stream:
        events = [
            Event(type="Test1", data=b"data1", timestamp=datetime.now(), metadata={}),
            Event(type="Test2", data=b"data2", timestamp=datetime.now(), metadata={}),
        ]
        new_version = await stream.write(events)
        assert new_version == 2

# Test 5
@pytest.mark.asyncio
async def test_append_empty(open_stream):
    async with open_stream("test") as stream:
        new_version = await stream.write([])
        assert new_version == 0

# Test 6
@pytest.mark.asyncio
async def test_append_invalid_event(open_stream):
    async with open_stream("test") as stream:
        with pytest.raises(TypeError):
            await stream.write([{"type": "InvalidEvent"}])

# Test 7
@pytest.mark.asyncio
async def test_append_invalid_data(open_stream):
    async with open_stream("test") as stream:
        with pytest.raises(pydantic_core.ValidationError):
            await stream.write([Event(type="Test", data=None, timestamp=datetime.now(), metadata={})])

# Test 8
@pytest.mark.asyncio
async def test_append_invalid_timestamp(open_stream):
    async with open_stream("test") as stream:
        with pytest.raises(pydantic_core.ValidationError):
            await stream.write([Event(type="Test", data=b"data", timestamp=None, metadata={})])

# Test 9
@pytest.mark.asyncio
async def test_append_with_no_metadata(open_stream):
    """Tests that an event can be written without any metadata."""
    async with open_stream("test") as stream:
        event = Event(type="Test", data=b"data", timestamp=datetime.now())
        new_version = await stream.write([event])
        assert new_version == 1

        read_events = [e async for e in stream.read()]
        assert len(read_events) == 1
        assert read_events[0].metadata is None

@pytest.mark.asyncio
async def test_unsupported_scheme():
    with tempfile.TemporaryDirectory() as tmpdir:
        config = {"url": f"file://{tmpdir}/test.txt"}
        with pytest.raises(ValueError, match="Unsupported scheme: file. Only 'sqlite' is supported."):
            async with stream_factory(config) as open_stream:
                # The error should be raised when the context is entered, not just on creation.
                async with open_stream("test"):
                    pass

@pytest.mark.asyncio
async def test_file_persistence(open_stream):
    # Session 1: write an event
    async with open_stream("test") as stream:
        event = Event(type="Test", data=b"data", timestamp=datetime.now(), metadata={})
        await stream.write([event])
    
    # Session 2: read and verify
    async with open_stream("test") as stream:
        events = [e async for e in stream.read()]
        assert len(events) == 1
        assert events[0].type == "Test"
        assert events[0].data == b"data"

@pytest.mark.asyncio
async def test_file_watch(open_stream):
    stream_name = "test_watch"
    async with open_stream(stream_name) as stream:
        # Write initial event to test replay
        initial_event = Event(type="Initial", data=b"initial", timestamp=datetime.now(), metadata={})
        await stream.write([initial_event])
        
        count = 0
        async def consume_watch():
            nonlocal count
            # The watch starts from version 0, replaying the initial event first.
            async with open_stream(stream_name) as watch_stream:
                async for _ in watch_stream.watch(from_version=0):
                    count += 1
                    if count == 2:
                        break
        
        try:
            watch_task = asyncio.create_task(consume_watch())
            await asyncio.sleep(0.3)  # Allow replay and first poll
            
            # Write second event concurrently to test live watch
            async def write_concurrently():
                async with open_stream(stream_name) as stream2:
                    second_event = Event(type="Second", data=b"second", timestamp=datetime.now(), metadata={})
                    await stream2.write([second_event])

            write_task = asyncio.create_task(write_concurrently())
            
            await asyncio.wait_for(watch_task, timeout=5.0)
            await write_task
        except asyncio.TimeoutError:
            pytest.fail("Watch loop timed out")
        
        assert count == 2

@pytest.mark.asyncio
async def test_watch_from_present_default(open_stream):
    """Tests that watch() by default only shows new events."""
    stream_id = "watch_default_test"
    # Write an initial event
    async with open_stream(stream_id) as stream:
        await stream.write([Event(type="v1", data=b"1", timestamp=datetime.now())])
        assert stream.version == 1

    events_seen = []
    
    async def consume_stream():
        # watch() with no args should start from the current version (1)
        async with open_stream(stream_id) as stream:
            async for event in stream.watch():
                events_seen.append(event)
                break  # Stop after one event

    watch_task = asyncio.create_task(consume_stream())
    await asyncio.sleep(0.5)  # Give watcher time to start
    
    async with open_stream(stream_id) as writer_stream:
        await writer_stream.write([Event(type="v2", data=b"2", timestamp=datetime.now())])
    
    await asyncio.wait_for(watch_task, timeout=5)

    assert len(events_seen) == 1
    assert events_seen[0].type == "v2"
    assert events_seen[0].version == 2

@pytest.mark.asyncio
async def test_optimistic_concurrency_control(open_stream):
    """Tests that write fails if the expected_version does not match."""
    stream_id = "concurrency_test"
    # Session 1: open a stream and get its version
    async with open_stream(stream_id) as stream1:
        version1 = await stream1.write([Event(type="A", data=b"1", timestamp=datetime.now(), metadata={})])
        assert version1 == 1

        # Session 2: open the same stream
        async with open_stream(stream_id) as stream2:
            assert stream2.version == 1
            # Session 1 writes again, advancing the version
            await stream1.write([Event(type="B", data=b"2", timestamp=datetime.now(), metadata={})])

            # Session 2 tries to write with a stale version, which should fail
            with pytest.raises(ValueError, match="Concurrency conflict"):
                await stream2.write([Event(type="C", data=b"3", timestamp=datetime.now(), metadata={})], expected_version=1)

@pytest.mark.asyncio
async def test_read_from_empty_stream(open_stream):
    """Tests that reading from an empty stream yields no events."""
    async with open_stream("empty_stream_test") as stream:
        events = [e async for e in stream.read()]
        assert len(events) == 0

@pytest.mark.asyncio
async def test_read_from_version(open_stream):
    """
    Tests reading events from a specific version number.
    The `from_version` parameter is exclusive, returning events *after* that version.
    """
    stream_id = "test_read_from"
    async with open_stream(stream_id) as stream:
        await stream.write([
            Event(type="v1", data=b"1", timestamp=datetime.now(), metadata={}),  # version 1
            Event(type="v2", data=b"2", timestamp=datetime.now(), metadata={}),  # version 2
            Event(type="v3", data=b"3", timestamp=datetime.now(), metadata={}),  # version 3
        ])

    async with open_stream(stream_id) as stream:
        # Read events with a version greater than 1. This should return events v2 and v3.
        events = [e async for e in stream.read(from_version=1)]
        assert len(events) == 2
        assert events[0].type == "v2"
        assert events[0].version == 2
        assert events[1].type == "v3"
        assert events[1].version == 3

@pytest.mark.asyncio
async def test_read_and_filter(open_stream):
    async with open_stream("test") as stream:
        await stream.write([
            Event(type="Test", data=b"data1", timestamp=datetime.now(), metadata={"foo": "bar"}),
            Event(type="Test", data=b"data2", timestamp=datetime.now(), metadata={"foo": "baz"}),
        ])
    
    async with open_stream("test") as stream:
        all_events = [e async for e in stream.read()]
        filtered_events = [e for e in all_events if e.metadata.get("foo") == "bar"]
        assert len(filtered_events) == 1
        assert filtered_events[0].data == b"data1"

@pytest.mark.asyncio
async def test_metrics(open_stream):
    async with open_stream("test") as stream:
        metrics = await stream.metrics()
        assert metrics["current_version"] == 0
        assert metrics["event_count"] == 0
        assert metrics["last_timestamp"] is None

        event1 = Event(type="Test1", data=b"data1", timestamp=datetime.now(timezone.utc), metadata={})
        await stream.write([event1])
        metrics = await stream.metrics()
        assert metrics["current_version"] == 1
        assert metrics["event_count"] == 1
        assert metrics["last_timestamp"] == event1.timestamp

        event2 = Event(type="Test2", data=b"data2", timestamp=datetime.now(timezone.utc), metadata={})
        await stream.write([event2])
        metrics = await stream.metrics()
        assert metrics["current_version"] == 2
        assert metrics["event_count"] == 2
        assert metrics["last_timestamp"] == event2.timestamp

@pytest.mark.asyncio
async def test_load_snapshot_when_none_exists(open_stream):
    """Tests that loading a snapshot returns None when no snapshot has been saved."""
    async with open_stream("no_snapshot_test") as stream:
        await stream.write([Event(type="A", data=b"1", timestamp=datetime.now(), metadata={})])
        snapshot = await stream.load_snapshot()
        assert snapshot is None

@pytest.mark.asyncio
async def test_snapshot_and_replay_with_watch(open_stream):
    stream_name = "test_snapshot_watch"
    async with open_stream(stream_name) as stream:
        # Write some events
        event1 = Event(type="Test1", data=b"data1", timestamp=datetime.now(), metadata={})
        event2 = Event(type="Test2", data=b"data2", timestamp=datetime.now(), metadata={})
        await stream.write([event1, event2])

        # Save a snapshot
        snapshot_state = b"snapshot_state_v2"
        await stream.snapshot(snapshot_state)

    # Reopen the stream and verify replay from snapshot
    async with open_stream(stream_name) as stream:
        # 1. Load snapshot
        latest_snapshot = await stream.load_snapshot()
        assert latest_snapshot is not None
        assert latest_snapshot.state == snapshot_state
        assert latest_snapshot.version == 2

        # 2. Watch for events since snapshot
        replayed_events = []
        async def consume_watch():
            # Watch from snapshot version
            async with open_stream(stream_name) as watch_stream:
                async for event in watch_stream.watch(from_version=latest_snapshot.version):
                    replayed_events.append(event)
                    if len(replayed_events) == 2:  # event3 (replay) + event4 (live)
                        break
        
        try:
            watch_task = asyncio.create_task(consume_watch())
            await asyncio.sleep(0.3)  # let it start polling

            # Write events after snapshot to test replay and live watch
            async with open_stream(stream_name) as stream2:
                event3 = Event(type="Test3", data=b"data3", timestamp=datetime.now(), metadata={})
                await stream2.write([event3])  # This will be replayed
                await asyncio.sleep(0.3)  # let it be processed
                event4 = Event(type="Test4", data=b"data4", timestamp=datetime.now(), metadata={})
                await stream2.write([event4])  # This will be live

            await asyncio.wait_for(watch_task, timeout=5.0)

        except asyncio.TimeoutError:
            pytest.fail("Watch loop timed out")

        # The replayed events should be event3 and event4
        assert len(replayed_events) == 2
        assert replayed_events[0].data == b"data3"
        assert replayed_events[0].version == 3
        assert replayed_events[1].data == b"data4"
        assert replayed_events[1].version == 4

@pytest.mark.asyncio
async def test_performance_read_write_1000_events(open_stream):
    """Measures the performance of writing and reading 1000 events."""
    num_events = 1000
    events_to_write = [
        Event(type="PerfTest", data=f"data{i}".encode(), timestamp=datetime.now())
        for i in range(num_events)
    ]

    async with open_stream("perf_stream") as stream:
        # --- Write performance test ---
        start_write = time.perf_counter()
        await stream.write(events_to_write)
        write_duration = time.perf_counter() - start_write
        print(f"\nWrite {num_events} events in {write_duration:.4f}s")
        # Assert that it's reasonably fast, e.g., under 1 second.
        assert write_duration < 1.0

        # --- Read performance test ---
        start_read = time.perf_counter()
        read_events = [e async for e in stream.read()]
        read_duration = time.perf_counter() - start_read
        print(f"Read {num_events} events in {read_duration:.4f}s")
        assert len(read_events) == num_events
        # Assert that it's reasonably fast, e.g., under 0.5 seconds.
        assert read_duration < 0.5