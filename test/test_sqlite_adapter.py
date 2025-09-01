import pytest
from pytest_asyncio import fixture
import asyncio
from datetime import datetime
from event_sourcing_v2.models import Event, Snapshot
from event_sourcing_v2.adaptors.sqlite import (
    SQLiteStorage,
    sqlite_open_adapter,
)


@fixture
async def storage_factory():
    """Provides a clean, in-memory SQLite storage factory for each test."""
    config = {"db_path": ":memory:"}
    async with SQLiteStorage(config) as storage:
        yield storage


@pytest.mark.asyncio
async def test_write_single_event(storage_factory):
    """Tests writing a single event to a new stream."""
    stream_id = "test_stream"
    event = Event(type="TestEvent", data=b"some data", timestamp=datetime.now())

    async with storage_factory.open_stream(stream_id) as stream:
        new_version = await stream.write([event], expected_version=0)
        assert new_version == 1

        # Verify the event was written correctly
        events = [e async for e in stream.read()]
        assert len(events) == 1
        assert events[0].type == "TestEvent"
        assert events[0].data == b"some data"
        assert events[0].version == 1


@pytest.mark.asyncio
async def test_read_events(storage_factory):
    """Tests reading events from a stream."""
    stream_id = "test_stream"
    event1 = Event(type="Event1", data=b"data1", timestamp=datetime.now())
    event2 = Event(type="Event2", data=b"data2", timestamp=datetime.now())

    async with storage_factory.open_stream(stream_id) as stream:
        await stream.write([event1, event2], expected_version=0)

        # Read all events
        events = [e async for e in stream.read()]
        assert len(events) == 2
        assert events[0].type == "Event1"
        assert events[1].type == "Event2"


@pytest.mark.asyncio
async def test_versioning(storage_factory):
    """Tests that version numbers are correctly assigned."""
    stream_id = "test_stream"
    event1 = Event(type="Event1", data=b"data1", timestamp=datetime.now())
    event2 = Event(type="Event2", data=b"data2", timestamp=datetime.now())

    async with storage_factory.open_stream(stream_id) as stream:
        version = await stream.write([event1], expected_version=0)
        assert version == 1

        version = await stream.write([event2], expected_version=1)
        assert version == 2

        events = [e async for e in stream.read()]
        assert events[0].version == 1
        assert events[1].version == 2


@pytest.mark.asyncio
async def test_idempotency(storage_factory):
    """Tests that writing the same event with an idempotency key is handled correctly."""
    stream_id = "test_stream"
    event = Event(id="unique_id", type="TestEvent", data=b"data", timestamp=datetime.now())

    async with storage_factory.open_stream(stream_id) as stream:
        # First write should succeed
        version = await stream.write([event], expected_version=0)
        assert version == 1

        # Second write should be ignored
        version = await stream.write([event], expected_version=1)
        assert version == 1

        events = [e async for e in stream.read()]
        assert len(events) == 1


@pytest.mark.asyncio
async def test_concurrency_control(storage_factory):
    """Tests that a write fails if the expected version is incorrect."""
    stream_id = "test_stream"
    event = Event(type="TestEvent", data=b"data", timestamp=datetime.now())

    async with storage_factory.open_stream(stream_id) as stream:
        await stream.write([event], expected_version=0)

        # This should fail because the stream is at version 1, but we expect 0
        with pytest.raises(ValueError, match="Concurrency conflict"):
            await stream.write([event], expected_version=0)


@pytest.mark.asyncio
async def test_snapshots(storage_factory):
    """Tests saving and loading snapshots."""
    stream_id = "test_stream"
    snapshot_data = b"my snapshot data"

    async with storage_factory.open_stream(stream_id) as stream:
        # Write an event to get the stream to version 1
        await stream.write(
            [Event(type="TestEvent", data=b"data", timestamp=datetime.now())],
            expected_version=0,
        )

        # Save a snapshot
        await stream.snapshot(snapshot_data)

        # Load the snapshot
        loaded_snapshot = await stream.load_snapshot()
        assert loaded_snapshot is not None
        assert loaded_snapshot.version == 1
        assert loaded_snapshot.state == snapshot_data


@pytest.mark.asyncio
async def test_notifier_live_event(storage_factory):
    """Tests that the notifier sends a live event to a subscriber."""
    stream_id = "notifier_stream"
    
    async with storage_factory.open_stream(stream_id) as stream:
        queue = await stream.notifier.subscribe(stream_id)
        
        # Write an event after subscribing
        event = Event(type="LiveEvent", data=b"live", timestamp=datetime.now())
        await stream.write([event], expected_version=0)
        
        # Check if the event is received
        try:
            received_event = await asyncio.wait_for(queue.get(), timeout=1.0)
            assert received_event.type == "LiveEvent"
            assert received_event.data == b"live"
        except asyncio.TimeoutError:
            pytest.fail("Notifier did not receive live event in time.")
        finally:
            await stream.notifier.unsubscribe(stream_id, queue)


@pytest.mark.asyncio
async def test_notifier_replay(storage_factory):
    """Tests that the notifier replays historical events to a new subscriber."""
    stream_id = "notifier_replay_stream"

    async with storage_factory.open_stream(stream_id) as stream:
        # Write an event *before* subscribing
        event = Event(type="HistoricalEvent", data=b"historical", timestamp=datetime.now())
        await stream.write([event], expected_version=0)

    # Give the notifier a moment to poll and process the event
    await asyncio.sleep(0.3)

    # Now, subscribe and check for the replayed event
    async with storage_factory.open_stream(stream_id) as stream:
        queue = await stream.notifier.subscribe(stream_id)
        try:
            live_event = Event(type="LiveEvent2", data=b"live2", timestamp=datetime.now())
            await stream.write([live_event], expected_version=1)
            
            received_event = await asyncio.wait_for(queue.get(), timeout=1.0)
            assert received_event.type == "LiveEvent2"

        except asyncio.TimeoutError:
            pytest.fail("Notifier did not receive live event after replay.")
        finally:
            await stream.notifier.unsubscribe(stream_id, queue)


@pytest.mark.asyncio
async def test_concurrent_writes_different_streams(storage_factory):
    """Tests that writes to different streams can happen concurrently without interference."""
    stream_id_1 = "stream_1"
    stream_id_2 = "stream_2"

    async def write_to_stream(stream_id, event_type):
        async with storage_factory.open_stream(stream_id) as stream:
            event = Event(type=event_type, data=b"data", timestamp=datetime.now())
            await stream.write([event], expected_version=0)

    # Run two writes concurrently
    task1 = asyncio.create_task(write_to_stream(stream_id_1, "Event1"))
    task2 = asyncio.create_task(write_to_stream(stream_id_2, "Event2"))
    await asyncio.gather(task1, task2)

    # Verify both streams were written correctly
    async with storage_factory.open_stream(stream_id_1) as stream1:
        events1 = [e async for e in stream1.read()]
        assert len(events1) == 1
        assert events1[0].type == "Event1"

    async with storage_factory.open_stream(stream_id_2) as stream2:
        events2 = [e async for e in stream2.read()]
        assert len(events2) == 1
        assert events2[0].type == "Event2"
