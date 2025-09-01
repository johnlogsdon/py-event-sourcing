import pytest
from pytest_asyncio import fixture
import asyncio
from datetime import datetime
from event_sourcing_v2.models import Event, Snapshot
from event_sourcing_v2.adaptors.sqlite import (
    sqlite_stream_factory,
    sqlite_open_adapter,
)


@fixture
async def storage_factory():
    """
    Provides a SQLiteStorage instance with a clean in-memory database for each test function.
    """
    config = {"db_path": ":memory:"}
    async with sqlite_stream_factory(config) as factory:
        yield factory


@fixture
async def open_stream(storage_factory):
    yield storage_factory


@pytest.mark.asyncio
async def test_write_single_event(open_stream):
    stream_id = "test_stream"
    event = Event(type="TestEvent", data=b"some data", timestamp=datetime.now())

    async with open_stream(stream_id) as s:
        new_version = await s.write([event], expected_version=0)
        assert new_version == 1

        # Verify the event was written correctly
        read_events = [e async for e in s.read()]
        assert len(read_events) == 1
        assert read_events[0].type == "TestEvent"
        assert read_events[0].data == b"some data"
        assert read_events[0].version == 1


@pytest.mark.asyncio
async def test_read_events(open_stream):
    stream_id = "test_stream"
    event1 = Event(type="Event1", data=b"data1", timestamp=datetime.now())
    event2 = Event(type="Event2", data=b"data2", timestamp=datetime.now())

    async with open_stream(stream_id) as s:
        await s.write([event1, event2], expected_version=0)

        # Read all events
        read_events = [e async for e in s.read()]
        assert len(read_events) == 2
        assert read_events[0].type == "Event1"
        assert read_events[1].type == "Event2"


@pytest.mark.asyncio
async def test_versioning(open_stream):
    stream_id = "test_stream"
    async with open_stream(stream_id) as s:
        assert s.version == 0
        await s.write(
            [Event(type="test", data=b"", timestamp=datetime.now())], expected_version=0
        )
        assert s.version == 1

        await s.write(
            [Event(type="test", data=b"", timestamp=datetime.now())], expected_version=1
        )
        assert s.version == 2

        read_events = [e async for e in s.read()]
        assert read_events[0].version == 1
        assert read_events[1].version == 2

        with pytest.raises(ValueError):
            await s.write(
                [Event(type="test", data=b"", timestamp=datetime.now())],
                expected_version=0,
            )


@pytest.mark.asyncio
async def test_idempotency(open_stream):
    stream_id = "test_stream"
    event = Event(
        type="idempotent-event",
        data=b"",
        id="unique-event-1",
        timestamp=datetime.now(),
    )
    async with open_stream(stream_id) as s:
        await s.write([event, event])  # Write the same event twice
        events = [e async for e in s.read()]
        assert len(events) == 1


@pytest.mark.asyncio
async def test_concurrency_control(open_stream):
    stream_id = "test_concurrency"
    async with open_stream(stream_id) as s1:
        await s1.write([Event(type="init", data=b"", timestamp=datetime.now())])
        assert s1.version == 1

        async with open_stream(stream_id) as s2:
            assert s2.version == 1
            await s2.write(
                [Event(type="write-by-s2", data=b"", timestamp=datetime.now())],
                expected_version=1,
            )
            assert s2.version == 2

            # s1 tries to write with a stale version
            with pytest.raises(ValueError, match="Concurrency conflict"):
                await s1.write(
                    [
                        Event(
                            type="write-by-s1", data=b"", timestamp=datetime.now()
                        )
                    ],
                    expected_version=1,
                )


@pytest.mark.asyncio
async def test_snapshots(open_stream):
    stream_id = "test_snapshots"
    async with open_stream(stream_id) as s:
        await s.write(
            [Event(type="event1", data=b"", timestamp=datetime.now())] * 10
        )
        assert s.version == 10
        snapshot_data = b"snapshot_state"
        await s.snapshot(snapshot_data)
        snapshot_loaded = await s.load_snapshot()
        assert snapshot_loaded is not None
        assert snapshot_loaded.version == 10
        assert snapshot_loaded.state == b"snapshot_state"


@pytest.mark.asyncio
async def test_notifier_live_event(open_stream):
    stream_id = "test_notifier_live"
    async with open_stream(stream_id) as s:
        # Start watching *before* the event is written
        watch_task = asyncio.create_task(anext(s.watch()))

        # Give the watcher a moment to subscribe
        await asyncio.sleep(0.01)

        await s.write(
            [Event(type="live-event", data=b"live-data", timestamp=datetime.now())]
        )

        # The watcher should receive the event
        watched_event = await asyncio.wait_for(watch_task, timeout=1)

        assert watched_event.type == "live-event"
        assert watched_event.data == b"live-data"


@pytest.mark.asyncio
async def test_notifier_replay(open_stream):
    stream_id = "test_notifier_replay"
    async with open_stream(stream_id) as s:
        # Write an event *before* watching
        await s.write(
            [Event(type="replay-event", data=b"replay-data", timestamp=datetime.now())]
        )
        assert s.version == 1

        # Now, start watching. It should first replay the existing event.
        watched_event = await anext(s.watch(from_version=0))
        assert watched_event.type == "replay-event"

        # Now, test a live event
        watch_task = asyncio.create_task(anext(s.watch(from_version=s.version)))
        await asyncio.sleep(0.01)
        await s.write(
            [Event(type="live-event", data=b"live-data-2", timestamp=datetime.now())]
        )
        live_event = await asyncio.wait_for(watch_task, timeout=1)

        assert live_event.type == "live-event"
        assert live_event.data == b"live-data-2"


@pytest.mark.asyncio
async def test_concurrent_writes_different_streams(open_stream):
    stream_id_1 = "stream_1"
    stream_id_2 = "stream_2"

    async def writer(sid, num_events):
        async with open_stream(sid) as s:
            for _ in range(num_events):
                await s.write(
                    [
                        Event(
                            type="concurrent-event",
                            data=b"",
                            timestamp=datetime.now(),
                        )
                    ]
                )

    num_events_per_stream = 20
    await asyncio.gather(
        writer(stream_id_1, num_events_per_stream),
        writer(stream_id_2, num_events_per_stream),
    )

    async with open_stream(stream_id_1) as s1:
        assert s1.version == num_events_per_stream
    async with open_stream(stream_id_2) as s2:
        assert s2.version == num_events_per_stream
