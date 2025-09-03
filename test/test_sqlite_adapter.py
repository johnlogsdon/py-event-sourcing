import pytest
from pytest_asyncio import fixture
import asyncio
from py_event_sourcing import CandidateEvent, sqlite_stream_factory, EventFilter, EqualsClause, InClause, LikeClause
import tempfile
import os

@fixture
async def open_stream():
    """
    Provides a factory with a clean, file-based database for each test function,
    ensuring complete isolation.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        async with sqlite_stream_factory(db_path) as factory:
            yield factory


@pytest.mark.asyncio
async def test_write_single_event(open_stream):
    stream_id = "test_stream"
    event = CandidateEvent(type="TestEvent", data=b"some data")

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
    event1 = CandidateEvent(type="Event1", data=b"data1")
    event2 = CandidateEvent(type="Event2", data=b"data2")

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
            [CandidateEvent(type="test", data=b"")], expected_version=0
        )
        assert s.version == 1

        await s.write(
            [CandidateEvent(type="test", data=b"")], expected_version=1
        )
        assert s.version == 2

        read_events = [e async for e in s.read()]
        assert read_events[0].version == 1
        assert read_events[1].version == 2

        with pytest.raises(ValueError):
            await s.write(
                [CandidateEvent(type="test", data=b"")],
                expected_version=0,
            )


@pytest.mark.asyncio
async def test_idempotency(open_stream):
    stream_id = "test_stream"
    event = CandidateEvent(
        type="idempotent-event",
        data=b"",
        idempotency_key="unique-event-1",
    )
    async with open_stream(stream_id) as s:
        await s.write([event, event])  # Write the same event twice
        events = [e async for e in s.read()]
        assert len(events) == 1


@pytest.mark.asyncio
async def test_concurrency_control(open_stream):
    stream_id = "test_concurrency"
    async with open_stream(stream_id) as s1:
        await s1.write([CandidateEvent(type="init", data=b"")])
        assert s1.version == 1

        async with open_stream(stream_id) as s2:
            assert s2.version == 1
            await s2.write(
                [CandidateEvent(type="write-by-s2", data=b"")],
                expected_version=1,
            )
            assert s2.version == 2

            # s1 tries to write with a stale version
            with pytest.raises(ValueError, match="Concurrency conflict"):
                await s1.write(
                    [
                        CandidateEvent(
                            type="write-by-s1", data=b""
                        )
                    ],
                    expected_version=1,
                )


@pytest.mark.asyncio
async def test_snapshots(open_stream):
    stream_id = "test_snapshots"
    async with open_stream(stream_id) as s:
        await s.write(
            [CandidateEvent(type="event1", data=b"")] * 10
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
            [CandidateEvent(type="live-event", data=b"live-data")]
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
            [CandidateEvent(type="replay-event", data=b"replay-data")]
        )
        assert s.version == 1

        # Now, start watching. It should first replay the existing event.
        watched_event = await anext(s.watch(from_version=0))
        assert watched_event.type == "replay-event"

        # Now, test a live event
        watch_task = asyncio.create_task(anext(s.watch(from_version=s.version)))
        await asyncio.sleep(0.01)
        await s.write(
            [CandidateEvent(type="live-event", data=b"live-data-2")]
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
                        CandidateEvent(
                            type="concurrent-event",
                            data=b"",
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


@pytest.mark.asyncio
async def test_end_to_end_filtering(open_stream):
    """
    Tests the complete filtering functionality, from the stream's `read` method
    down to the SQL query, on the @all stream.
    """
    # 1. Arrange: Write a diverse set of events
    # Customer A events
    async with open_stream("customer-1") as s:
        await s.write(
            CandidateEvent(type="order_placed", data=b"", metadata={"region": "EMEA", "value": 100})
        )
        await s.write(
            CandidateEvent(type="order_shipped", data=b"", metadata={"region": "EMEA", "value": 100})
        )
    # Customer B events
    async with open_stream("customer-2") as s:
        await s.write(
            CandidateEvent(type="order_placed", data=b"", metadata={"region": "APAC", "value": 250})
        )
    # Internal system events (should be filtered out)
    async with open_stream("system-alerts") as s:
        await s.write(
            CandidateEvent(type="heartbeat", data=b"", metadata={"region": "EMEA"})
        )

    # 2. Act: Read from the @all stream with a specific filter
    async with open_stream("@all") as s:
        # We want all 'order_placed' events from 'customer' streams in the 'EMEA' region.
        event_filter = EventFilter(clauses=[
            LikeClause(field="stream_id", value="customer-%"),
            EqualsClause(field="event_type", value="order_placed"),
            EqualsClause(field="metadata.region", value="EMEA"),
        ])
        
        filtered_events = [e async for e in s.read(event_filter=event_filter)]

    # 3. Assert: Check that only the correct event was returned
    assert len(filtered_events) == 1
    event = filtered_events[0]
    assert event.stream_id == "customer-1"
    assert event.type == "order_placed"
    assert event.metadata["region"] == "EMEA"
