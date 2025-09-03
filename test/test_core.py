import asyncio
import tempfile
import time
from pathlib import Path
from typing import Iterator

import pydantic_core
import pytest
from pytest_asyncio import fixture

from py_event_sourcing import sqlite_stream_factory, CandidateEvent


@fixture
def db_path() -> Iterator[str]:
    """Provides a path to a temporary, isolated database file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield str(Path(tmpdir) / "test.db")


@fixture
async def open_stream(db_path):
    async with sqlite_stream_factory(db_path) as open_stream_func:
        yield open_stream_func


@pytest.mark.asyncio
async def test_missing_db_path():
    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        async with sqlite_stream_factory():
            pass


# Test 1
@pytest.mark.asyncio
async def test_open_close(open_stream):
    async with open_stream("test") as stream:
        assert stream.stream_id == "test"


# Test 2
@pytest.mark.asyncio
async def test_append_basic(open_stream):
    async with open_stream("test") as stream:
        event = CandidateEvent(type="Test", data=b"data", metadata={})
        new_version = await stream.write([event])
        assert new_version == 1


# Test 3
@pytest.mark.asyncio
async def test_append_idempotent(open_stream):
    async with open_stream("test") as stream:
        event = CandidateEvent(idempotency_key="unique", type="Test", data=b"data")
        await stream.write([event])
        new_version = await stream.write([event])
        assert new_version == 1


# Test 4
@pytest.mark.asyncio
async def test_append_multiple(open_stream):
    async with open_stream("test") as stream:
        events = [
            CandidateEvent(type="Test1", data=b"data1", metadata={}),
            CandidateEvent(type="Test2", data=b"data2", metadata={}),
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
            await stream.write(
                [CandidateEvent(type="Test", data=None, metadata={})]
            )


# Test 8
@pytest.mark.asyncio
async def test_append_with_no_metadata(open_stream):
    """Tests that an event can be written without any metadata."""
    async with open_stream("test") as stream:
        event = CandidateEvent(type="Test", data=b"data")
        new_version = await stream.write([event])
        assert new_version == 1

        read_events = [e async for e in stream.read()]
        assert len(read_events) == 1
        assert read_events[0].metadata is None


@pytest.mark.asyncio
async def test_file_persistence(open_stream):
    # Session 1: write an event
    async with open_stream("test") as stream:
        event = CandidateEvent(type="Test", data=b"data", metadata={})
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
        initial_event = CandidateEvent(
            type="Initial", data=b"initial", metadata={}
        )
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
                    second_event = CandidateEvent(
                        type="Second", data=b"second", metadata={}
                    )
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
        await stream.write([CandidateEvent(type="v1", data=b"1")])
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
        await writer_stream.write(
            [CandidateEvent(type="v2", data=b"2")]
        )

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
        version1 = await stream1.write(
            [CandidateEvent(type="A", data=b"1", metadata={})]
        )
        assert version1 == 1

        # Session 2: open the same stream
        async with open_stream(stream_id) as stream2:
            assert stream2.version == 1
            # Session 1 writes again, advancing the version
            await stream1.write(
                [CandidateEvent(type="B", data=b"2", metadata={})]
            )

            # Session 2 tries to write with a stale version, which should fail
            with pytest.raises(ValueError, match="Concurrency conflict"):
                await stream2.write(
                    [CandidateEvent(type="C", data=b"3", metadata={})],
                    expected_version=1,
                )


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
        await stream.write(
            [
                CandidateEvent(type="v1", data=b"1", metadata={}),  # version 1
                CandidateEvent(type="v2", data=b"2", metadata={}),  # version 2
                CandidateEvent(type="v3", data=b"3", metadata={}),  # version 3
            ]
        )

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
        await stream.write(
            [
                CandidateEvent(
                    type="Test",
                    data=b"data1",
                    metadata={"foo": "bar"},
                ),
                CandidateEvent(
                    type="Test",
                    data=b"data2",
                    metadata={"foo": "baz"},
                ),
            ]
        )

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

        event1 = CandidateEvent(
            type="Test1",
            data=b"data1",
            metadata={},
        )
        await stream.write([event1])
        metrics = await stream.metrics()
        assert metrics["current_version"] == 1
        assert metrics["event_count"] == 1
        
        stored_events = [e async for e in stream.read()]
        assert metrics["last_timestamp"] == stored_events[0].timestamp

        event2 = CandidateEvent(
            type="Test2",
            data=b"data2",
            metadata={},
        )
        await stream.write([event2])
        metrics = await stream.metrics()
        assert metrics["current_version"] == 2
        assert metrics["event_count"] == 2
        
        stored_events = [e async for e in stream.read()]
        assert metrics["last_timestamp"] == stored_events[1].timestamp


@pytest.mark.asyncio
async def test_load_snapshot_when_none_exists(open_stream):
    """Tests that loading a snapshot returns None when no snapshot has been saved."""
    async with open_stream("no_snapshot_test") as stream:
        await stream.write(
            [CandidateEvent(type="A", data=b"1", metadata={})]
        )
        snapshot = await stream.load_snapshot()
        assert snapshot is None


@pytest.mark.asyncio
async def test_snapshot_and_replay_with_watch(open_stream):
    stream_name = "test_snapshot_watch"
    async with open_stream(stream_name) as stream:
        # Write some events
        event1 = CandidateEvent(
            type="Test1", data=b"data1", metadata={}
        )
        event2 = CandidateEvent(
            type="Test2", data=b"data2", metadata={}
        )
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
                async for event in watch_stream.watch(
                    from_version=latest_snapshot.version
                ):
                    replayed_events.append(event)
                    if len(replayed_events) == 2:  # event3 (replay) + event4 (live)
                        break

        try:
            watch_task = asyncio.create_task(consume_watch())
            await asyncio.sleep(0.3)  # let it start polling

            # Write events after snapshot to test replay and live watch
            async with open_stream(stream_name) as stream2:
                event3 = CandidateEvent(
                    type="Test3", data=b"data3", metadata={}
                )
                await stream2.write([event3])  # This will be replayed
                await asyncio.sleep(0.3)  # let it be processed
                event4 = CandidateEvent(
                    type="Test4", data=b"data4", metadata={}
                )
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
        CandidateEvent(type="PerfTest", data=f"data{i}".encode())
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


@pytest.mark.asyncio
async def test_high_concurrency_scenario(db_path: str):
    """
    Tests a high-concurrency scenario with multiple readers, writers,
    and a watcher operating on the same stream simultaneously.
    """
    stream_id = "high_concurrency_stream"
    num_writers = 5
    num_readers = 5
    events_per_writer = 20  # Increased load
    total_events = num_writers * events_per_writer

    async with sqlite_stream_factory(db_path) as open_stream:
        # Use a single stream instance for all write operations to correctly
        # serialize writes and manage the stream version.
        async with open_stream(stream_id) as writer_stream:
            writer_lock = asyncio.Lock()
            all_writes_done = asyncio.Event()

            async def writer_task():
                for _ in range(events_per_writer):
                    async with writer_lock:
                        event = CandidateEvent(
                            type="WriteEvent",
                            data=b"data",
                        )
                        await writer_stream.write([event])

            async def reader_task():
                # Perform reads until all writes are done, then one final read.
                while not all_writes_done.is_set():
                    async with open_stream(stream_id) as stream:
                        _ = [e async for e in stream.read()]
                    await asyncio.sleep(0)  # Yield control to the event loop
                # Final read to ensure we get the last events
                async with open_stream(stream_id) as stream:
                    return [e async for e in stream.read()]

            async def watcher_task():
                events_watched = []
                async with open_stream(stream_id) as stream:
                    async for event in stream.watch():
                        events_watched.append(event)
                        if len(events_watched) == total_events:
                            break
                return events_watched

            # Start all tasks
            writer_tasks = [
                asyncio.create_task(writer_task()) for _ in range(num_writers)
            ]
            reader_tasks = [
                asyncio.create_task(reader_task()) for _ in range(num_readers)
            ]
            watcher = asyncio.create_task(watcher_task())

            # Wait for writers to complete
            await asyncio.gather(*writer_tasks)
            all_writes_done.set()

            # Wait for readers and watcher to complete
            final_reads = await asyncio.gather(*reader_tasks)
            watched_events = await watcher

            # --- Validation ---
            # 1. Final version should be correct
            assert writer_stream.version == total_events

            # 2. All readers should have eventually read all events
            for i, events in enumerate(final_reads):
                assert len(events) == total_events, f"Reader {i} failed"

            # 3. Watcher should have received all events in order
            assert len(watched_events) == total_events
            for i in range(total_events):
                assert watched_events[i].version == i + 1


@pytest.mark.asyncio
async def test_write_to_all_stream_is_disallowed(open_stream):
    """Tests that attempting to write to the '@all' stream raises an error."""
    async with open_stream("@all") as stream:
        with pytest.raises(ValueError, match="Writing to the '@all' stream is not permitted."):
            await stream.write([CandidateEvent(type="test", data=b"")])


@pytest.mark.asyncio
async def test_read_from_all_stream(open_stream):
    """Tests that reading from the '@all' stream returns events from all streams in global order."""
    # Write to stream 1
    async with open_stream("stream1") as s1:
        await s1.write([CandidateEvent(type="s1-e1", data=b"1")])
        await s1.write([CandidateEvent(type="s1-e2", data=b"2")])

    # Write to stream 2
    async with open_stream("stream2") as s2:
        await s2.write([CandidateEvent(type="s2-e1", data=b"3")])

    # Read from @all
    async with open_stream("@all") as s_all:
        all_events = [e async for e in s_all.read(from_version=0)]
        assert len(all_events) == 3
        assert all_events[0].sequence_id == 1
        assert all_events[0].type == "s1-e1"
        assert all_events[1].sequence_id == 2
        assert all_events[1].type == "s1-e2"
        assert all_events[2].sequence_id == 3
        assert all_events[2].type == "s2-e1"

        # Read from a specific sequence ID
        events_after_seq1 = [e async for e in s_all.read(from_version=1)]
        assert len(events_after_seq1) == 2
        assert events_after_seq1[0].sequence_id == 2
        assert events_after_seq1[1].sequence_id == 3


@pytest.mark.asyncio
async def test_watch_all_stream(open_stream):
    """Tests that watching the '@all' stream receives events from all streams."""
    events_seen = []

    async def consume_stream():
        async with open_stream("@all") as stream:
            async for event in stream.watch(from_version=0):
                events_seen.append(event)
                if len(events_seen) == 3:
                    break

    # Start watcher
    watch_task = asyncio.create_task(consume_stream())
    await asyncio.sleep(0.5)

    # Write to stream 1 (should be replayed)
    async with open_stream("stream1") as s1:
        await s1.write([CandidateEvent(type="s1-e1", data=b"1")])

    # Write to stream 2 (should be a live event for the watcher)
    async with open_stream("stream2") as s2:
        await s2.write([CandidateEvent(type="s2-e1", data=b"2")])

    # Write to stream 1 again (live)
    async with open_stream("stream1") as s1:
        await s1.write([CandidateEvent(type="s1-e2", data=b"3")])

    await asyncio.wait_for(watch_task, timeout=5)

    assert len(events_seen) == 3
    assert events_seen[0].type == "s1-e1"
    assert events_seen[0].sequence_id == 1
    assert events_seen[1].type == "s2-e1"
    assert events_seen[1].sequence_id == 2
    assert events_seen[2].type == "s1-e2"
    assert events_seen[2].sequence_id == 3


@pytest.mark.asyncio
async def test_snapshot_all_stream(open_stream):
    """Tests that snapshotting the '@all' stream works correctly."""
    # Write some events to different streams
    async with open_stream("stream1") as s1:
        await s1.write([CandidateEvent(type="s1-e1", data=b"1")]) # seq 1
    async with open_stream("stream2") as s2:
        await s2.write([CandidateEvent(type="s2-e1", data=b"2")]) # seq 2
    async with open_stream("stream1") as s1:
        await s1.write([CandidateEvent(type="s1-e2", data=b"3")]) # seq 3

    # Build a projection from the @all stream
    all_events_state = []
    last_sequence_id = 0
    async with open_stream("@all") as s_all:
        async for event in s_all.read():
            all_events_state.append(event.type)
            last_sequence_id = event.sequence_id
        
        # Snapshot the projection state. The version is the last sequence_id.
        await s_all.snapshot(
            str(all_events_state).encode(), 
            version=last_sequence_id, 
            projection_name="all_events_proj"
        )

    # Write another event
    async with open_stream("stream2") as s2:
        await s2.write([CandidateEvent(type="s2-e2", data=b"4")]) # seq 4

    # Restore from snapshot and continue
    async with open_stream("@all") as s_all:
        snapshot = await s_all.load_snapshot(projection_name="all_events_proj")
        assert snapshot is not None
        assert snapshot.version == 3
        
        restored_state = eval(snapshot.state.decode())
        
        # Read from the snapshot's version (which is the sequence id)
        async for event in s_all.read(from_version=snapshot.version):
            restored_state.append(event.type)

        assert restored_state == ["s1-e1", "s2-e1", "s1-e2", "s2-e2"]