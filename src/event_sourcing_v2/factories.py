import asyncio
from contextlib import asynccontextmanager
import aiosqlite
from typing import AsyncContextManager, Dict
from .streams import Stream
from .adapters import sqlite_open_adapter
from .notifier import Notifier
import os
import urllib.parse

_notifiers: Dict[str, Notifier] = {}
_connections: Dict[str, aiosqlite.Connection] = {}
_notifier_lock = asyncio.Lock()
_connection_lock = asyncio.Lock()

async def cleanup_shared_resources():
    """
    Stops all active notifiers and closes all shared connections.
    This should be called before the application exits to ensure graceful shutdown.
    """
    async with _notifier_lock, _connection_lock:
        notifier_tasks = [notifier.stop() for notifier in _notifiers.values()]
        connection_tasks = [conn.close() for conn in _connections.values()]
        
        await asyncio.gather(*notifier_tasks, *connection_tasks)
        _notifiers.clear()
        _connections.clear()

@asynccontextmanager
async def open_stream(config: Dict, stream_id: str) -> AsyncContextManager["Stream"]:
    # If no URL is provided, default to an in-memory SQLite database.
    url = config.get('url')
    if not url:
        url = 'sqlite://'
        config['url'] = url

    scheme = url.split('://', 1)[0] if '://' in url else ''

    if scheme == 'sqlite':
        parsed = urllib.parse.urlparse(url)
        db_path = parsed.path
        if os.name == 'nt' and db_path.startswith('/') and not db_path.startswith('//'):
            db_path = db_path[1:]
        if not db_path or db_path == '/':
            db_path = ':memory:'

        async with _notifier_lock:
            if db_path not in _notifiers:
                _notifiers[db_path] = Notifier(db_path)
                await _notifiers[db_path].start()
        notifier = _notifiers[db_path]
        
        async with _connection_lock:
            if db_path not in _connections:
                _connections[db_path] = await aiosqlite.connect(db_path)
        connection = _connections[db_path]

        open_adapter = sqlite_open_adapter
    else:
        raise ValueError(f"Unsupported scheme: {scheme}. Only 'sqlite' is supported.")

    # Pass the shared connection to the adapter.
    # Pass the shared connection and db_path to the adapter.
    async with open_adapter(connection, db_path, stream_id) as handle:
        stream = Stream(config, stream_id, handle, notifier)
        yield stream
