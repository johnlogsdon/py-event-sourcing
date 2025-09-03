# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.3] - 2025-09-03

### Fixed
- Fixed broken relative links to all documentation files in README for PyPI compatibility
- Updated all relative links (basic_usage.py, benchmark.py, CONTRIBUTING.md, LICENSE, CHANGELOG.md) to absolute GitHub URLs
- All documentation links now work properly on both GitHub and PyPI platforms

## [1.0.1] - 2025-09-03

### Fixed
- Corrected import statement in README Quick Start example from `pysource` to `py_event_sourcing`

## [1.0.0] - 2025-09-03

### Added
- Initial release of py-event-sourcing
- Core event sourcing functionality with SQLite backend
- Async-first API using asyncio
- Event streaming with watchers
- Snapshot support for performance optimization
- Event filtering capabilities
- Comprehensive test suite
- Documentation and examples

### Features
- **Event Streams**: Create and manage event streams with SQLite persistence
- **Idempotent Writes**: Prevent duplicate events using idempotency keys
- **Optimistic Concurrency**: Ensure data consistency with version-based writes
- **Event Watching**: Real-time event watching with efficient polling
- **Snapshots**: Performance optimization through state snapshots
- **Global Event Stream**: Query across all streams using the special '@all' stream
- **Filtering**: Filter events by stream ID, event type, and custom criteria

### Technical Details
- Built with Python 3.11+
- Uses SQLite for reliable, file-based persistence
- Zero external dependencies except pydantic and aiosqlite
- Fully async API for high-performance applications
- Extensible adapter pattern for different storage backends

## [Unreleased]

### Planned
- Additional storage adapters (PostgreSQL, Redis)
- Enhanced filtering options
- Performance optimizations
- Additional documentation
