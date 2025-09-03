# Contributing to py-event-sourcing

Thank you for your interest in contributing to py-event-sourcing! This document provides guidelines and information for contributors.

## Development Setup

### Prerequisites
- Python 3.11 or later
- [uv](https://github.com/astral-sh/uv) package manager

### Getting Started

1. **Clone the repository:**
   ```bash
   git clone https://github.com/johnlogsdon/py-event-sourcing.git
   cd py-event-sourcing
   ```

2. **Set up the development environment:**
   ```bash
   uv sync --dev
   ```

3. **Install pre-commit hooks (recommended):**
   ```bash
   uv run pre-commit install
   ```

## Development Workflow

### Code Style
This project uses:
- **Black** for code formatting
- **isort** for import sorting
- **flake8** for linting
- **mypy** for type checking

### Running Tests
```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=py_event_sourcing

# Run specific test file
uv run pytest test/test_core.py
```

### Code Quality Checks
```bash
# Format code
uv run black src/ test/

# Sort imports
uv run isort src/ test/

# Lint code
uv run flake8 src/ test/

# Type check
uv run mypy src/py_event_sourcing/
```

### Making Changes

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes following the coding standards**

3. **Add tests for new functionality**

4. **Run the full test suite and quality checks:**
   ```bash
   uv run pytest --cov=py_event_sourcing
   uv run black --check src/ test/
   uv run isort --check-only src/ test/
   uv run flake8 src/ test/
   uv run mypy src/py_event_sourcing/
   ```

5. **Update documentation if needed**

6. **Commit your changes:**
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

7. **Push and create a pull request**

## Commit Message Guidelines

This project follows [Conventional Commits](https://conventionalcommits.org/):

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `style:` - Code style changes (formatting, etc.)
- `refactor:` - Code refactoring
- `test:` - Adding or updating tests
- `chore:` - Maintenance tasks

Examples:
- `feat: add snapshot support for event streams`
- `fix: resolve race condition in event watching`
- `docs: update installation instructions`

## Testing Guidelines

### Unit Tests
- Write tests for all new functionality
- Use descriptive test names
- Test both success and failure cases
- Mock external dependencies when appropriate
- Aim for high test coverage

### Integration Tests
- Test complete workflows
- Use temporary files/databases for isolation
- Clean up resources after tests

### Running Benchmarks
```bash
uv run python benchmark.py
```

## Documentation

### Code Documentation
- Use docstrings for all public functions, classes, and modules
- Follow Google-style docstrings
- Include type hints where possible

### README Updates
- Keep the README up to date with any API changes
- Update installation instructions if dependencies change
- Add examples for new features

## Pull Request Process

1. **Ensure all CI checks pass**
2. **Update CHANGELOG.md** with your changes
3. **Get review** from maintainers
4. **Address feedback** and make requested changes
5. **Merge** once approved

## Release Process

Releases are handled by maintainers:

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create git tag
4. Build and publish to PyPI
5. Create GitHub release

## Code of Conduct

Please be respectful and inclusive in all interactions. This project follows a code of conduct to ensure a welcoming environment for all contributors.

## Questions?

If you have questions about contributing, feel free to:
- Open an issue for discussion
- Reach out to maintainers
- Check existing issues and pull requests for similar topics

Thank you for contributing to py-event-sourcing! 🎉
