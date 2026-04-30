# Contributing to Crypto Data

First off, thank you for considering contributing to crypto-data! It's people like you that make this package better for the entire quant/crypto community.

## 📋 Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Testing Requirements](#testing-requirements)
- [Pull Request Process](#pull-request-process)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Features](#suggesting-features)

---

## 🤝 Code of Conduct

This project adheres to a simple code of conduct:

- **Be respectful**: Treat everyone with respect and consideration
- **Be constructive**: Provide helpful, actionable feedback
- **Be patient**: Remember that maintainers and contributors are volunteers
- **Be collaborative**: Work together towards the common goal

Unacceptable behavior will not be tolerated. Please report issues to the maintainers.

---

## 💡 How Can I Contribute?

### Types of Contributions We Welcome

1. **Bug Reports**: Found a bug? Let us know!
2. **Bug Fixes**: Fix bugs in the codebase
3. **Documentation**: Improve docs, add examples, fix typos
4. **Features**: Propose and implement new features (after discussion)
5. **Tests**: Add test coverage for untested code
6. **Performance**: Optimize slow operations

### Philosophy: Keep It Simple

This package does **ONE thing**: data ingestion. We intentionally avoid:

- ❌ Query builders or ORMs
- ❌ Data loaders or readers (users query DuckDB directly)
- ❌ Trading strategies or indicators
- ❌ GUI interfaces

**We prioritize**: Simplicity > Features

---

## 🛠️ Development Setup

### 1. Fork and Clone

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/crypto-data.git
cd crypto-data
```

### 2. Create a Virtual Environment

```bash
# Using venv
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Or using conda
conda create -n crypto-data python=3.11
conda activate crypto-data
```

### 3. Install Development Dependencies

```bash
pip install -e ".[dev]"
```

This installs:
- Core dependencies (duckdb, pandas, aiohttp)
- Development tools (pytest, black, flake8, mypy)
- Validation framework (pandera, scipy)

### 4. Verify Installation

```bash
# Run tests to ensure everything works
pytest tests/ -v

# Should show: 347 tests passing
```

---

## 📝 Coding Standards

### Python Style

We follow **PEP 8** with some relaxations:

- **Line length**: 100 characters (not 79)
- **Quotes**: Single quotes preferred, double quotes for strings with single quotes
- **Imports**: Organized (standard library → third-party → local)

### Use Black for Formatting

```bash
# Format all Python files
black src/ tests/

# Check formatting without modifying
black --check src/ tests/
```

### Type Hints

All functions should have type hints:

```python
# ✅ GOOD
def download_data(symbol: str, start_date: str) -> bool:
    pass

# ❌ BAD
def download_data(symbol, start_date):
    pass
```

### Docstrings

Use **NumPy-style docstrings**:

```python
def my_function(param1: int, param2: str) -> List[str]:
    """
    Brief description of what the function does.

    Longer description if needed. Explain the purpose,
    not just what it does.

    Parameters
    ----------
    param1 : int
        Description of param1
    param2 : str
        Description of param2

    Returns
    -------
    List[str]
        Description of return value

    Raises
    ------
    ValueError
        When param1 is negative

    Examples
    --------
    >>> my_function(42, 'test')
    ['result1', 'result2']
    """
    pass
```

### Linting

```bash
# Check for style issues
flake8 src/ tests/

# Type checking
mypy src/
```

---

## ✅ Testing Requirements

### Writing Tests

- **Use pytest**: All tests must use pytest framework
- **Test organization**: Mirror source structure in `tests/`
- **Naming**: Test files must start with `test_`, functions with `test_`
- **Fixtures**: Use conftest.py for shared fixtures
- **Async tests**: Use `pytest-asyncio` for async functions

### Test Example

```python
# tests/binance_datasets/test_new_feature.py
import pytest
from crypto_data.binance_datasets import BinanceKlinesDataset, Period
from crypto_data.clients.coinmarketcap import CoinMarketCapClient
from crypto_data.enums import DataType, Interval

class TestNewFeature:
    def test_basic_functionality(self):
        """Test that basic feature works as expected."""
        # Arrange
        dataset = BinanceKlinesDataset(DataType.SPOT, Interval.MIN_5)

        # Act
        result = dataset.build_temp_filename("BTCUSDT", period=Period("2024-01"))

        # Assert
        assert result == "BTCUSDT-spot-5m-2024-01.zip"

    @pytest.mark.asyncio
    async def test_async_functionality(self, monkeypatch):
        """Test async feature."""
        async def fake_call(*args, **kwargs):
            return {
                "data": [
                    {"symbol": "BTC", "cmcRank": 1, "quotes": [{"marketCap": 1}], "tags": []}
                ]
            }

        async with CoinMarketCapClient() as client:
            monkeypatch.setattr(client, "_call_with_retry", fake_call)
            result = await client.get_historical_listings("2024-01-01", 1)

        assert result[0]["symbol"] == "BTC"
```

### Running Tests

```bash
# Run default local test suite (external validation tests are skipped)
pytest tests/ -v

# Run external validation tests against Binance/CoinMarketCap APIs
pytest tests/ -v --run-validation

# Run specific test file
pytest tests/binance/test_downloader.py -v

# Run tests with coverage
pytest tests/ --cov=crypto_data --cov-report=html

# Run tests matching pattern
pytest tests/ -k "test_download" -v

# Run fast tests only (skip slow integration tests)
pytest tests/ -m "not slow" -v
```

### Coverage Requirements

- **Minimum coverage**: 80% (currently at 88.44%)
- **New code**: Must have 100% coverage
- **Critical paths**: Error handling, data validation must be tested

### Test Categories

Mark tests appropriately:

```python
@pytest.mark.slow
def test_download_full_dataset():
    """Slow integration test."""
    pass

@pytest.mark.integration
def test_end_to_end_workflow():
    """Integration test."""
    pass
```

---

## 🔄 Pull Request Process

### 1. Create a Branch

```bash
# Create a feature branch
git checkout -b feature/my-awesome-feature

# Or a bugfix branch
git checkout -b fix/issue-123
```

### 2. Make Your Changes

- Write code following our standards
- Add tests for new functionality
- Update documentation if needed
- Keep commits focused and atomic

### 3. Test Your Changes

```bash
# Run full test suite
pytest tests/ -v

# Check formatting
black --check src/ tests/

# Check linting
flake8 src/ tests/

# Check types
mypy src/
```

### 4. Update Documentation

If you:
- Add a new public function → Update docstrings + README
- Change behavior → Update CLAUDE.md + CHANGELOG
- Fix a bug → Add to CHANGELOG under "Bug Fixes"
- Add a feature → Add to CHANGELOG under "New Features"

### 5. Commit Your Changes

```bash
# Use clear, descriptive commit messages
git add .
git commit -m "Add feature: async download retry logic

- Implement exponential backoff for 429 errors
- Add max_retries parameter to client
- Update tests for retry behavior
- Add documentation for new parameter"
```

**Commit Message Guidelines**:
- First line: <50 chars, imperative mood ("Add", not "Added")
- Body: Explain WHAT and WHY, not HOW
- Reference issues: "Fixes #123" or "Closes #456"

### 6. Push and Create PR

```bash
# Push to your fork
git push origin feature/my-awesome-feature
```

Then create a Pull Request on GitHub.

### 7. PR Checklist

Before submitting, ensure:

- [ ] All tests pass (`pytest tests/ -v`)
- [ ] Code is formatted (`black src/ tests/`)
- [ ] No linting errors (`flake8 src/ tests/`)
- [ ] Coverage maintained/improved (`pytest --cov`)
- [ ] Documentation updated (if applicable)
- [ ] CHANGELOG.md updated (if user-facing change)
- [ ] Commit messages are clear and descriptive

### 8. Code Review

- Be responsive to feedback
- Make requested changes promptly
- Ask questions if feedback is unclear
- Be patient - reviews take time

---

## 🐛 Reporting Bugs

### Before Submitting

1. **Search existing issues**: Has this been reported before?
2. **Try latest version**: Bug might already be fixed
3. **Minimal reproduction**: Can you reproduce with minimal code?

### Bug Report Template

```markdown
## Bug Description
Clear description of what the bug is.

## Steps to Reproduce
1. Download data with: `create_binance_database(...)`
2. Run query: `SELECT ...`
3. See error

## Expected Behavior
What you expected to happen.

## Actual Behavior
What actually happened (include error message).

## Environment
- OS: [e.g., Ubuntu 22.04]
- Python version: [e.g., 3.11.2]
- crypto-data version: [e.g., 4.0.0]
- DuckDB version: [e.g., 0.9.1]

## Logs
```
Paste relevant logs here (use code blocks)
```

## Additional Context
Any other relevant information.
```

---

## 💭 Suggesting Features

### Feature Request Template

```markdown
## Feature Description
Clear description of the feature you'd like.

## Use Case
Explain WHY this feature would be useful.
What problem does it solve?

## Proposed Solution
How would you implement this? (Optional)

## Alternatives Considered
What other approaches did you think about?

## Impact
- Breaking change? Yes/No
- Affects: [users/developers/both]
- Complexity: [low/medium/high]
```

### Feature Discussion

1. **Open an issue first**: Discuss before coding
2. **Wait for approval**: Ensure feature aligns with project philosophy
3. **Keep it simple**: Follow the "Simplicity > Features" principle
4. **Consider scope**: Does this belong in crypto-data or a separate package?

---

## 🔍 Code Review Guidelines

### For Reviewers

- **Be kind**: Critique code, not people
- **Be specific**: Point to exact lines, suggest improvements
- **Be timely**: Review within 48 hours if possible
- **Be thorough**: Check tests, docs, edge cases

### For Contributors

- **Be responsive**: Address feedback promptly
- **Ask questions**: If unclear, ask for clarification
- **Don't take it personally**: Reviews improve code quality
- **Iterate**: Small changes are easier to review

---

## 📚 Additional Resources

- [README.md](README.md) - User-facing documentation
- [CLAUDE.md](CLAUDE.md) - Technical documentation for developers
- [CHANGELOG.md](CHANGELOG.md) - Version history
- [GitHub Issues](https://github.com/qu4ant/crypto-data/issues) - Bug reports and discussions

---

## 🎉 Recognition

Contributors will be:
- Listed in commit history
- Mentioned in release notes (if significant contribution)
- Credited in README (for major features)

---

## 📧 Questions?

- **General questions**: Open a GitHub Discussion
- **Bug reports**: Open a GitHub Issue
- **Security issues**: Email maintainers directly (see README)

---

**Thank you for contributing to crypto-data!** 🚀

Every contribution, no matter how small, helps make this package better for the entire community.
