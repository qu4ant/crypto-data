#!/usr/bin/env python3
"""
Setup script for Binance Historical Data Management System
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file for long description
readme_path = Path(__file__).parent / "docs" / "README.md"
long_description = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""

# Read requirements from requirements.txt
requirements_path = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_path.exists():
    with open(requirements_path, 'r', encoding='utf-8') as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="crypto-data",
    version="2.0.0",
    author="Guillaume Tonnerre",
    description="Pure data infrastructure for cryptocurrency market data - OHLCV from Binance, universe rankings from CoinGecko",
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
            "requests-mock>=1.9.0",  # For testing CoinGecko API
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Office/Business :: Financial",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    entry_points={
        "console_scripts": [
            "crypto-data-download=scripts.download_data:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)