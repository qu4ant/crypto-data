#!/usr/bin/env python3
"""
Setup script for crypto-data - GitHub distribution
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read requirements from requirements.txt
requirements_path = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_path.exists():
    with open(requirements_path, 'r', encoding='utf-8') as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="crypto-data",
    version="4.0.0",
    description="DuckDB ingestion pipeline for cryptocurrency market data",
    url="https://github.com/qu4ant/crypto-data",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-asyncio>=0.21.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
            "requests-mock>=1.9.0",
        ]
    },
)