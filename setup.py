"""Setup configuration for wdlp package"""
from setuptools import setup, find_packages
from pathlib import Path

# Read version from __init__.py
def get_version():
    """Read version from package __init__.py"""
    init_py = Path("src/wdlp/__init__.py").read_text()
    for line in init_py.split("\n"):
        if line.startswith("__version__"):
            return line.split("=")[-1].strip().strip('"')
    raise RuntimeError("Version not found")

# Read requirements from requirements.txt
def get_requirements():
    """Read requirements from requirements.txt"""
    with open("requirements.txt") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

# Read long description from README.md
long_description = Path("README.md").read_text(encoding="utf-8")

setup(
    name="wdlp",
    version=get_version(),
    description="Process FCC Wireless License Database .dat files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="FCC Data Tools Team",
    author_email="fcc-data-tools@example.com",
    url="https://github.com/fcc-data-tools/wdlp",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    package_data={
        "wdlp": ["py.typed"],  # For type checkers
    },
    entry_points={
        "console_scripts": [
            "wdlp=wdlp.main:main",
        ],
    },
    python_requires=">=3.8,<4.0",
    install_requires=get_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.0,<8.0",
            "pytest-cov>=4.0,<5.0",
            "black>=23.0,<24.0",
            "isort>=5.0,<6.0",
            "mypy>=1.0,<2.0",
            "flake8>=6.0,<7.0",
            "pylint>=2.0,<3.0",
            "types-all",  # Type stubs for third-party packages
        ],
        "docs": [
            "sphinx>=7.0,<8.0",
            "sphinx-rtd-theme>=1.0,<2.0",
            "myst-parser>=2.0,<3.0",  # For Markdown support
        ],
    },
    zip_safe=False,  # For mypy to find py.typed
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Typing :: Typed",
    ],
    keywords=[
        "fcc",
        "wireless",
        "license",
        "database",
        "data-processing",
        "data-conversion",
    ],
    project_urls={
        "Documentation": "https://wdlp.readthedocs.io/",
        "Source": "https://github.com/fcc-data-tools/wdlp",
        "Issues": "https://github.com/fcc-data-tools/wdlp/issues",
    },
)
