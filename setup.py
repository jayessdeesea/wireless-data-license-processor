from setuptools import setup, find_packages
import tempfile

setup(
    name="wdlp",
    version="0.1.0",
    description="A tool for processing .dat files and transforming them into various formats.",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "pydantic==2.0",
        "amazon.ion==0.5.0",
        "pandas==1.5.0",
        "pyarrow==10.0.0",
    ],
    entry_points={
        "console_scripts": [
            "wdlp=wdlp.main:main",
        ],
    },
)
