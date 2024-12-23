from setuptools import setup, find_packages

setup(
    name="latam-challenge",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pydantic>=2.0.0",
        "memory-profiler",
        "ijson",
    ],
) 