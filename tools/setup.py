from setuptools import setup, find_packages

setup(
    name="latam-challenge",
    version="0.9",
    packages=find_packages(),
    install_requires=[
        "pydantic>=2.0.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "memory-profiler",
        "emoji",
        "ijson",
    ],
    python_requires=">=3.8",
    author="Juan Pablo Godoy",
    email="Godoypablojuan@gmail.com",
    description="LATAM Data Engineering Challenge",
) 