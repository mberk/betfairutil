from setuptools import setup

with open("README.md", "r") as f:
    long_description = f.read()

requires = ["betfairlightweight"]
tests_require = ["pytest"]

setup(
    name="betfairutil",
    version="0.7.1",
    description="Utility functions for working with Betfair data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Maurice Berk",
    author_email="maurice@mauriceberk.com",
    url="https://github.com/mberk/betfairutil",
    packages=[
        "betfairutil",
        "betfairutil.examples",
    ],
    install_requires=requires,
    tests_require=tests_require,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    extras_require={
        "files": ["betfairlightweight>=2.12.0", "orjson", "smart_open"],
        "data_frames": ["pandas"],
    },
)
