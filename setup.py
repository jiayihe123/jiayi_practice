from platform import python_version
from setuptools import setup

setup(
    name="jiayi_beam_test",
    version="0.0.1",
    install_requires=[
        "apache-beam[gcp]==2.37.0",
    ],
    python_version="3.8"
)
