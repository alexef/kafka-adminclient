import os

from setuptools import setup, find_packages

# Pull version from source without importing
# since we can't import something we haven't built yet :)
exec (open('kafkaadmin/version.py').read())

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

setup(
    name="kafka-python-admin",
    version=__version__,

    packages=find_packages(exclude=['test']),
    author="Alex Eftimie",
    author_email="alexeftimie@gmail.com",
    url="https://github.com/alexef/kafka-adminclient",
    license="Apache License 2.0",
    description="Pure Python Admin client for Apache Kafka based on dpkp/python-kafka",
    long_description=README,
    keywords="apache kafka",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    install_requires=[
        'kafka-python',
    ]
)
