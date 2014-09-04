try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os


def read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname)).read().strip()
    except IOError:
        return ''

setup(
    name='Xapiand',
    version='2.0b',
    author='German M. Bravo (Kronuz)',
    author_email='german.mb@gmail.com',
    packages=[
        'xapiand',
        'xapiand.bin',
        'xapiand.server',
        'xapiand.client',
        'xapiand.management',
        'xapiand.management.commands',
    ],
    url='http://pypi.python.org/pypi/Xapiand/',
    license='LICENSE.txt',
    description="Xapian indexing and querying server implemented in Python",
    long_description=read('README.rst'),
    install_requires=[
        "gevent",
    ],
)
