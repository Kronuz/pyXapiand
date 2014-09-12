try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os

from xapiand import version


def read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname)).read().strip()
    except IOError:
        return ''

setup(
    name='Xapiand',
    version=version,
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
    url='http://github.com/Kronuz/Xapiand',
    license='MIT / GPL2',
    keywords='indexing server xapian',
    description="Xapian indexing and querying server implemented in Python",
    long_description=read('README.rst'),
    entry_points={
        'console_scripts': [
            'xapiand = xapiand.__main__:main',
        ]
    },
    install_requires=[
        "gevent",
    ],
)
