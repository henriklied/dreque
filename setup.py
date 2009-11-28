#!/usr/bin/env python

from distutils.core import setup

from dreque import __version__ as version

dependencies = ["redis"]

setup(
    name = 'dreque',
    version = version,
    description = 'Persistent job queueing library using Redis inspired by Resque',
    author = 'Samuel Stauffer',
    author_email = 'samuel@lefora.com',
    url = 'http://github.com/samuel/dreque',
    packages = ['dreque'],
    requires = dependencies,
    install_requires = dependencies,
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
