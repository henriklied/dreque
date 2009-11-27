#!/usr/bin/env python

from distutils.core import setup

from dreque import __version__ as version

setup(
    name = 'dreque',
    version = version,
    description = 'Job queueing library',
    author = 'Samuel Stauffer',
    author_email = 'samuel@lefora.com',
    url = 'http://github.com/samuel/dreque',
    packages = ['dreque'],
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
