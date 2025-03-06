#!/usr/bin/env python
from setuptools import setup

setup(
      name='tap-tinybird', 
      version='0.1.0',
      description='Singer.io tap for extracting Tinybird search results.',
      author='P.A. Masse',
      url='http://www.split.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_tinybird'],
      install_requires=[
            'singer-python>=5.0.12',
            'requests',
            'pendulum',
            'tinybird-python-sdk @ git+https://github.com/tinybirdco/tinybird-python-sdk@2660627000adbdccf3e69301bcd6693c075a0827',
            'ujson',
            'voluptuous==0.10.5'
      ],
      entry_points='''
            [console_scripts]
            tap-tinybird=tap_tinybird:main
      ''',
      packages=['tap_tinybird']
)
