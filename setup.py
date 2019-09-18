# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='mercos-data-test',
    version='0.1.0',
    description='Python package for data manipulation',
    long_description=readme,
    author='Rodrigo Gehlen De Marco',
    author_email='rodrigo.g.marco@gmail.com',
    url='https://github.com/rodrigogehlendemarco/mercos-data-test',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)
