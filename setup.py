from distutils.core import setup

setup(
	name='Python LSF Collection',
	version='0.0.1',
	author='David Irvine',
	author_email='irvined@gmail.com',
	packages=['lsfpy', 'lsfpy.test'],
	scripts=[],
	url='http://code.google.com/p/python-lsf-collection/',
	license='LICENSE.txt',
	description='A collection of classes and utilities for manipulating data and tools under platform LSF.',
	long_description=open('README.txt').read(),
)
