'''
This is a standard python setuptools setup file used for packaging and
installation of python code.
In this case we are using setup.py solely to allow installation via pip.
Example pip command for install:
'pip install git+ssh://git@github.com/secretlair/commons.git#egg=commons'
More information about python packaging:
https://the-hitchhikers-guide-to-packaging.readthedocs.io/en/latest/quickstart.html
http://python-packaging.readthedocs.io/en/latest/index.html
More information about pip installing from git repositories:
https://pip.pypa.io/en/stable/reference/pip_install/#git
'''

from setuptools import setup, find_packages
setup(
    name='s3_gateway',
    version='0.0.0',
    url='https://github.com/odrive/py-s3-gateway',
    author='odrive',
    author_email='support@odrive.com',
    packages=find_packages('src'),
    package_dir={'': 'src'},
)
