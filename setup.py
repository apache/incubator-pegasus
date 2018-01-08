from setuptools import setup

setup(
    name='pypegasus',
    version='0.1',
    install_requires=['twisted>=17.9.0', 'aenum>=2.0.9', 'thrift==0.9.3'],
    packages=['', 'base', 'rrdb', 'utils', 'operate', 'transport', 'replication'],
    url='',
    license='',
    author='Lai Yingchun',
    author_email='laiyingchun@xiaomi.com',
    description='python client for xiaomi/pegasus'
)
