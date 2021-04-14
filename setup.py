from setuptools import setup, find_packages
import pypegasus

setup(
    name='pypegasus',
    version=pypegasus.__version__,
    install_requires=['Twisted==21.2.0', 'aenum==3.0.0', 'thrift==0.13.0', 'pyOpenSSL==20.0.1','cryptography==3.2'],
    packages=find_packages(),
    package_data={'': ['logger.conf']},
    platforms='any',
    url='https://github.com/XiaoMi/pegasus-python-client',
    license='Apache License 2.0',
    author='Lai Yingchun',
    author_email='laiyingchun@xiaomi.com',
    description='python3 client for apache/incubator-pegasus',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries'
    ],
    zip_safe=False
)
