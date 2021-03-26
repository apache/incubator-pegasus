from setuptools import setup, find_packages
import pypegasus

setup(
    name='pypegasus',
    version=pypegasus.__version__,
    install_requires=['twisted>=17.9.0', 'aenum>=2.0.9', 'thrift>=0.9.3', 'pyopenssl>=17.5.0'],
    packages=find_packages(),
    package_data={'': ['logger.conf']},
    platforms='any',
    url='https://github.com/XiaoMi/pegasus-python-client',
    license='Apache License 2.0',
    author='Lai Yingchun',
    author_email='laiyingchun@xiaomi.com',
    description='python client for xiaomi/pegasus',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 2',
        'Topic :: Software Development :: Libraries'
    ],
    zip_safe=False
)
