#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from setuptools import setup, find_packages
import pypegasus

setup(
    name='pypegasus3',
    version=pypegasus.__version__,
    install_requires=['Twisted==21.2.0', 'aenum==3.0.0', 'thrift==0.13.0', 'pyOpenSSL==20.0.1','cryptography==3.2'],
    packages=find_packages(),
    package_data={'': ['logger.conf']},
    platforms='any',
    url='https://github.com/apache/incubator-pegasus/python-client',
    license='Apache License 2.0',
    author='Yingchun Lai',
    author_email='laiyingchun@apache.org',
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
