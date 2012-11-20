#Copyright 2011 SRI International
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
'''
Created on Oct 20, 2011

@author: jklo
'''
from setuptools import setup, find_packages

setup(
    name='LRDataPump',
    version='0.1.0',
    author='Jim Klo',
    author_email="jim.klo@sri.com",
    url = "https://github.com/jimklo/LRDataPump",
    description='Data Pump designed to work as a Cron',
    packages=find_packages(),
    install_requires = ["argparse","LRSignature","lxml","ijson"],
    license='Apache 2.0 License',
)
