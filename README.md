LRDatapump
==========

Overview
--------
This is a utility for extracting metadata from an OAI-PMH endpoint and publishing into a Learning Registry node.

It supports NSDL_DC and OAI_DC currently, but will most likely need to be extended to popluate the Resource Data Documents.


Prequisites
-----------
+ Python 2.7
+ virtualenv
+ CouchDB (optional but recommended for tracking what has been published)


Installation and Use
--------------------

1. Checkout files from GitHub


        $ git clone git://github.com/jimklo/LRDataPump.git
        $ git checkout -b threaded origin/threaded


2. Install a virtualenv and activate


        $ virtualenv --python=python2.7 myenv
        $ . myenv/bin/activate


3. Install dependendncies, you might also need a compiler and other libraries. Inspect the output and install other dependencies according to your environment


        (myenv)$ cd LRDatapump
        (myenv)$ pip install -e ./


4. Create a configuration file, myconfig.json. Modify the template below as appropriate for you.  Be sure to remove the comments as they are invalid in th config.


        {
            "config": {
                "gpgbin": "/usr/local/bin/gpg", 
                "keyId": "A8A790EA220403B7",                                /* Your GPG Key ID */
                "keyLocations": [                                           /* A list of HTTP(S) accesible locations for your GPG public key */
                    "http://pool.sks-keyservers.net:11371/pks/lookup?op=get&search=0xA8A790EA220403B7", 
                    "https://keyserver2.pgp.com/vkd/DownloadKey.event?keyid=0xA8A790EA220403B7"
                ], 
                "lr-test-data": false,                                      /* This should be false if publishing to production */
                "metadataPrefix": "oai_dc",                                 /* OAI-PMH metadataPrefix parameter */
                "passphrase": "secretsauce",                                /* Your GPG Private key passphrase */ 
                "path": "/oai",                                             /* OAI-PMH endpoint path on server */
                "publish_passwd": "password",                               /* Learning Registry Basic Auth Publishing password */
                "publish_url": "http://sandbox.learningregistry.org",       /* LR node you want to publish to */
                "publish_user": "jim.klo@somewhere.com",                    /* Learning Registry Basic Auth Publishing user */
                "server": "http://oer.equella.com",                         /* OAI-PMH endpoint server */
                "set": null,                                                /* OAI-PMH set to harvest */
                "sign": true,                                               /* GPG Sign Resource Data Documents */
                "tos": "http://www.learningregistry.org/information-assurances/open-information-assurances-1-0", 
                "verb": "ListRecords"
            }, 
            "couch": {
                "db": "equella_dpump",                                      /* local CouchDB to log publish progress */
                "server": "http://localhost:5984", 
                "user:passwd": "admin:password"
            }, 
            "identity": {
                "curator": "Equella",                                       /* identity block for Resource Data Doc, refer to spec for field defs */
                "signer": "Jim Klo @ SRI", 
                "submitter": "Jim Klo @ SRI", 
                "submitter_type": "agent"
            }, 
            "namespaces": {
                "dc": "http://purl.org/dc/elements/1.1/",                   /* list of namespaces used for parsing your metadata */
                "dct": "http://purl.org/dc/terms/",                         /* - add or update as necessary */
                "ieee": "http://www.ieee.org/xsd/LOMv1p0", 
                "nsdl_dc": "http://ns.nsdl.org/nsdl_dc_v1.02/", 
                "oai": "http://www.openarchives.org/OAI/2.0/", 
                "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/", 
                "xsi": "http://www.w3.org/2001/XMLSchema-instance"
            }
        }


5. Invoke datapump:


        (myenv)$ python -m datapump.run --config ./myconfig.json


6. Done


License
-------

Copyright 2012 SRI International

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Funding
-------

This project has been funded at least or in part with Federal funds from the U.S. Department of Education under Contract Number ED-04-CO-0040/0010. The content of this publication does not necessarily reflect the views or policies of the U.S. Department of Education nor does mention of trade names, commercial products, or organizations imply endorsement by the U.S. Government.
