# vf-cis-cms-df-pg-sftp-delta

SFTP Delta
=========================

Contains plugins to perform code from a SFTP server to GCS, and from GCS to SFTP.
Uses a delta file to control uploading of files that have already been uploaded.

Blocks must work together:

    - SFTPDelta + SFTPtoGCS
    - GCSDelta + GCStoSFTP

Source Delta blocks read the files from the source and output a list of files that meet the delta condition.

Sink blocks upload files to destination.

It is recomended use a repartition plugin between source and sink.

*Output from source blocks is not the content of the readed files, is a list of files.*


Build
-----
To build this plugin:

```
   mvn clean package
```

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/sftpdelta-<version>.jar config-file <target/sftpdelta-<version>.json>

For example, if your artifact is named 'sftpdelta-<version>':

    > load artifact target/sftpdelta-<version>.jar config-file target/sftpdelta-<version>.json


## License and Trademarks

git © 2021 Vodafone

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.  
