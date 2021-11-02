SFTPtoGS
=========

SFTPtoGS sends to Google Storage a list of files retrieved from a specified directory on a SFTP server in the former step of the pipeline.


Usage Notes
-----------
This plugin is intended to be used in conjunction with SftpDelta Source plugin.
The use of `Repartition` between SftpDeltaSource and SFTPtoGCS plugins, will help in terms of parallelization.

The `Path` property defines the destination bucket (*gs://<bucket>/path/to/output*) where files will be stored in GCP.


Plugin Configuration
--------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Path** | **Y** | N/A | Bucket path where files will be stored.|
| **Num. retries** | **N** | 3 | Number of retries in case of upload fail. Limit value is 100.|
| **Time to wait** | **N** | 30 | Time between retries in seconds. Limit value is 300.|
| **Archive Original Files** | **Y** | No | Specifies whether the user intends to archive the original sftp files.|
| **Archive/Remove** | **Y** | Archive | Select option to archive or delete originals.|
| **SFTP Target Path** | **Y** | N/A | Mandatory if *Archive* is selected. Path where original files will be archived. Must exists.|
| **Encryption Algorithm**| **N** | NONE | Option to encrypt files with pgp encryption before uploading. |
| **Public Key Path** | **N** | N/A | Path to public key file into SFTP server. Mandatory if PGP encryption is selected.|
| **Service Account Type** | **Y** | File Path | Specifies the type of Authentication that will be used to connect to GCP.|
| **File Path**| **N** | auto-detect | Service account file path |
| **JSON** | **N** | N/A | Service account JSON containing private key to connect to GCP.|

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

For example, if your artifact is named 'sftpdelta-1.0.0':

    > load artifact target/sftpdelta-1.0.0.jar config-file target/sftpdelta-1.0.0.json
