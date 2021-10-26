SFTPtoGS
=========

SFTPtoGS sends to Google Storage a list of files retrieved in the former step from a specified directory on a SFTP server.


Usage Notes
-----------
In order perform SFTPtoGS, it is needed to provide the Project ID and bucket on google storage where files will be stored.

The `Path` property defines the destination bucket (*gs://<bucket>/path/to/output*) where files will be stored in GCP.


Plugin Configuration
--------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Path** | **Y** | N/A | Bucket path where files will be stored.|
| **Num. retries** | **N** | 3 | Number of retries in case of upload fail.|
| **Time to wait** | **N** | 30 | Time between retries in seconds.|
| **Archive Original Files** | **Y** | No | Specifies if archive/delete original sftp is required.|
| **Archive/Remove** | **Y** | Archive | Select option to archive or delete originals.|
| **SFTP Target Path** | **Y** | Archive | Mandatory if *Archive* is selected. Path where original files will be archived.|
| **Credentials** | **Y** | File Path | Specifies the type of Authentication that will be used to connect to GCP.|
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
