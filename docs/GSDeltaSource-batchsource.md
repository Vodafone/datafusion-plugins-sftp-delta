GSDeltaSource
=========

GSDeltaSource allows creating a list of the files from the specified bucket on GCS.


Usage Notes
-----------
In order perform GSDeltaSource, we require GCP conectivity in order to connect and list the selected bucket.

This plugin is intended to be used in conjunction with GS to SFTP plugin.
The use of `Repartition` between GSDeltaSource and GCStoSFTP plugins will help in terms of parallelization.

Bucket on GCS which needs to be listed can be specified using `GCS Path` property.
The specified bucket/directory should exist and absolute path to the directory must be provided.
If directory is empty then execution will continue without any error.
If directory doesn't exist then execution will throw an Exception.

The files will be listed if modify date is from `Last date processed` to execution time.
If empty `Last date processed` will be 0.

The last time execution will be persisted in `Last date processed File`


Plugin Configuration
--------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Reference Name** | **N** | N/A | Name used to uniquely identify this sink for lineage, annotating metadata, etc.. |
| **GCS Path** | **Y** | N/A | Absolute path of the bucket/directory on GCS to be listed. If the directory is empty, the execution of the plugin will be no-op.|
| **Regex Path Filter** | **N** | N/A | Regex to choose only the files that are of interest. All files will be listed by default (.*). https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#sum|
| **Recursive** | **Y** | No | Allows the user to choose if list recursively on the GCS bucket/directory.|
| **Last date processed File** | **N** | N/A | Path to file with the last modification time file to persist, gs://. |
| **Last date processed** | **N** | N/A | The last modification time. They are represented as seconds from Jan 1, 1970 in UTC. |
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
