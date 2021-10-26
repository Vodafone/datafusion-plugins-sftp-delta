SFTPDeltaSource
=========

GCStoSFTP allows writing into a SFTP server the list of files read from GCS.


Usage Notes
-----------
In order perform GCStoSFTP, we require host and port on which the SFTP server is running. SFTP implements secure file
transfer over SSH. Typically port number 22 is used for SFTP(which is also default port for SSH). We also require valid
credentials in the form of user name and password. Please make sure that you are able to SSH to the SFTP server using
specified user and password. SSH connection to SFTP server can be customized by providing additional configurations
such as enable host key checking by setting configuration property 'StrictHostKeyChecking' to 'yes'. These additional
configurations can be specified using `Properties for SSH` section.

Directory on the SFTP server which files will be stored can be specified using `Target Path` property. The specified
directory should exist and absolute path to the directory must be provided. If directory is empty then execution will
continue without any error. if directory doesn't exist then execution will throw an Exception.


Plugin Configuration
--------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Reference Name** | **N** | N/A | Name used to uniquely identify this sink for lineage, annotating metadata, etc.. |
| **Host** | **Y** | N/A | Specifies the host name of the SFTP server.|
| **Port** | **Y** | 22 | Numeric value that Specifies the port on which SFTP server is running.|
| **Target Path** | **Y** | N/A | Absolute path of the directory on the SFTP server which is to be listed. If the directory is empty, the execution of the plugin will be no-op.|
| **Username** | **Y** | N/A | Specifies the name of the user which will be used to connect to the SFTP server.|
| **Authentication** | **Y** | **PrivateKey** | Specifies the type of Authentication that will be used to connect to the SFTP Server.|
| **Private Key**| **N** | N/A | Private RSA Key to be used to connect to the SFTP Server. This key is recommended to be stored in the Secure Key Store, and macro called into the Configuration. Must be a RSA key starting with -----BEGIN RSA PRIVATE KEY-----|
| **Private Key Passphrase** | **N** | N/A | Passphrase to be used with RSA Private Key if a Passphrase was specified when key was generated.|
| **Password** | **N** | N/A | Specifies the password of the user. Only Required if Private Key is not being used.|
| **Num. retries** | **N** | 3 | Number of retries in case of upload fail.|
| **Time to wait** | **N** | 30 | Time between retries in seconds.|
| **Archive Original Files** | **Y** | No | Specifies if archive/delete original sftp is required.|
| **Archive/Remove** | **Y** | Archive | Select option to archive or delete originals.|
| **Properties for SSH** | **N** | N/A | Specifies the properties that are used to configure SSH connection to the SFTP server. For example to enable verbose logging add property 'LogLevel' with value 'VERBOSE'. To enable host key checking set 'StrictHostKeyChecking' to 'yes'. SSH can be configured with the properties described here 'https://linux.die.net/man/5/ssh_config'. |

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
