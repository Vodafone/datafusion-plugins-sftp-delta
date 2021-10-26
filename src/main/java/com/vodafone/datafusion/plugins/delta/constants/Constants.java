package com.vodafone.datafusion.plugins.delta.constants;

public class Constants {

    public static final String SFTP_TO_GCS_NAME = "SFTPtoGCS";
    public static final String GCS_TO_SFTP_NAME = "GCStoSFTP";
    public static final String PREVIEW_ENABLED = "exec.preview";
    public static final String SFTP_TO_GCS_BUFFERSIZE = "SFTPtoGCS.buffer_size";
    public static final int DEFAULT_BUFFER_SIZE = 15 * 1024 * 1024;
    public static final Long DEFAULT_SAFETY_READ_TIME = 300L;
    public static final int TIMEOUT = 300;
    public static final int MAX_WAIT_TIME = 300;
    public static final int MAX_RETRIES = 100;
    public static final Long ZERO = 0L;
    public static final String ZERO_STRING = "0";

    public static final String CONF_JSON_PACKAGE_KEY = "com.vodafone.datafusion.plugins.sftpdelta.source.config";
    public static final String CONF_LOGICAL_START_TIME = "logical.start.time";
    public static final String CONF_SAFETY_READ_TIME = "safety.read.time";

    public static final String SCHEMA_CONNECTION_TYPE = "connectionType";
    public static final String SCHEMA_CONNECTION = "connection";
    public static final String SCHEMA_FULLFILENAME = "fullfilename";
    public static final String SCHEMA_BASENAME = "basename";
    public static final String SCHEMA_FILENAME = "filename";
    public static final String SCHEMA_MTIME = "mtime";

    public static final String METRICS_SEPARATOR = "|";
    public static final String METRICS_UPLOADED_FILES = "total_uploaded_files";
    public static final String METRICS_SIZE_BYTES = "size_bytes";
    public static final String METRICS_CREATED_ON = "created_on";
    public static final String METRICS_SEEN_ON = "seen_on";
    public static final String METRICS_TRANSFERED_ON = "transferred_on";
    public static final String METRICS_TRANSFER_TIME = "transfer_time";
    public static final String METRICS_TRANSFER_RETRIES = "transfer_retries";

    public static final String MD5 = "MD5";

    public static final String CONN_PRIVATEKEY_SELECT = "privatekey";
    public static final String CONN_SFTP = "sftp";

    public static final String TRUE_STRING = "true";
    public static final String JSON = "json";
    public static final String KEY = "key";
    public static final String FILE = "file";
    public static final String HDFS = "hdfs";
    public static final String GS = "gs";
    public static final String ENCRYPT_EXTENSION = "pgp";
    public static final String AUTO_DETECT = "auto-detect";
    public static final String CONNECTION = "connection";
    public static final String FILENAME = "filename";
    public static final String SIZE = "size";
    public static final String MTIME = "mtime";
    public static final String PROJECT = "project";
    public static final String PATH = "path";
    public static final String TARGET_PATH = "targetPath";
    public static final String REFERENCENAME = "referenceName";
    public static final String FILEPATH = "filePath";
    public static final String SATYPE = "serviceAccountType";
    public static final String SERVICE_FILE_PATH = "serviceFilePath";
    public static final String SERVICE_ACCOUNT_JSON = "serviceAccountJSON";
    public static final String RENAME = "rename";
    public static final String REMOVE = "remove";
    public static final String TIME_TO_WAIT = "timeToWait";
    public static final String NUM_RETRIES = "numRetries";

    public static final String SFTP_SERVER = "sftpServer";
    public static final String SFTP_PORT = "sftpPort";
    public static final String SFTP_PATH = "sftpPath";
    public static final String REGEX_FILTER = "regexFilter";
    public static final String SFTP_USER = "sftpUser";
    public static final String SFTP_PASS = "sftpPass";
    public static final String SFTP_PRIVATEKEY = "PrivateKey";
    public static final String SFTP_PRIVATEKEYPASS = "PrivateKeyPass";
    public static final String SSH_PROPERTIES = "sshProperties";
    public static final String AUTHENTICATION = "authentication";
    public static final String PERSISTDELTA = "persistDelta";
    public static final String FROMDELTA = "fromDelta";
    public static final String RECURSIVE = "recursive";
    public static final String GCS_PATH = "gcsPath";
    public static final String PROXY_IP = "proxyIP";
    public static final String PROXY_PORT = "proxyPort";
    public static final String ENCRYPTION_PUBLIC_KEY_FILE_PATH = "publicKeyPath";
    public static final String NAME_ENCRYPTION = "encryption";
    public static final String ENCRYPTION_PRIVATE_KEY_FILE_PATH = "privateKeyPath";
    public static final String ENCRYPTION_PRIVATE_KEY_PASSWORD = "privateKeyPassword";

    public static final String SLASH = "/";
    public static final String EMPTY_STRING = "";
    public static final String DOT = ".";
    public static final String MACRO = "${";
    public static final String DOUBLE_DOT = "..";
    public static final String COLON = ":";
    public static final String AT = "@";
    public static final String YES = "yes";
    public static final String NO = "no";
    public static final String GS_ROOT = "gs://";
    public static final String SFTP_ROOT = "sftp://";
    public static final int UNIXTIME_DIGIT_NUMBER = 10;

    public static final int SSH_FX_NO_SUCH_FILE = 2;
}
