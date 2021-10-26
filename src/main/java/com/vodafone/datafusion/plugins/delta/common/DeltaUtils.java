package com.vodafone.datafusion.plugins.delta.common;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DeltaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaUtils.class);

    /**
     * Validates if unix timestamp value is a valid value
     *
     * @param unixTimestamp Unix Timestamp
     * @throws Exception
     */
    public static void timestampValidation(String unixTimestamp) throws Exception {
        if (unixTimestamp.length() != UNIXTIME_DIGIT_NUMBER) {
            LOG.error("[SFTP Delta] Delta file format is not valid: " + unixTimestamp);
            throw new Exception("Delta file time format is not valid.");
        } else if (isFutureTimestamp(unixTimestamp)){
            LOG.error("[SFTP Delta] Delta file time is not valid. It contains a future value: " + unixTimestamp);
            throw new Exception("Delta file time is not valid. It contains a future value: " + unixTimestamp);
        }
    }

    /**
     *
     * @param credentials
     * @param bucketName
     * @param objectName
     * @return
     */
    public static String readGCSFile(GoogleCredentials credentials, String bucketName, String objectName) throws Exception {
        String fileContent;
        try {
            Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
            Blob blob = storage.get(bucketName, objectName);
            fileContent = new String(blob.getContent());
            LOG.debug("[SFTP Delta] GCS object value: " + fileContent);
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error reading file {} in GCS: {}", objectName, ex.getMessage());
            throw new IOException(ex);
        }
        return fileContent;
    }

    /**
     *
     * @param serviceAccountType
     * @param serviceAccountJSON
     * @param serviceFilePath
     * @return
     * @throws IOException
     */
    public static GoogleCredentials getGCPCredentials(String serviceAccountType, String serviceAccountJSON, String serviceFilePath)
            throws Exception {
        LOG.debug("[SFTP Delta] Getting GCP credentials.");
        GoogleCredentials credentials;
        try {
            if (serviceAccountType.equals(JSON)) {
                assert serviceAccountJSON != null;
                credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(serviceAccountJSON.getBytes()));
            } else {
                assert serviceFilePath != null;
                if (serviceFilePath.equals(AUTO_DETECT)) {
                    credentials = GoogleCredentials.getApplicationDefault();
                } else {
                    credentials = GoogleCredentials.fromStream(new FileInputStream(serviceFilePath));
                }
            }
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error getting GCP credentials: " + ex.getMessage());
            throw new Exception(ex);
        }
        return credentials;
    }

    /**
     *
     * @param serviceAccountType
     * @param serviceAccount
     * @return
     * @throws IOException
     */
    public static GoogleCredentials getGCPCredentials(String serviceAccountType, String serviceAccount)
            throws Exception {
        LOG.debug("[SFTP Delta] Getting GCP credentials.");
        GoogleCredentials credentials;
        try {
            if (serviceAccountType.equals(JSON)) {
                assert serviceAccount != null;
                credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(serviceAccount.getBytes()));
            } else {
                assert serviceAccount != null;
                if (serviceAccount.equals(AUTO_DETECT)) {
                    credentials = GoogleCredentials.getApplicationDefault();
                } else {
                    credentials = GoogleCredentials.fromStream(new FileInputStream(serviceAccount));
                }
            }
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error getting GCP credentials: " + ex.getMessage());
            throw new Exception(ex);
        }
        return credentials;
    }

    /**
     *
     * @param serviceAccountType
     * @param serviceAccount
     * @return
     */
    public static Storage getGCPStorage(String serviceAccountType, String serviceAccount) throws Exception {
        GoogleCredentials credentials;
        Storage storage;

        credentials = DeltaUtils
                .getGCPCredentials(serviceAccountType, serviceAccount);

        storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build().getService();

        return storage;
    }

    /**
     *
     * @param newValue      value to update
     * @param credentials   GCP credentials
     * @param bucketName    bucketName
     * @param objectName    objectName
     * @throws IOException
     */
    public static void updateGSFile(GoogleCredentials credentials, String bucketName,
                                    String objectName, String newValue) throws Exception{
        LOG.debug("[SFTP Delta] Updating GCS delta file.");
        try {
            Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
            Blob blob = storage.get(bucketName, objectName);
            if (null == blob || !blob.exists()){
                BlobId blobId = BlobId.of(bucketName, objectName);
                BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                storage.create(blobInfo);
                blob = storage.get(bucketName, objectName);
            }

            WritableByteChannel channel = blob.writer();
            channel.write(ByteBuffer.wrap(newValue.getBytes(UTF_8)));
            channel.close();
            LOG.debug("[SFTP Delta] Updated GCS delta file to: " + newValue);
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error updating file in GCS: {}", ex.getMessage());
            throw new Exception(ex);
        }
    }

    /**
     *  Informs if a bucket exists in GCS
     *
     * @param credentials   Google credentials
     * @param gsPath        path to check
     * @return
     */
    public static boolean checkGCSbucket(GoogleCredentials credentials, String gsPath) throws Exception{
        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
        try{
            storage.list(GCSPath.from(gsPath).getBucket());
        } catch (Exception ex) {
            throw new Exception("[SFTP Delta] Bucket does not exist.");
        }
        return true;
    }

    /**
     *  Informs if a object exists inside a bucket in GCS
     *
     * @param credentials   Google credentials
     * @param gsPath        path to check
     * @return
     */
    public static boolean checkGCSobject(GoogleCredentials credentials,String gsPath){
        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();

        Page<Blob>blobs=
                storage.list(GCSPath.from(gsPath).getBucket(),
                        Storage.BlobListOption.prefix(GCSPath.from(gsPath).getName()),
                        Storage.BlobListOption.currentDirectory());

        return blobs.getValues().iterator().hasNext();
    }

    /**
     * Retrieves safety.read.time property from pipeline
     *
     * @param context
     * @return
     */
    public static long getSafetyTime(TaskAttemptContext context) {
        Long safetyReadTime;

        try {
            safetyReadTime = Long.parseLong(context.getConfiguration().get(CONF_SAFETY_READ_TIME));
            if(safetyReadTime < 0){
                throw new Exception("[SFTP Delta] safety.read.time cannot be a negative value.");
            }
            LOG.debug("[SFTP Delta] safety.read.time param: {}", safetyReadTime);
        } catch (Exception e) {
            safetyReadTime = DEFAULT_SAFETY_READ_TIME;
        }

        return safetyReadTime;
    }

    /**
     * Retrieves safety.read.time property from pipeline
     *
     * @param context
     * @return
     */
    public static long getSafetyTime(BatchSourceContext context) {
        Long safetyReadTime;

        try {
            safetyReadTime = Long.parseLong(context.getArguments().get(CONF_SAFETY_READ_TIME));
            if(safetyReadTime < 0){
                throw new Exception("safety.read.time cannot be a negative value.");
            }
            LOG.debug("[SFTP Delta] safety.read.time param: ", safetyReadTime);
        } catch (Exception e) {
            safetyReadTime = DEFAULT_SAFETY_READ_TIME;
        }

        return safetyReadTime;
    }

    /**
     *
     * @param connConfig
     * @return
     * @throws Exception
     */
    public static SFTPConnector getSftpConnector(SFTPConnectorConfig connConfig) throws Exception{
        SFTPConnector conn;

        if (connConfig.authType.equals(CONN_PRIVATEKEY_SELECT)) {
            conn = new SFTPConnector(connConfig.sftpServer, connConfig.sftpPort,
                    connConfig.sftpUser, connConfig.getPrivateKey(), connConfig.proxyIP,connConfig.proxyPort, connConfig.getPassphrase(), connConfig.getSSHProperties());
        } else {
            conn = new SFTPConnector(connConfig.sftpServer, connConfig.sftpPort,
                    connConfig.sftpUser, connConfig.sftpPass, connConfig.proxyIP,connConfig.proxyPort, connConfig.getSSHProperties());
        }

        return conn;
    }

    /**
     *
     * @param context   batch sink context
     * @param fields    fields
     */
    public static void emitLineage(String referenceName, BatchSinkContext context, List<Schema.Field> fields) {
        LineageRecorder lineageRecorder = new LineageRecorder(context, referenceName);

        if (!fields.isEmpty()) {
            lineageRecorder.recordWrite("Write", "Wrote to DB table.",
                    fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
        }
    }

    public static boolean checkLineage(Blob blob, MessageDigest messageDigest, InputStream inputStream)
            throws Exception {
        String SFTPMD5 = md5(messageDigest);
        String gcsMD5 = blob != null ? blob.getMd5ToHexString() : null;
        boolean isEncripted = blob.getName().endsWith(ENCRYPT_EXTENSION);

        if (!Objects.equals(SFTPMD5, gcsMD5) && !isEncripted) {
            LOG.error("[SFTP Delta] MD5 does not match origin-target {}: {} <-> {} ", blob.getName(), SFTPMD5, gcsMD5);
            inputStream.close();
            throw new IOException("Check MD5 Error");
        }

        return true;
    }

    /**
     * Change directory in sftp server to folder where files will be written
     *
     * @param sftpChannel   sftp channel
     * @param basePath      base sftp path
     * @param filePath      file path, including subfolders
     * @throws SftpException
     */
    public static void cdToSftpPath(ChannelSftp sftpChannel, String basePath, String filePath) throws SftpException {
        String[] folders = filePath.substring(0, filePath.lastIndexOf(SLASH)).split(SLASH);
        sftpChannel.cd(basePath + SLASH);
        for (String folder : folders) {
            if (folder.length() > 0) {
                try {
                    sftpChannel.cd(folder);
                } catch (SftpException e) {
                    sftpChannel.mkdir(folder);
                    sftpChannel.cd(folder);
                }
            }
        }
    }

    /**
     * Validates if unix timestamp has a future value
     *
     * @param unixTimestamp Unix Timestamp
     * @return
     */
    private static boolean isFutureTimestamp(String unixTimestamp) {
        LocalDateTime now = java.time.LocalDateTime.now();
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(unixTimestamp)), ZoneOffset.UTC);
        return now.isBefore(dateTime);
    }

    /**
     * md5 conversor
     * @param md    message digest value
     * @return
     */
    private static String md5(MessageDigest md) {
        byte[] raw = md.digest();
        BigInteger bigInt = new BigInteger(1, raw);
        StringBuilder hash = new StringBuilder(bigInt.toString(16));
        while (hash.length() < 32) {
            hash.insert(0, '0');
        }

        return hash.toString();
    }

}
