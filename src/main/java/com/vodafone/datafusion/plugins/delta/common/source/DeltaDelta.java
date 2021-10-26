package com.vodafone.datafusion.plugins.delta.common.source;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.vodafone.datafusion.plugins.delta.common.GCSPath;
import com.vodafone.datafusion.plugins.delta.common.DeltaUtils;
import com.vodafone.datafusion.plugins.delta.gsdelta.source.GSDeltaSourceConfig;
import com.vodafone.datafusion.plugins.delta.sftpdelta.source.SFTPDeltaSourceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;

/**
 * Class to manage the delta time
 */
public class DeltaDelta {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaDelta.class);
    /**
     * Save the last time
     *
     * @param time      date to save
     * @param config    SFTPDeltaSourceConfig config
     * @throws IOException
     */
    public static void setPersistTime(Long time, SFTPDeltaSourceConfig config) throws Exception {
        if (config.persistDelta == null || config.persistDelta.equals(EMPTY_STRING)) {
            return;
        }
        
        try {
            if (config.persistDelta.startsWith(GS_ROOT)) {
                GoogleCredentials gcpCredentials = DeltaUtils
                        .getGCPCredentials(config.serviceAccountType, config.serviceAccountJSON, config.serviceFilePath);
                DeltaUtils
                        .updateGSFile(gcpCredentials, GCSPath.getBucketName(config.persistDelta), GCSPath.getObjectName(config.persistDelta), time.toString());
            } else {
                Path source = new Path(config.persistDelta);
                Configuration conf = new Configuration();
                FileSystem fileSystem = source.getFileSystem(conf);
                fileSystem.mkdirs(source.getParent());
                FSDataOutputStream outputStream = fileSystem.create(source);
                outputStream.write(time.toString().getBytes());
                outputStream.close();
                fileSystem.close();
                LOG.debug("[SFTP Delta] Updated delta file to value: " + time);
            }
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Delta file could not be updated.");
            throw new Exception(ex);
        }
    }

    /**
     * Save the last time
     *
     * @param time      date to save
     * @param config    GSDeltaSourceConfig config
     * @throws IOException
     */
    public static void setPersistTime(Long time, GSDeltaSourceConfig config) throws Exception {
        if (config.persistDelta == null || config.persistDelta.equals(EMPTY_STRING)) {
            return;
        }

        try {
            if (config.persistDelta.startsWith(GS_ROOT)) {
                GoogleCredentials gcpCredentials = DeltaUtils
                        .getGCPCredentials(config.serviceAccountType, config.serviceAccountJSON, config.serviceFilePath);
                DeltaUtils
                        .updateGSFile(gcpCredentials, GCSPath.getBucketName(config.persistDelta), GCSPath.getObjectName(config.persistDelta), time.toString());
            } else {
                Path source = new Path(config.persistDelta);
                Configuration conf = new Configuration();
                FileSystem fileSystem = source.getFileSystem(conf);
                fileSystem.mkdirs(source.getParent());
                FSDataOutputStream outputStream = fileSystem.create(source);
                outputStream.write(time.toString().getBytes());
                outputStream.close();
                fileSystem.close();
                LOG.debug("[SFTP Delta] Updated delta file to value: " + time);
            }
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Delta file could not be updated.");
            throw new Exception(ex);
        }
    }

    /**
     * Calculate the Delta
     *
     * @param conf      Configuration CDAP
     * @param config    SFTPDeltaSourceConfig
     * @throws IOException
     */
    public static Long getFromTime(Configuration conf, SFTPDeltaSourceConfig config) {
        String data;
        byte[] resultByteArray;
        Long fromTime = ZERO;

        if (config.fromDelta != null) {
            return config.fromDelta;
        }

        if (config.persistDelta == null || config.persistDelta.equals(EMPTY_STRING)) {
            return ZERO;
        }

        try {
            LOG.info("[SFTP Delta] Reading delta file from path: " + config.persistDelta);
            if (config.persistDelta.startsWith(GS_ROOT)) {
                GoogleCredentials gcpCredentials = DeltaUtils
                        .getGCPCredentials(config.serviceAccountType, config.serviceAccountJSON, config.serviceFilePath);
                data = DeltaUtils
                        .readGCSFile(gcpCredentials, GCSPath.getBucketName(config.persistDelta), GCSPath.getObjectName(config.persistDelta));
                if (!Strings.isNullOrEmpty(data) && data.length() > 0) {
                    DeltaUtils.timestampValidation(data);
                    fromTime = Long.parseLong(data);
                }
            } else {
                Path source = new Path(config.persistDelta);
                FileSystem fileSystem = source.getFileSystem(conf);
                FSDataInputStream fsDataInputStream = fileSystem.open(source);
                resultByteArray = ByteStreams.toByteArray(fsDataInputStream);
                data = new String(resultByteArray);
                if (resultByteArray.length > 0) {
                    DeltaUtils.timestampValidation(data);
                    fromTime = Long.parseLong(data);
                }
                fileSystem.close();
            }
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error reading Delta file: {}. {}", config.persistDelta, ex.getMessage());
            LOG.info("[SFTP Delta] 0 is assigned to delta value.");
            fromTime = ZERO;
        }
        return fromTime;
    }

    /**
     * Calculate the Delta
     *
     * @param config    SFTPDeltaSourceConfig
     * @throws IOException
     */
    public static Long getFromTime(GSDeltaSourceConfig config) {
        String data;
        Long fromTime = ZERO;

        if (config.fromDelta != null) {
            return config.fromDelta;
        }

        if (config.persistDelta == null || config.persistDelta.equals(EMPTY_STRING)) {
            return ZERO;
        }

        try {
            LOG.info("[SFTP Delta] Reading delta file from path: " + config.persistDelta);
            GoogleCredentials gcpCredentials = DeltaUtils
                    .getGCPCredentials(config.serviceAccountType, config.serviceAccountJSON, config.serviceFilePath);
            data = DeltaUtils
                    .readGCSFile(gcpCredentials, GCSPath.getBucketName(config.persistDelta), GCSPath.getObjectName(config.persistDelta));
            if (!Strings.isNullOrEmpty(data) && data.length() > 0) {
                DeltaUtils.timestampValidation(data);
                fromTime = Long.parseLong(data);
            }
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error reading Delta file: {}. {}", config.persistDelta, ex.getMessage());
            LOG.info("[SFTP Delta] 0 is assigned to delta value.");
            fromTime = ZERO;
        }
        return fromTime;
    }
}
