package com.vodafone.datafusion.plugins.sftpdelta.source;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.vodafone.datafusion.plugins.sftpdelta.common.GCSPath;
import com.vodafone.datafusion.plugins.sftpdelta.common.SFTPDeltaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.GC;

import java.io.IOException;

import static com.vodafone.datafusion.plugins.sftpdelta.constants.Constants.*;

/**
 * Class to manage the delta time
 */
public class SFTPDeltaDelta {
    private static final Logger LOG = LoggerFactory.getLogger(SFTPDeltaDelta.class);
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
                GoogleCredentials gcpCredentials = SFTPDeltaUtils
                        .getGCPCredentials(config.serviceAccountType, config.serviceAccountJSON, config.serviceFilePath);
                SFTPDeltaUtils
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
    public static Long getFromTime(Configuration conf, SFTPDeltaSourceConfig config) throws IOException {
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
                GoogleCredentials gcpCredentials = SFTPDeltaUtils
                        .getGCPCredentials(config.serviceAccountType, config.serviceAccountJSON, config.serviceFilePath);
                data = SFTPDeltaUtils
                        .readGCSFile(gcpCredentials, GCSPath.getBucketName(config.persistDelta), GCSPath.getObjectName(config.persistDelta));
                if (!Strings.isNullOrEmpty(data) && data.length() > 0) {
                    SFTPDeltaUtils.timestampValidation(data);
                    fromTime = Long.parseLong(data);
                }
            } else {
                Path source = new Path(config.persistDelta);
                FileSystem fileSystem = source.getFileSystem(conf);
                FSDataInputStream fsDataInputStream = fileSystem.open(source);
                resultByteArray = ByteStreams.toByteArray(fsDataInputStream);
                data = new String(resultByteArray);
                if (resultByteArray.length > 0) {
                    SFTPDeltaUtils.timestampValidation(data);
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

}
