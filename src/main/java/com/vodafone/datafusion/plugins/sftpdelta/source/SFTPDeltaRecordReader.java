package com.vodafone.datafusion.plugins.sftpdelta.source;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.vodafone.datafusion.plugins.sftpdelta.common.SFTPConnector;
import com.vodafone.datafusion.plugins.sftpdelta.common.SFTPConnectorConfig;
import com.vodafone.datafusion.plugins.sftpdelta.common.SFTPDeltaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.zookeeper.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.vodafone.datafusion.plugins.sftpdelta.constants.Constants.*;
import static java.util.stream.Collectors.toList;


/**
 * An {@link RecordReader}
 */
public class SFTPDeltaRecordReader extends RecordReader<NullWritable, SFTPDeltaRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(SFTPDeltaRecordReader.class);
    private static final Gson gson = new GsonBuilder().create();

    private int total, current;
    private Long safetyReadTime;
    private Long toTime;
    private Long fromTime;

    private SFTPDeltaRecord data;

    private Iterator<SFTPDeltaRecord> records;

    private SFTPDeltaSourceConfig config;

    private ChannelSftp sftpChannel;
    private SFTPConnector conn;
    private String configJson;
    private String connectorJson;

    @Override
    public void initialize(InputSplit inputSplit,
                           TaskAttemptContext taskAttemptContext) throws IOException {
        LOG.debug("[SFTP Delta] Initializing record reader.");
        Configuration conf = taskAttemptContext.getConfiguration();
        configJson = conf.get(CONF_JSON_PACKAGE_KEY);

        config = gson.fromJson(configJson, SFTPDeltaSourceConfig.class);

        connectorJson = gson.toJson(new SFTPConnectorConfig(
                config.sftpServer,
                config.sftpPort,
                config.sftpPath,
                config.sftpUser,
                config.sftpPass,
                config.authType,
                config.privateKey,
                config.passphrase,
                config.sshProperties
        ));

        safetyReadTime = SFTPDeltaUtils.getSafetyTime(taskAttemptContext);

        toTime = Long.parseLong(conf.get(CONF_LOGICAL_START_TIME)) / 1000 - safetyReadTime;
        fromTime = SFTPDeltaDelta.getFromTime(conf, config);

        try {
            if (config.authType.equals(CONN_PRIVATEKEY_SELECT)) {
                this.conn = new SFTPConnector(config.sftpServer, config.sftpPort,
                        config.sftpUser, config.getPrivateKey(), config.getPassphrase(), config.getSSHProperties());
            } else {
                this.conn = new SFTPConnector(config.sftpServer, config.sftpPort,
                        config.sftpUser, config.sftpPass, config.getSSHProperties());
            }

            this.sftpChannel = this.conn.getSftpChannel();
            List<SFTPDeltaRecord> fileList = new ArrayList<>();
            listDirectory(sftpChannel, config.sftpPath, fileList);

            this.records = fileList.iterator();
            this.current = 0;
            this.total = fileList.size();
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] An error occurred initializing record reader: " + ex.getMessage());
            ex.printStackTrace();
            throw new IOException(ex);
        }
    }

    @Override
    public boolean nextKeyValue() {
        if (!records.hasNext()) {
            return false;
        }

        data = records.next();
        this.current++;

        return true;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public SFTPDeltaRecord getCurrentValue() {
        return data;
    }

    @Override
    public float getProgress() {
        if (total == 0) return 1.0f;

        return current / total;
    }

    @Override
    public void close() throws IOException {
        try {
            if (this.conn != null)
                this.conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * Validates if the modify date is between fromDelta to execution time
     *
     * @param entry Connection to the SFTP
     * @return delta
     */
    private boolean isNewFile(ChannelSftp.LsEntry entry) {
        SftpATTRS attrs = entry.getAttrs();
        return attrs.isReg() &&
                attrs.getMTime() > fromTime.intValue() &&
                attrs.getMTime() <= toTime.intValue();
    }

    /**
     * If the filename is match or not with regexFilter
     *
     * @param entry Connection to the SFTP
     * @return match
     * @throws SftpException If any SFTP errors occur
     */
    private boolean isMatches(ChannelSftp.LsEntry entry) {
        String name = entry.getFilename();

        if (Strings.isNullOrEmpty(config.regexFilter)) return true;

        return name.matches(config.regexFilter);
    }

    /**
     * listDirectory recursively list all files and subdirectories in a given directory.
     *
     * @param channelSftp Connection to the SFTP
     * @param path        The origin directory
     * @param fileList        The list of files
     * @throws SftpException If any SFTP errors occur
     */
    private void listDirectory(ChannelSftp channelSftp, String path, List<SFTPDeltaRecord> fileList) throws SftpException {
        LOG.info("[SFTP Delta] Listing directory: " + path);
        Vector<ChannelSftp.LsEntry> files = channelSftp.ls(path);
        String name;
        for ( ChannelSftp.LsEntry entry : files ) {
            if (isNewFile(entry) && isMatches(entry)) {
                name = path + "/" + entry.getFilename();
                fileList.add(new SFTPDeltaRecord(connectorJson,
                        config.getFullPath(),
                        config.sftpPath,
                        name,
                        entry.getAttrs().getSize(),
                        entry.getAttrs().getMTime()
                ));
            }

            if (entry.getAttrs().isDir()) {
                if (!entry.getFilename().equals(DOT) && !entry.getFilename().equals(DOUBLE_DOT)) {
                    listDirectory(channelSftp, path + SLASH + entry.getFilename(), fileList);
                }
            }
        }
        LOG.info("[SFTP Delta] Number of files listed: " + fileList.size());
        List<String> stringList = fileList.stream().map(file -> file.filename.substring(1)).collect(toList());
        LOG.info("[SFTP Delta] Files to be transferred: " + stringList);
    }
}
