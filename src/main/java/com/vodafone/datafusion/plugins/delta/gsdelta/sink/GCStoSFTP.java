package com.vodafone.datafusion.plugins.delta.gsdelta.sink;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.google.common.base.Strings;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import com.vodafone.datafusion.plugins.delta.common.*;
import com.vodafone.datafusion.plugins.delta.common.sink.DeltaOutputFormatProvider;
import com.vodafone.datafusion.plugins.delta.encryption.FileEncrypt;
import com.vodafone.datafusion.plugins.delta.encryption.PGPCertUtil;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.*;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPOnePassSignatureList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Objects;

import static com.vodafone.datafusion.plugins.delta.common.DeltaUtils.emitLineage;
import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(GCS_TO_SFTP_NAME)
@Description("Sink plugin to send GCS files to SFTP.")
public class GCStoSFTP extends BatchSink<StructuredRecord, Void, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(GCStoSFTP.class);
    private GCStoSFTPConfig config;

    private ChannelSftp sftpChannel;
    private SFTPConnector sftpConnector;
    private MessageDigest messageDigest;
    private Storage storage;
    private GoogleCredentials credentials = null;

    private InputStream privateKeyStream;

    private boolean isPreviewEnabled;

    private StageMetrics metrics;
    private String pipelineName;
    private String runId;
    private String seenOn;

    private long fileRetries;

    public GCStoSFTP(GCStoSFTPConfig config) {
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer configurer) {
        super.configurePipeline(configurer);
        StageConfigurer stageConfigurer = configurer.getStageConfigurer();
        FailureCollector collector = stageConfigurer.getFailureCollector();
        config.validate(collector);
    }

    @Override
    public void prepareRun(BatchSinkContext context) throws Exception {
        LOG.debug("[SFTP Delta] Sink prepareRun.");
        try {
            FailureCollector collector = context.getFailureCollector();
            config.validate(collector);
            collector.getOrThrowException();

            context.getArguments().set(PREVIEW_ENABLED, String.valueOf(context.isPreviewEnabled()));

            Schema inputSchema = context.getInputSchema();
            if (null != inputSchema) {
                emitLineage(config.referenceName, context, inputSchema.getFields());
            }

            context.addOutput(Output.of(config.referenceName, new DeltaOutputFormatProvider(context)));
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error on sink prepareRun. " + ex.getMessage());
            throw new Exception(ex.getMessage());
        }
    }

    @Override
    public void initialize(BatchRuntimeContext context) throws Exception {
        LOG.debug("[SFTP Delta] Initializing sink.");
        super.initialize(context);
        isPreviewEnabled = Objects.equals(context.getArguments().get(PREVIEW_ENABLED), TRUE_STRING);
        pipelineName = context.getPipelineName();
        seenOn = String.valueOf(context.getLogicalStartTime());
        runId = context.getMetrics().getTags().get("wfr");

        if (!Strings.isNullOrEmpty(config.archiveOriginals) && !Strings.isNullOrEmpty(config.archiveOption)
                && config.archiveOriginals.equals("yes") && config.archiveOption.equals("rename")
                && Strings.isNullOrEmpty(config.targetPath) && !config.targetPath.startsWith(GS_ROOT)){
            LOG.error("[SFTP Delta] Invalid targetPath. GCS Paths should start with gs://");
            throw new Exception("Invalid targetPath. GCS Paths should start with gs://");
        }

        try {
            SFTPConnectorConfig connConfig = new SFTPConnectorConfig(config.sftpServer, config.sftpPort, config.sftpPath,
                    config.sftpUser, config.sftpPass, config.proxyIP, config.proxyPort, config.authType, config.privateKey, config.passphrase, config.sshProperties);

            sftpConnector = DeltaUtils.getSftpConnector(connConfig);
            sftpChannel = sftpConnector.getSftpChannel();
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error connecting to SFTP server: " + ex.getMessage());
            throw new Exception(ex);
        }

        try{
            sftpChannel.stat(config.sftpPath);
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] SFTP path does not exists: " + ex.getMessage());
            throw new Exception("[SFTP Delta] SFTP path does not exists: " + ex.getMessage());
        }

        if (!Strings.isNullOrEmpty(config.archiveOriginals) && !Strings.isNullOrEmpty(config.archiveOption)
                && config.archiveOriginals.equals("yes") && config.archiveOption.equals("rename")
                && Strings.isNullOrEmpty(config.targetPath) && !config.targetPath.startsWith(GS_ROOT)){
            LOG.error("[SFTP Delta] Invalid targetPath. GCS Paths should start with gs://");
            throw new Exception("[SFTP Delta] Invalid targetPath. GCS Paths should start with gs://");
        }

        metrics = context.getMetrics();
    }

    @Override
    public void onRunFinish(boolean succeeded, BatchSinkContext context) {
        LOG.info("[SFTP Delta] SFTP upload completed: " + succeeded);
        try {
            if(privateKeyStream != null) {
                privateKeyStream.close();
            }
        } catch (IOException e) {
            LOG.debug("[SFTP Delta] Error closing private key stream.");
        }
    }

    @Override
    public void transform(StructuredRecord input, Emitter<KeyValue<Void, Void>> emitter) throws Exception {
        super.transform(input, emitter);

        long startTime = System.currentTimeMillis();

        assert config.authType != null;

        if (isPreviewEnabled) {
            LOG.debug("[SFTP Delta] Preview enabled from {} to path {}", input.get(SCHEMA_FULLFILENAME), config.sftpPath);
            return;
        }

        if (null == credentials){
            storage = DeltaUtils.getGCPStorage(input.get(SCHEMA_CONNECTION_TYPE), input.get(SCHEMA_CONNECTION));
        }

        GCSPath gcsPath = GCSPath.from(input.get(SCHEMA_FULLFILENAME));
        BlobId blobId = BlobId.of(gcsPath.getBucket(), gcsPath.getName());
        Blob blob = storage.get(blobId);

        boolean isEncrypted = blob.getName().endsWith(ENCRYPT_EXTENSION);

        if (!Strings.isNullOrEmpty(config.sftpPath) && null != blob) {
            InputStream writtenStream = null;
            DigestInputStream cis = null;
            try{
                messageDigest = MessageDigest.getInstance(MD5);

                writtenStream = retryPolicyStorage(config, blob, input.get(SCHEMA_BASENAME), input.get(SCHEMA_FULLFILENAME));

                boolean checkLineage = false;
                if (!isEncrypted && null != writtenStream) {
                    cis = new DigestInputStream(writtenStream, messageDigest);
                    checkLineage = DeltaUtils.checkLineage(blob, messageDigest, cis);
                    if (!checkLineage) {
                        throw new Exception("[SFTP Delta] Error writing file.");
                    }
                }

                if((checkLineage || isEncrypted) && config.archiveOriginals.equals(YES)) {
                    if (config.archiveOption.equals(RENAME)) {
                        LOG.debug("[SFTP Delta] Archiving file: " + input.get(SCHEMA_FILENAME));
                        archiveGCSFile(input.get(SCHEMA_FULLFILENAME), config.targetPath);
                    } else if (config.archiveOption.equals(REMOVE)) {
                        LOG.debug("[SFTP Delta] Removing file: " + input.get(SCHEMA_FILENAME));
                        deleteGCSFile(input.get(SCHEMA_FULLFILENAME));
                    }
                }

                LOG.info("[SFTP Delta] File {} was written to sftp path: {}", (String) input.get(SCHEMA_FILENAME), config.sftpPath);
                setMetrics(blob, input, startTime);
                LOG.info("[SFTP Delta] Transferred {} bytes: from {} to {}", input.get(SIZE), input.get(SCHEMA_FULLFILENAME), config.sftpPath);
            } catch (Exception e){
                LOG.error("[SFTP Delta] File {} could not be written to sftp path", (String) input.get(SCHEMA_FILENAME));
            } finally {
                if (null != writtenStream) writtenStream.close();
                if (null != cis) cis.close();
            }
        }
    }

    @Override
    public void destroy() {
        if (sftpConnector != null) {
            try {
                sftpConnector.close();
            } catch (Exception e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @param config            SFTPtoGCSConfig configuration
     * @param blob              Blob object to write in sftp
     * @param fullfilePath      file name complete path
     * @return
     * @throws Exception
     */
    private InputStream retryPolicyStorage(GCStoSFTPConfig config, Blob blob, String basename, String fullfilePath) throws Exception{
        InputStream writtenStream;
        DigestInputStream cis = null;
        boolean isEncrypted = false;
        int retries = 0;

        RetryOnException retryHandler = new RetryOnException(config.numRetries, config.timeToWait);

        String filename = fullfilePath.substring(fullfilePath.lastIndexOf('/') + 1);
        if (filename.endsWith(ENCRYPT_EXTENSION) && null != config.privateKeyPath) {
            isEncrypted = true;
            filename = filename.substring(0, filename.length()-4);
        }

        while(true) {
            try {
                //write into SFTP server
                String filePath = fullfilePath.substring(basename.length() - 1);

                DeltaUtils.cdToSftpPath(sftpChannel, config.sftpPath, filePath);

                if (isEncrypted && null != config.privateKeyPath) {
                    FileEncrypt.decryptFileIS(sftpChannel, blob, config.privateKeyPath, config.privateKeyPassword.toCharArray(), filename);
                } else {
                    cis = new DigestInputStream(new ByteArrayInputStream(blob.getContent()), messageDigest);
                    sftpChannel.put(cis, filename);
                }
                writtenStream = sftpChannel.get(filename);
                break;
            } catch (SftpException ex) {
                retryHandler.exceptionOccurred(filename);
                retries++;
                continue;
            } catch (Exception ex) {
                LOG.error("[SFTP Delta] An error occurred writing file: " + ex.getMessage());
                throw ex;
            } finally {
                if(null != cis) cis.close();
            }
        }

        fileRetries = retries;

        return writtenStream;
    }

    /**
     * archive file in SFTP channel
     *
     * @param sourceFilePath    original sftp file to archive
     * @param targetFilePath    target path to archive file
     */
    private void archiveGCSFile(String sourceFilePath, String targetFilePath) {
        try {
            if(sourceFilePath.equals(targetFilePath)) {
                throw new Exception("[SFTP Delta] Origin and target directories are equal.");
            }
            GCSPath gcsSourcePath = GCSPath.from(sourceFilePath);
            GCSPath gcsTargetPath = GCSPath.from(targetFilePath);
            Blob blob = storage.get(gcsSourcePath.getBucket(), gcsSourcePath.getName());

            CopyWriter copyWriter = blob.copyTo(gcsTargetPath.getBucket(),
                    gcsTargetPath.getName() + gcsSourcePath.getName().substring(gcsSourcePath.getName().lastIndexOf(SLASH)));
            copyWriter.getResult();
            blob.delete();

            LOG.debug("[SFTP Delta] File {} has been archived to {}", sourceFilePath, targetFilePath);
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error archiving file {} to sftp target directory. Error: {}", sourceFilePath, ex.getMessage());
        }
    }

    /**
     * delete file in SFTP channel
     *
     * @param sourceFilePath    original sftp file to delete
     */
    private void deleteGCSFile(String sourceFilePath) {
        try {
            GCSPath gcsSourcePath = GCSPath.from(sourceFilePath);
            Blob blob = storage.get(gcsSourcePath.getBucket(), gcsSourcePath.getName());
            blob.delete();
            LOG.debug("[SFTP Delta] File {} has been deleted.", sourceFilePath);
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error deleting file {} from sftp source directory. Error: {}", sourceFilePath, ex.getMessage());
        }
    }

    /**
     * Process metrics
     *
     * @param blob          gcs file
     * @param inputFile     structured record
     * @param startTime     processing start time
     */
    private void setMetrics(Blob blob, StructuredRecord inputFile, Long startTime) {
        long endTime = System.currentTimeMillis();
        String metricBase = pipelineName + METRICS_SEPARATOR
                + runId + METRICS_SEPARATOR
                + inputFile.get(SCHEMA_FILENAME) + METRICS_SEPARATOR;

        metrics.gauge(metricBase + METRICS_CREATED_ON, ((Integer)inputFile.get(SCHEMA_MTIME)).longValue());
        metrics.gauge(metricBase + METRICS_SIZE_BYTES, Math.toIntExact(blob.getSize()));
        metrics.gauge(metricBase + METRICS_SEEN_ON, Long.parseLong(seenOn));
        metrics.gauge(metricBase + METRICS_TRANSFERED_ON, endTime);
        metrics.gauge(metricBase + METRICS_TRANSFER_RETRIES, fileRetries);
        metrics.gauge(metricBase + METRICS_TRANSFER_TIME, Math.toIntExact(endTime - startTime));
        metrics.count(metricBase + METRICS_UPLOADED_FILES, 1);

        LOG.info("[SFTP Delta] Processed file {} -> init {} - finish {}", inputFile.get(SCHEMA_FILENAME), startTime, endTime);
    }
    
}
