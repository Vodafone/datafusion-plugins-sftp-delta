package com.vodafone.datafusion.plugins.sftpdelta.sink;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.jcraft.jsch.ChannelSftp;

import com.splunk.*;

import com.vodafone.datafusion.plugins.sftpdelta.common.*;
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
import io.cdap.plugin.common.LineageRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.vodafone.datafusion.plugins.sftpdelta.constants.Constants.*;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(SFTP_TO_GCS_NAME)
@Description("Sink plugin to send GCS Bucket from SFTP.")
public class SFTPtoGCS extends BatchSink<StructuredRecord, Void, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(SFTPtoGCS.class);
    private static final Gson gson = new GsonBuilder().create();
    private final SFTPtoGCSConfig config;

    SFTPConnectorConfig connConfig;
    private ChannelSftp sftpChannel;
    private SFTPConnector conn;
    private boolean isPreviewEnabled;

    private Storage storage;
    private GoogleCredentials credentials;
    private GCSPath gcsPath;

    private StageMetrics metrics;
    private String pipelineName;
    private String runId;
    private String seenOn;
    private long fileRetries;

    int bufSize = DEFAULT_BUFFER_SIZE;

    public SFTPtoGCS(SFTPtoGCSConfig config) {
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
    public void onRunFinish(boolean succeeded, BatchSinkContext context) {
        LOG.info("[SFTP Delta] GCS upload completed: " + succeeded);
    }

    @Override
    public void prepareRun(BatchSinkContext context) throws Exception {
        LOG.debug("[SFTP Delta] Sink prepareRun.");
        try {
            FailureCollector collector = context.getFailureCollector();
            config.validate(collector);
            collector.getOrThrowException();

            context.getArguments().set(SFTP_TO_GCS_PREVIEW, String.valueOf(context.isPreviewEnabled()));

            Schema inputSchema = context.getInputSchema();
            if (null != inputSchema) {
                emitLineage(context, inputSchema.getFields());
            }

            context.addOutput(Output.of(config.referenceName, new SFTPtoGCSOutputFormatProvider(context)));
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error on sink prepareRun. " + ex.getMessage());
            throw new Exception(ex.getMessage());
        }
    }

    @Override
    public void initialize(BatchRuntimeContext context) throws Exception {
        LOG.debug("[SFTP Delta] Initializing sink.");
        super.initialize(context);
        isPreviewEnabled = Objects.equals(context.getArguments().get(SFTP_TO_GCS_PREVIEW), TRUE_STRING);
        pipelineName = context.getPipelineName();
        seenOn = String.valueOf(context.getLogicalStartTime());
        runId = context.getMetrics().getTags().get("run");

        try {
            bufSize = Integer.parseInt(Objects.requireNonNull(context.getArguments().get(SFTP_TO_GCS_BUFFERSIZE))) * 1024 * 1024;
            LOG.debug("[SFTP Delta] Sink bufSize param: ", bufSize);
        } catch (Exception e) {
            bufSize = DEFAULT_BUFFER_SIZE;
        }

        try {
            assert config.serviceAccountType != null;
            credentials = SFTPDeltaUtils
                    .getGCPCredentials(config.serviceAccountType, config.serviceAccountJSON, config.serviceFilePath);

            storage = StorageOptions.newBuilder()
                    .setCredentials(credentials)
                    .build().getService();

            if(!SFTPDeltaUtils.checkGCSbucket(credentials, config.path)){
                throw new Exception("[SFTP Delta] Bucket does not exist");
            }
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error defining GCS storage: " + ex.getMessage());
            throw new Exception(ex);
        }
        metrics = context.getMetrics();
    }

    @Override
    public void transform(StructuredRecord input, Emitter<KeyValue<Void, Void>> emitter) throws Exception {
        long startTime = System.currentTimeMillis();
        String gcsFilename;
        String jsonConnection = input.get(CONNECTION);
        String filename = input.get(FILENAME);

        connConfig = gson.fromJson(jsonConnection, SFTPConnectorConfig.class);
        gcsPath = GCSPath.from(config.path);

        String fullSourcePath = connConfig.sftpPath + filename;
        String gPath = gcsPath.getUri().getPath();

        if (gPath.length() > 1 && gPath.endsWith(SLASH)) {
            gcsFilename = gcsPath.getUri().getPath().substring(1, gPath.length() - 1) + filename;
        } else {
            gcsFilename = gcsPath.getUri().getPath().substring(1) + filename;
        }

        if (isPreviewEnabled) {
            LOG.debug("[SFTP Delta] Preview enabled to path: {} -> {} - {}", fullSourcePath, gcsPath.getBucket(), gcsFilename);
            return;
        }

        if (conn == null || !conn.isConnected(connConfig.sftpServer, connConfig.sftpPort, connConfig.sftpUser)) {
            if (conn != null) {
                conn.close();
            }

            if (connConfig.authType.equals(CONN_PRIVATEKEY_SELECT)) {
                conn = new SFTPConnector(connConfig.sftpServer, connConfig.sftpPort,
                        connConfig.sftpUser, connConfig.getPrivateKey(), connConfig.getPassphrase(), connConfig.getSSHProperties());
            } else {
                conn = new SFTPConnector(connConfig.sftpServer, connConfig.sftpPort,
                        connConfig.sftpUser, connConfig.sftpPass, connConfig.getSSHProperties());
            }

            sftpChannel = conn.getSftpChannel();
        }

        InputStream inputStream = sftpChannel.get(fullSourcePath);
        MessageDigest messageDigest = MessageDigest.getInstance(MD5);
        DigestInputStream cis = new DigestInputStream(inputStream, messageDigest);

        try {
            LOG.debug("[SFTP Delta] Processing file: " + fullSourcePath);

            Blob blob = retryPolicyStorage(config, filename, gcsFilename, cis);
            checkLineage(blob, messageDigest, inputStream, filename, fullSourcePath);
            LOG.info("[SFTP Delta] {} last modification time: {}", fullSourcePath, input.get(MTIME));
            setMetrics(blob, input, startTime);
            sendSplunkEvents(blob, input, startTime);
            LOG.info("[SFTP Delta] Transferred {} bytes: from {} to {}", input.get(SIZE), fullSourcePath, config.path);
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] File transfer error {}: {}", filename.substring(1), ex.getMessage());
        }
    }

    @Override
    public void destroy() {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * archive file in SFTP channel
     *
     * @param sourceFilePath    original sftp file to archive
     * @param targetFilePath    target path to archive file
     */
    private void archiveSftpFile(String sourceFilePath, String targetFilePath) {
        try {
            if(sourceFilePath.equals(targetFilePath)) {
                throw new Exception("Origin and target directories are equal.");
            }
            sftpChannel.rename(sourceFilePath,targetFilePath);
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
    private void deleteSftpFile(String sourceFilePath) {
        try {
            sftpChannel.rm(sourceFilePath);
            LOG.debug("[SFTP Delta] File {} has been deleted.", sourceFilePath);
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error deleting file {} from sftp source directory. Error: {}", sourceFilePath, ex.getMessage());
        }
    }

    /**
     *
     * @param context   batch sink context
     * @param fields    fields
     */
    private void emitLineage(BatchSinkContext context, List<Schema.Field> fields) {
        LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);

        if (!fields.isEmpty()) {
            lineageRecorder.recordWrite("Write", "Wrote to DB table.",
                    fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
        }
    }

    /**
     * md5 conversor
     * @param md    message digest value
     * @return
     */
    private String md5(MessageDigest md) {
        byte[] raw = md.digest();
        BigInteger bigInt = new BigInteger(1, raw);
        StringBuilder hash = new StringBuilder(bigInt.toString(16));
        while (hash.length() < 32) {
            hash.insert(0, '0');
        }

        return hash.toString();
    }

    /**
     *
     * @param config        SFTPtoGCSConfig configuration
     * @param filename      file name without complete path
     * @param gcsFilename   gcs file name with complete path
     * @param cis           input stream
     * @return
     * @throws Exception
     */
    private Blob retryPolicyStorage(SFTPtoGCSConfig config, String filename, String gcsFilename, DigestInputStream cis) throws Exception{
        Blob blob;

        BlobId blobId = BlobId.of(gcsPath.getBucket(), gcsFilename);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();
        
        RetryOnException retryHandler = new RetryOnException(config.numRetries, config.timeToWait);
        int retries = 0;
        while(true) {
            try {
                blob = storage.createFrom(blobInfo, cis, bufSize);
                break;
            } catch (Exception ex) {
                retryHandler.exceptionOccurred(filename);
                retries++;
                continue;
            }
        }
        fileRetries = retries;

        return blob;
    }

    private void checkLineage(Blob blob, MessageDigest messageDigest, InputStream inputStream, String filename, String fullSourcePath)
            throws Exception {
        String SFTPMD5 = md5(messageDigest);
        String gcsMD5 = blob != null ? blob.getMd5ToHexString() : null;

        if (!Objects.equals(SFTPMD5, gcsMD5)) {
            LOG.error("[SFTP Delta] MD5 does not match origin-target {}: {} <-> {} ", filename, SFTPMD5, gcsMD5);
            inputStream.close();
            throw new IOException("Check MD5 Error");
        }

        if(config.archiveOriginals.equals(YES)) {
            if (config.archiveOption.equals(RENAME)) {
                LOG.debug("[SFTP Delta] Archiving file: " + filename);
                archiveSftpFile(fullSourcePath, config.targetPath.concat(filename));
            } else if (config.archiveOption.equals(REMOVE)) {
                LOG.debug("[SFTP Delta] Removing file: " + filename);
                deleteSftpFile(fullSourcePath);
            }
        }
    }

    /**
     * Process metrics
     *
     * @param blob          gcs file
     * @param inputFile     structured record
     * @param startTime     processing start time
     */
    private void setMetrics(Blob blob, StructuredRecord inputFile, Long startTime){
        long endTime = System.currentTimeMillis();
        String metricBase = METRICS_SEPARATOR
                + pipelineName + METRICS_SEPARATOR
                + runId + METRICS_SEPARATOR
                + inputFile.get("basename") + METRICS_SEPARATOR;

        metrics.gauge(metricBase + "created_on", ((Integer)inputFile.get("mtime")).longValue());
        metrics.gauge(metricBase + METRICS_SIZE_BYTES, Math.toIntExact(blob.getSize()));
        metrics.gauge(metricBase + METRICS_SEEN_ON, Long.parseLong(seenOn));
        metrics.gauge(metricBase + METRICS_TRANSFERED_ON, endTime);
        metrics.gauge(metricBase + METRICS_TRANSFER_RETRIES, fileRetries);
        metrics.gauge(metricBase + METRICS_TRANSFER_TIME, Math.toIntExact(endTime - startTime));
        metrics.count(metricBase + METRICS_UPLOADED_FILES, 1);

        LOG.info("[SFTP Delta] Processed file {} -> init {} - finish {}", inputFile.get("basename"), startTime, endTime);
    }

    /**
     * Splunk events
     *
     * @param blob          gcs file
     * @param inputFile     structured record
     * @param startTime     processing start time
     */
    private void sendSplunkEvents(Blob blob, StructuredRecord inputFile, Long startTime){
        /*
        LOG.info("[SFTP Delta] Sending metrics to Splunk for file {}.", (String) inputFile.get("basename"));
        long endTime = System.currentTimeMillis();
        String metricBase = pipelineName + METRICS_SEPARATOR
                + runId + METRICS_SEPARATOR
                + inputFile.get("basename") + METRICS_SEPARATOR;
        LOG.debug("1");
        try{
            Map<String, Object> connArgs = new HashMap<String, Object>();
            connArgs.put("host", "localhost");
            connArgs.put("username", "root");
            connArgs.put("password", "12345678");
            connArgs.put("port", 8089);
            connArgs.put("schema", "https");
            HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
            Service splunkService = Service.connect(connArgs);
            Receiver receiver = splunkService.getReceiver();

            //Set the sourcetype
            Args logArgs = new Args();
            logArgs.put("sourcetype", "sftpdelta");
            receiver.log("main", metricBase + METRICS_CREATED_ON + METRICS_SEPARATOR + inputFile.get("mtime"));
            receiver.log("main", metricBase + METRICS_SIZE_BYTES + METRICS_SEPARATOR + Math.toIntExact(blob.getSize()));
            receiver.log("main", metricBase + METRICS_SEEN_ON + METRICS_SEPARATOR + Long.parseLong(seenOn));
            receiver.log("main", metricBase + METRICS_TRANSFERED_ON + METRICS_SEPARATOR + endTime);
            receiver.log("main", metricBase + METRICS_TRANSFER_RETRIES + METRICS_SEPARATOR + fileRetries);
            receiver.log("main", metricBase + METRICS_TRANSFER_TIME + METRICS_SEPARATOR + Math.toIntExact(endTime - startTime));

            LOG.info("[SFTP Delta] Sent metrics to Splunk for file {}.", (String) inputFile.get("basename"));
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Sending metrics to Splunk error: {}.", ex.getMessage());
        }
        */
    }
}
