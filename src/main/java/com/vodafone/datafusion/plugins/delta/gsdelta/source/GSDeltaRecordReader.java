package com.vodafone.datafusion.plugins.delta.gsdelta.source;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.jcraft.jsch.SftpException;
import com.vodafone.datafusion.plugins.delta.common.DeltaUtils;
import com.vodafone.datafusion.plugins.delta.common.GCSPath;
import com.vodafone.datafusion.plugins.delta.common.source.DeltaDelta;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;
import static java.util.stream.Collectors.toList;


/**
 * An {@link RecordReader}
 */
public class GSDeltaRecordReader extends RecordReader<NullWritable, GSDeltaRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(GSDeltaRecordReader.class);
    private static final Gson gson = new GsonBuilder().create();

    private int total, current;
    private Long safetyReadTime;
    private Long toTime;
    private Long fromTime;

    private GSDeltaRecord data;

    private Iterator<GSDeltaRecord> records;

    private GSDeltaSourceConfig config;
    private String configJson;

    private Storage storage;
    private GoogleCredentials credentials;

    @Override
    public void initialize(InputSplit inputSplit,
                           TaskAttemptContext taskAttemptContext) throws IOException {
        LOG.debug("[SFTP Delta] Initializing record reader.");

        Configuration conf = taskAttemptContext.getConfiguration();
        configJson = conf.get(CONF_JSON_PACKAGE_KEY);

        config = gson.fromJson(configJson, GSDeltaSourceConfig.class);

        try {
            if(!config.gcsPath.startsWith(GS_ROOT)){
                LOG.error("[SFTP Delta] Invalid Path. GCS Paths should start with gs://");
                throw new Exception("Invalid Path. GCS Paths should start with gs://");
            }

            credentials = DeltaUtils
                    .getGCPCredentials(config.serviceAccountType, config.serviceAccountJSON, config.serviceFilePath);

            storage = StorageOptions.newBuilder()
                    .setCredentials(credentials)
                    .build().getService();

            safetyReadTime = DeltaUtils.getSafetyTime(taskAttemptContext);
            toTime = Long.parseLong(conf.get(CONF_LOGICAL_START_TIME)) / 1000 - safetyReadTime;

            fromTime = DeltaDelta.getFromTime(config);

            ArrayList<GSDeltaRecord> fileList;
            fileList = listDirectory(GCSPath.from(config.gcsPath), config.recursive);

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
    public GSDeltaRecord getCurrentValue() {
        return data;
    }

    @Override
    public float getProgress() {
        if (total == 0) return 1.0f;

        return current / total;
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * Validates if the modify date is between fromDelta to execution time
     *
     * @param blob GCS object
     * @return delta
     */
    private boolean isNewFile(Blob blob) {
        if(!blob.isDirectory()) {
            return blob.getCreateTime() / 1000 > fromTime.intValue() && blob.getCreateTime() / 1000 <= toTime;
        } else {
            return false;
        }
    }

    /**
     * If the filename is match or not with regexFilter
     *
     * @param   blob File from GCS
     * @return  match
     */
    private boolean isMatches(Blob blob) {
        String name = blob.getName().substring(blob.getName().lastIndexOf('/') + 1);
        if (Strings.isNullOrEmpty(config.regexFilter)){
            return true;
        } else {
            return name.matches(config.regexFilter);
        }
    }

    /**
     * listDirectory recursively list all files and subdirectories in a given directory.
     *
     * @param gcsPath        The origin directory
     * @throws SftpException If any SFTP errors occur
     */
    private ArrayList listDirectory(GCSPath gcsPath, String recursive) throws Exception{
        LOG.info("[SFTP Delta] Listing directory: " + config.gcsPath);

        DeltaUtils.checkGCSbucket(credentials, config.gcsPath);

        Page<Blob> blobs;
        String prefix;
        if (!gcsPath.getName().endsWith(SLASH)) {
            prefix = gcsPath.getName() + SLASH;
        } else {
            prefix = gcsPath.getName();
        }
        if(recursive.equals(YES)){
            blobs = storage.list(
                    gcsPath.getBucket(),
                    Storage.BlobListOption.prefix(prefix));
        }else{
            blobs = storage.list(
                    gcsPath.getBucket(),
                    Storage.BlobListOption.prefix(prefix),
                    Storage.BlobListOption.currentDirectory());
        }

        ArrayList<GSDeltaRecord> fileList = new ArrayList<>();

        for (Blob blob : blobs.iterateAll()) {
            if(isSubfolder(gcsPath.getName(), blob.getName())  && isNewFile(blob) && isMatches(blob)){
                String connection = null;
                if(config.serviceAccountType.equals("filePath")) {
                    connection = config.serviceFilePath;
                } else if (config.serviceAccountType.equals("json")) {
                    connection = config.serviceAccountJSON;
                }
                fileList.add(new GSDeltaRecord(
                        config.serviceAccountType,
                        connection,
                        config.getFullPath(),
                        blob.getName(),
                        blob.getSize(),
                        (int) (long) blob.getCreateTime()));
            }
        }

        LOG.info("[SFTP Delta] Number of files listed: " + fileList.size());
        List<String> stringList = fileList.stream().map(file -> file.filename).collect(toList());
        LOG.info("[SFTP Delta] Files to be transferred: " + stringList);

        return fileList;
    }

    private boolean isSubfolder(String basePath, String blobPath) {
        String path = blobPath.endsWith(SLASH)? blobPath.substring(0, blobPath.length()-1) : blobPath;
        boolean isSubfolder = !basePath.equals(path);

        return isSubfolder;
    }
}
