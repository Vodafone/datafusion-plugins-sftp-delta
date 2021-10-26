package com.vodafone.datafusion.plugins.delta.gsdelta.source;

import com.vodafone.datafusion.plugins.delta.common.GCSPath;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.SLASH;

public class GSDeltaRecord {
    public String connectionType;
    public String connection;
    public String fullfilename;
    public String filename;
    public String basename;
    public Long size;
    public Integer mTime;

    public GSDeltaRecord(String connectionType,
                         String connection,
                         String path,
                         String filename,
                         Long size,
                         Integer mTime) {
        this.connectionType = connectionType;
        this.connection = connection;
        int i = path.length();
        this.basename = path;
        this.filename = getFilename(filename);
        this.fullfilename = getFullFilename(path, filename);
        this.size = size;
        this.mTime = mTime;
    }

    /**
     * Extract the basename from absolute path
     *
     * @param path absolute path
     * @return basename
     */
    public static String basename(String path) {
        return path.substring(0, path.lastIndexOf('/') + 1);
    }

    private String getFilename(String blobName) {
        String filename;
        filename = blobName.substring(blobName.lastIndexOf('/'));
        if (filename.startsWith("/")){
            filename = filename.substring(1);
        }
        return filename;
    }

    private String getFullFilename(String path, String filename) {
        GCSPath gcsPath = GCSPath.from(path);
        String fullFilename = SLASH + gcsPath.getBucket() + SLASH + filename;

        return fullFilename;
    }
}
