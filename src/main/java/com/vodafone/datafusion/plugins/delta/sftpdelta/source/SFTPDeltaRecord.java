package com.vodafone.datafusion.plugins.delta.sftpdelta.source;

public class SFTPDeltaRecord {
    public String connection;
    public String fullfilename;
    public String filename;
    public String basename;
    public Long size;
    public Integer mTime;

    public SFTPDeltaRecord(String connection,
                           String prefix,
                           String path,
                           String filename,
                           Long size,
                           Integer mTime) {
        this.connection = connection;
        int i = path.length();
        this.basename = basename(filename);
        this.fullfilename = prefix + filename.substring(i);
        this.filename = filename.substring(i);
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
}
