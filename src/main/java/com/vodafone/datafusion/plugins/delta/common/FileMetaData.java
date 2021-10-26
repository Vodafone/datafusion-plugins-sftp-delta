package com.vodafone.datafusion.plugins.delta.common;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Vector;


public class FileMetaData {
    private static final Logger LOG = LoggerFactory.getLogger(FileMetaData.class);
    Path path;
    FileSystem fileSystem;
    ChannelSftp channelSftp;
    String filePath;

    private FileMetaData() {
    }

    public FileMetaData(ChannelSftp channelSftp, String filePath, Configuration conf) {
        this.filePath = filePath;
        this.channelSftp = channelSftp;
        if (null != filePath && filePath.startsWith("hdfs")) {
            LOG.debug("FileMetaData::hdfs:: {}", filePath);
            this.path = new Path(filePath);
            LOG.debug("FileMetaData::hdfs:: path.toString() = {}", getPath().toString());
            try {
                this.fileSystem = FileSystem.get(URI.create(filePath), conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            String strPath = filePath;
            LOG.debug("FileMetaData::else:: {}", strPath);
            this.path = new Path(strPath);
            LOG.debug("FileMetaData::else:: path.toString() = {}", getPath().toString());
            try {
                this.fileSystem = path.getFileSystem(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Path getPath() {
        return path;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public long getLastModifiedTime() {
        try {
            if(null != channelSftp){
                Vector<ChannelSftp.LsEntry> files = channelSftp.ls(filePath);
                return files.get(0).getAttrs().getMTime();
            } else {
                return fileSystem.getFileStatus(new Path(filePath)).getModificationTime();
            }
        } catch (IOException | SftpException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
