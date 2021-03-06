package com.vodafone.datafusion.plugins.delta.common;

import com.google.common.net.UrlEscapers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;

/**
 * A path on GCS. Contains information about the bucket and blob name (if applicable).
 * A path is of the form gs://bucket/name.
 */
public class GCSPath {
    private static final Logger LOG = LoggerFactory.getLogger(GCSPath.class);

    private final URI uri;
    private final String bucket;
    private final String name;

    private GCSPath(URI uri, String bucket, String name) {
        this.uri = uri;
        this.bucket = bucket;
        this.name = name;
    }

    public URI getUri() {
        return uri;
    }

    public String getBucket() {
        return bucket;
    }

    /**
     * @return the object name. This will be an empty string if the path represents a bucket.
     */
    public String getName() {
        return name;
    }

    boolean isBucket() {
        return name.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GCSPath gcsPath = (GCSPath) o;
        return Objects.equals(uri, gcsPath.uri) &&
                Objects.equals(bucket, gcsPath.bucket) &&
                Objects.equals(name, gcsPath.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, bucket, name);
    }

    /**
     * Parse the given path string into a GCSPath. Paths are expected to be of the form
     * gs://bucket/dir0/dir1/file, or bucket/dir0/dir1/file.
     *
     * @param path the path string to parse
     * @return the GCSPath for the given string.
     * @throws IllegalArgumentException if the path string is invalid
     */
    public static GCSPath from(String path) {
        if (path.isEmpty()) {
            LOG.error("[SFTP Delta] GCS path can not be empty. The path must be similar to 'gs://<bucket-name>/path'.");
            throw new IllegalArgumentException("GCS path can not be empty. The path must be similar to 'gs://<bucket-name>/path'.");
        }

        if (path.startsWith(SLASH)) {
            path = path.substring(1);
        } else if (path.startsWith(GS_ROOT)) {
            path = path.substring(GS_ROOT.length());
        }

        String bucket = path;
        int idx = path.indexOf(SLASH);
        // if the path within bucket is provided, then only get the bucket
        if (idx > 0) {
            bucket = path.substring(0, idx);
        }

        if (!Pattern.matches("[a-z0-9-_.]+", bucket)) {
            LOG.error("[SFTP Delta] Invalid bucket name:" + path);
            throw new IllegalArgumentException(
                    String.format("Invalid bucket name in path '%s'. Bucket name should only contain lower case alphanumeric, " +
                            "'-'. '_' and '.'. Please follow GCS naming convention: " +
                            "https://cloud.google.com/storage/docs/naming-buckets", path));
        }

        String file = idx > 0 ? path.substring(idx).replaceAll("^/", "") : "";
        URI uri = URI.create(GS_ROOT + bucket + SLASH + UrlEscapers.urlFragmentEscaper().escape(file));
        return new GCSPath(uri, bucket, file);
    }

    public static String getBucketName(String gcpPath) {
        GCSPath gcsPath = GCSPath.from(gcpPath);
        return gcsPath.getBucket();
    }

    public static String getObjectName(String gcpPath) {
        GCSPath gcsPath = GCSPath.from(gcpPath);
        return gcsPath.getName();
    }
}
