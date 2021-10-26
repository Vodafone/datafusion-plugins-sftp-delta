package com.vodafone.datafusion.plugins.delta.gsdelta.source;

import com.google.common.base.Strings;
import com.vodafone.datafusion.plugins.delta.common.GCSPath;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import org.apache.commons.validator.routines.UrlValidator;

import javax.annotation.Nullable;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;

public class GSDeltaSourceConfig  extends PluginConfig {

    @Macro
    @Description("Reference name")
    @Name(REFERENCENAME)
    public final String referenceName;

    @Macro
    @Description("GCS path")
    @Name(GCS_PATH)
    public final String gcsPath;

    @Macro
    @Description("Regex Path Filter. For Example: .*.parquet 'https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#sum'")
    @Nullable
    @Name(REGEX_FILTER)
    public final String regexFilter;

    @Macro
    @Description("Recursive reading")
    @Nullable
    @Name(RECURSIVE)
    public final String recursive;

    @Description("GCS location where the Last date processed will be persisted, " +
            "for example, gs://gcs/path/ftp.date")
    @Name(PERSISTDELTA)
    @Macro
    @Nullable
    public final String persistDelta;

    @Description("Date for files have to be processed, for example, 2020-01-24 23:00:00 UTC")
    @Name(FROMDELTA)
    @Macro
    @Nullable
    public final Long fromDelta;

    @Description("GCP Service Account Type")
    @Name(SATYPE)
    @Macro
    public final String serviceAccountType;

    @Description("File Path")
    @Name(SERVICE_FILE_PATH)
    @Macro
    @Nullable
    public final String serviceFilePath;

    @Description("JSON")
    @Name(SERVICE_ACCOUNT_JSON)
    @Macro
    @Nullable
    public final String serviceAccountJSON;

    public GSDeltaSourceConfig(String referenceName,
                               String gcsPath,
                               @Nullable String regexFilter,
                               @Nullable String recursive,
                               @Nullable String persistDelta,
                               @Nullable Long fromDelta,
                               String serviceAccountType,
                               @Nullable String serviceFilePath,
                               @Nullable String serviceAccountJSON) {

        this.referenceName = referenceName;
        this.gcsPath = gcsPath;
        this.regexFilter = regexFilter;
        this.recursive = recursive;
        this.persistDelta = persistDelta;
        this.fromDelta = fromDelta;
        this.serviceAccountType = serviceAccountType;
        this.serviceFilePath = serviceFilePath;
        this.serviceAccountJSON = serviceAccountJSON;
    }

    public String getFullPath() {
        String fullPath = gcsPath;
        if (fullPath.startsWith(GS_ROOT)) {
            fullPath = gcsPath.substring(GS_ROOT.length() - 1);
        }
        if (!fullPath.endsWith("/")) {
            fullPath = fullPath.concat("/");
        }
        return fullPath;
    }

    public void validate(FailureCollector collector) {
        try {
            if(!Strings.isNullOrEmpty(gcsPath) && !gcsPath.startsWith(MACRO)) {
                GCSPath.from(gcsPath);
                if(!gcsPath.startsWith(GS_ROOT)){
                    collector.addFailure("Invalid Path.", "GCS Paths should start with gs://. Ensure the value.")
                            .withConfigProperty(GCS_PATH);
                }
            }
        } catch (Exception e) {
            collector.addFailure("Invalid Path.", "Ensure the value.")
                    .withConfigProperty(GCS_PATH);
        }

        if (!Strings.isNullOrEmpty(regexFilter)) {
            try {
                Pattern p = Pattern.compile(regexFilter);
            } catch (PatternSyntaxException e) {
                collector.addFailure("Invalid Regex Path Filter: " + e.getMessage(), "Ensure the value.")
                        .withConfigProperty(REGEX_FILTER);
            }
        }

        if (!Strings.isNullOrEmpty(persistDelta)) {
            String[] schemes = {HDFS, FILE, GS};
            UrlValidator urlValidator = new UrlValidator(schemes, UrlValidator.ALLOW_LOCAL_URLS);
            if (!urlValidator.isValid(persistDelta)) {
                collector.addFailure("URL no valid.", "Ensure the value file:// gs://")
                        .withConfigProperty(PERSISTDELTA);
            }
        }

        if (!Strings.isNullOrEmpty(persistDelta) && persistDelta.startsWith(GS_ROOT) && serviceAccountType.equals(JSON)) {
            if (Strings.isNullOrEmpty(serviceAccountJSON)) {
                collector.addFailure("Invalid Service Account JSON.", "Ensure the value.")
                        .withConfigProperty(SERVICE_ACCOUNT_JSON);
            }
        }

        if (!Strings.isNullOrEmpty(persistDelta) && persistDelta.startsWith(GS_ROOT) && serviceAccountType.equals(FILEPATH)) {
            if (Strings.isNullOrEmpty(serviceFilePath)) {
                collector.addFailure("Invalid Service Account File Path.", "Ensure the value.")
                        .withConfigProperty(SERVICE_FILE_PATH);
            }
        }

    }

    @Override
    public String toString() {
        return getFullPath();
    }
}
