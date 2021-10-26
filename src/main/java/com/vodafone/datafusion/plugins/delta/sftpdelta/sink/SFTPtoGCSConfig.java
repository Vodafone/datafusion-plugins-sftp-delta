package com.vodafone.datafusion.plugins.delta.sftpdelta.sink;

import com.google.common.base.Strings;
import com.vodafone.datafusion.plugins.delta.common.GCSPath;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;
import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;


public class SFTPtoGCSConfig extends PluginConfig {

    @Description("GCS Path")
    @Macro
    @Nullable
    public final String path;

    @Description("SFTP target path")
    @Macro
    @Nullable
    public final String targetPath;

    @Description(REFERENCENAME)
    @Macro
    public final String referenceName;

    @Description(SERVICE_FILE_PATH)
    @Macro
    @Nullable
    public final String serviceFilePath;

    @Description(SERVICE_ACCOUNT_JSON)
    @Macro
    @Nullable
    public final String serviceAccountJSON;

    @Description(SATYPE)
    @Macro
    @Nullable
    public final String serviceAccountType;

    @Description("Archive / Remove original SFTP files")
    @Macro
    @Nullable
    public final String archiveOriginals;

    @Macro
    @Nullable
    public final String archiveOption;

    @Description("Number of retries in error case.\nNo over 100 allowed.")
    @Macro
    @Nullable
    public final String numRetries;

    @Description("Seconds between retries.\nNo over 300 allowed.")
    @Macro
    @Nullable
    public final String timeToWait;

    @Name(ENCRYPTION_PUBLIC_KEY_FILE_PATH)
    @Description("Public key path used for encryption.")
    @Macro
    @Nullable
    protected String publicKeyPath;

    @Name(NAME_ENCRYPTION)
    @Macro
    @Description("Specify the encryption algorithm. If None is selected then data is not encrypted.")
    protected String encryption = EncryptionType.PGP.getType();

    /**
     * SFTP to GCStorage configuration
     *
     * @param serviceAccountJSON
     * @param path
     * @param referenceName
     * @param serviceFilePath
     * @param serviceAccountType
     */
    public SFTPtoGCSConfig(
            @Nullable String serviceAccountJSON,
            String path,
            @Nullable String targetPath,
            @Nullable String archiveOriginals,
            @Nullable String archiveOption,
            String referenceName,
            @Nullable String serviceFilePath,
            @Nullable String serviceAccountType,
            @Nullable String numRetries,
            @Nullable String timeToWait,
            String encryption,
            @Nullable String publicKeyPath

    ) {
        this.serviceAccountJSON = serviceAccountJSON;
        this.path = path;
        this.targetPath = targetPath;
        this.archiveOriginals = archiveOriginals;
        this.archiveOption = archiveOption;
        this.referenceName = referenceName;
        this.serviceFilePath = serviceFilePath;
        this.serviceAccountType = serviceAccountType;
        this.numRetries = numRetries;
        this.timeToWait = timeToWait;
        this.encryption = encryption;
        this.publicKeyPath = publicKeyPath;
    }

    /**
     * Config validation
     *
     * @param collector Object containing validation failures
     */
    public void validate(FailureCollector collector) {
        try {
           if(!Strings.isNullOrEmpty(path) && !path.startsWith(MACRO)) {
               GCSPath.from(path);
           }
        } catch (Exception e) {
            collector.addFailure("Invalid Path.", "Ensure the value.")
                    .withConfigProperty(PATH);
        }

        if (!Strings.isNullOrEmpty(numRetries)){
            try{
                Integer.parseInt(numRetries);
            } catch (Exception ex){
                collector.addFailure("Invalid Num. retries value.", "Ensure the value.")
                        .withConfigProperty(NUM_RETRIES);
            }
            if (Integer.parseInt(numRetries) < ZERO || Integer.parseInt(numRetries) > MAX_RETRIES) {
                collector.addFailure("Invalid Num. retries value.", "Ensure the value.")
                        .withConfigProperty(NUM_RETRIES);
            }
        }

        if (!Strings.isNullOrEmpty(timeToWait)){
            try{
                Integer.parseInt(timeToWait);
            } catch (Exception ex){
                collector.addFailure("Invalid Time to wait value.", "Ensure the value.")
                        .withConfigProperty(TIME_TO_WAIT);
            }
            if (Integer.parseInt(timeToWait) < ZERO || Integer.parseInt(timeToWait) > MAX_WAIT_TIME) {
                collector.addFailure("Invalid Time to wait value.", "Ensure the value.")
                        .withConfigProperty(TIME_TO_WAIT);
            }
        }
    }

    private enum EncryptionType {
        PGP("PGP"),
        NONE("NONE");
        private String type;

        EncryptionType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }
}
