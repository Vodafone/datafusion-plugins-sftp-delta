package com.vodafone.datafusion.plugins.delta.sftpdelta.source;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import org.apache.commons.validator.routines.UrlValidator;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;

/**
 * Configurations for the SFTPDelta source plugin.
 */
public class SFTPDeltaSourceConfig extends PluginConfig {
    
    @Macro
    @Description("SFTP server.")
    @Name(SFTP_SERVER)
    public final String sftpServer;

    @Macro
    @Description("SFTP Server Port.")
    @Name(SFTP_PORT)
    public final Integer sftpPort;

    @Macro
    @Description("SFTP directory.")
    @Name(SFTP_PATH)
    public final String sftpPath;

    @Macro
    @Description("Recursive reading")
    @Nullable
    @Name(RECURSIVE)
    public final String recursive;

    @Macro
    @Description("Regex Path Filter. For Example: .*.parquet 'https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#sum'")
    @Nullable
    @Name(REGEX_FILTER)
    public final String regexFilter;

    @Macro
    @Description("The user of the SFTP.")
    @Name(SFTP_USER)
    public final String sftpUser;

    @Macro
    @Name(AUTHENTICATION)
    @Description("Authentication type to be used for connection")
    @Nullable
    public final String authType;

    @Description("SFTP pass")
    @Name(SFTP_PASS)
    @Nullable
    @Macro
    public final String sftpPass;

    @Description("Private Key to be used to login to SFTP Server. SSH key must be of RSA type")
    @Name(SFTP_PRIVATEKEY)
    @Macro
    @Nullable
    public final String privateKey; // ssh-keygen -p -f ~/.ssh/id_rsa -m pem

    @Description("Passphrase to be used with private key if passphrase was enabled when key was created. " +
            "If PrivateKey is selected for Authentication")
    @Name(SFTP_PRIVATEKEYPASS)
    @Macro
    @Nullable
    public final String passphrase;

    @Description("Location where the Last date processed will be persisted, " +
            "for example, hdfs://hadoop/process/ftp.date or gs://gcs/path/ftp.date")
    @Name(PERSISTDELTA)
    @Macro
    @Nullable
    public final String persistDelta;

    @Description("Date for files have to be processed, for example, 2020-01-24 23:00:00 UTC")
    @Name(FROMDELTA)
    @Macro
    @Nullable
    public final Long fromDelta;

    @Macro
    @Description("Reference name")
    @Name(REFERENCENAME)
    public final String referenceName;

    @Macro
    @Description("Properties that will be used to configure the SSH connection to the FTP server. " +
            "For example to enable verbose logging add property 'LogLevel' with value 'VERBOSE'. " +
            "To enable host key checking set 'StrictHostKeyChecking' to 'yes'. " +
            "SSH can be configured with the properties described here 'https://linux.die.net/man/5/ssh_config'.")
    @Nullable
    @Name(SSH_PROPERTIES)
    public String sshProperties;

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

    @Name(PROXY_IP)
    @Description("proxyIP")
    @Macro
    @Nullable
    public final String proxyIP;

    @Name(PROXY_PORT)
    @Description("proxyPort")
    @Macro
    @Nullable
    public final Integer proxyPort;


    public SFTPDeltaSourceConfig(String sftpServer,
                                 Integer sftpPort,
                                 String sftpPath,
                                 @Nullable String recursive,
                                 @Nullable String regexFilter,
                                 String sftpUser,
                                 @Nullable String sftpPass,
                                 String referenceName,
                                 String authType,
                                 @Nullable String privateKey,
                                 @Nullable String passphrase,
                                 @Nullable String persistDelta,
                                 String serviceAccountType,
                                 @Nullable String serviceFilePath,
                                 @Nullable String serviceAccountJSON,
                                 @Nullable Long fromDelta,
                                 @Nullable String proxyIP,
                                 @Nullable Integer proxyPort) {

        this.sftpServer = sftpServer;
        this.sftpPort = sftpPort;
        this.sftpPath = sftpPath;
        this.recursive = recursive;
        this.regexFilter = regexFilter;
        this.sftpUser = sftpUser;
        this.authType = authType;
        this.sftpPass = sftpPass;
        this.privateKey = privateKey;
        this.passphrase = passphrase;
        this.referenceName = referenceName;
        this.persistDelta = persistDelta;
        this.serviceAccountType = serviceAccountType;
        this.serviceFilePath = serviceFilePath;
        this.serviceAccountJSON = serviceAccountJSON;
        this.fromDelta = fromDelta;
        this.proxyIP=proxyIP;
        this.proxyPort=proxyPort;
    }

    public String getFullPath() {
        return SFTP_ROOT + sftpUser + AT + sftpServer + COLON + sftpPort + sftpPath;
    }

    public byte[] getPrivateKey() {
        if (null != privateKey){
            return privateKey.getBytes(StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }

    public byte[] getPassphrase() {
        return Strings.isNullOrEmpty(passphrase) ? new byte[0] : passphrase.getBytes(StandardCharsets.UTF_8);
    }

    public void validate(FailureCollector collector) {
        try {
            Integer.valueOf(sftpPort);
        } catch (NumberFormatException e) {
            collector.addFailure("Invalid The Port Server.", "Ensure the value.")
                    .withConfigProperty(SFTP_PORT);
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
                collector.addFailure("URL no valid.", "Ensure the value file:// hdfs:// gs://")
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
