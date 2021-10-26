package com.vodafone.datafusion.plugins.delta.gsdelta.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;
import static com.vodafone.datafusion.plugins.delta.constants.Constants.TIME_TO_WAIT;

public class GCStoSFTPConfig extends PluginConfig {

    @Description(REFERENCENAME)
    @Macro
    public final String referenceName;

    @Macro
    @Description("SFTP server")
    @Name(SFTP_SERVER)
    public final String sftpServer;

    @Macro
    @Description("SFTP Server Port")
    @Name(SFTP_PORT)
    public final Integer sftpPort;

    @Macro
    @Description("SFTP directory")
    @Name(SFTP_PATH)
    public final String sftpPath;

    @Macro
    @Description("SFTP user")
    @Name(SFTP_USER)
    public final String sftpUser;

    @Name(AUTHENTICATION)
    @Description("Authentication type to be used for connection")
    @Nullable
    @Macro
    public final String authType;

    @Description("SFTP password")
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

    @Description("GCS target path")
    @Macro
    @Nullable
    public final String targetPath;

    @Description("Archive / Remove original GCS files")
    @Macro
    @Nullable
    public final String archiveOriginals;

    @Nullable
    @Macro
    public final String archiveOption;

    @Description("Number of retries in error case.\nNo over 100 allowed.")
    @Macro
    @Nullable
    public final String numRetries;

    @Description("Seconds between retries.\nNo over 300 allowed.")
    @Macro
    @Nullable
    public final String timeToWait;

    @Description("Properties that will be used to configure the SSH connection to the FTP server. " +
            "For example to enable verbose logging add property 'LogLevel' with value 'VERBOSE'. " +
            "To enable host key checking set 'StrictHostKeyChecking' to 'yes'. " +
            "SSH can be configured with the properties described here 'https://linux.die.net/man/5/ssh_config'.")
    @Nullable
    @Name(SSH_PROPERTIES)
    @Macro
    public String sshProperties;

    @Name(PROXY_IP)
    @Description("proxyIP")
    @Macro
    @Nullable
    public final String proxyIP;

    @Name(PROXY_PORT)
    @Description("Proxy port")
    @Macro
    @Nullable
    public final Integer proxyPort;

    @Name(ENCRYPTION_PRIVATE_KEY_FILE_PATH)
    @Description("Private Key Path")
    @Macro
    @Nullable
    public final String privateKeyPath;

    @Name(ENCRYPTION_PRIVATE_KEY_PASSWORD)
    @Description("Private Key Password")
    @Macro
    @Nullable
    public final String privateKeyPassword;

    /**
     *
     * @param referenceName
     * @param sftpServer
     * @param sftpPort
     * @param sftpPath
     * @param sftpUser
     * @param sftpPass
     * @param authType
     * @param privateKey
     * @param passphrase
     * @param targetPath
     * @param archiveOriginals
     * @param archiveOption
     * @param numRetries
     * @param timeToWait
     * @param proxyIP
     * @param proxyPort
     * @param privateKeyPath
     * @param privateKeyPassword

     */
    public GCStoSFTPConfig(
            String referenceName,
            String sftpServer,
            Integer sftpPort,
            String sftpPath,
            String sftpUser,
            @Nullable String sftpPass,
            String authType,
            @Nullable String privateKey,
            @Nullable String passphrase,
            String targetPath,
            @Nullable String archiveOriginals,
            @Nullable String archiveOption,
            @Nullable String numRetries,
            @Nullable String timeToWait,
            @Nullable String proxyIP,
            @Nullable Integer proxyPort,
            @Nullable String privateKeyPath,
            @Nullable String privateKeyPassword
    ) {
        this.referenceName = referenceName;
        this.sftpServer = sftpServer;
        this.sftpPort = sftpPort;
        this.sftpPath = sftpPath;
        this.sftpUser = sftpUser;
        this.sftpPass = sftpPass;
        this.authType = authType;
        this.privateKey = privateKey;
        this.passphrase = passphrase;
        this.targetPath = targetPath;
        this.archiveOriginals = archiveOriginals;
        this.archiveOption = archiveOption;
        this.numRetries = numRetries;
        this.timeToWait = timeToWait;
        this.proxyIP=proxyIP;
        this.proxyPort=proxyPort;
        this.privateKeyPath=privateKeyPath;
        this.privateKeyPassword=privateKeyPassword;
    }

    /**
     * Config validation
     *
     * @param collector Object containing validation failures
     */
    public void validate(FailureCollector collector) {
        if (!Strings.isNullOrEmpty(authType) && authType.equals(CONN_PRIVATEKEY_SELECT)) {
            if (Strings.isNullOrEmpty(privateKey)) {
                collector.addFailure("Invalid PrivateKey.", "Ensure the value.")
                        .withConfigProperty(SFTP_PRIVATEKEY);
            }
        } else {
            if (EMPTY_STRING.equals(sftpPass)) {
                collector.addFailure("Invalid Password.", "Ensure the value.")
                        .withConfigProperty(SFTP_PASS);
            }
        }

        try {
            Integer.valueOf(sftpPort);
        } catch (NumberFormatException e) {
            collector.addFailure("Invalid The Port Server.", "Ensure the value.")
                    .withConfigProperty(SFTP_PORT);
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
}
