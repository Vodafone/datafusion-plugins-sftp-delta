package com.vodafone.datafusion.plugins.delta.common;

import com.google.common.base.Strings;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.plugin.common.KeyValueListParser;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;

public class SFTPConnectorConfig {

    public String sftpServer;
    public Integer sftpPort;
    public String sftpPath;
    public String sftpUser;
    public String sftpPass;
    public String authType;
    public String privateKey;
    public String passphrase;
    public String sshProperties;
    public String proxyIP;
    public Integer proxyPort;

    /**
     * Configuration values for sftp connection
     *
     * @param sftpServer
     * @param sftpPort
     * @param sftpPath
     * @param sftpUser
     * @param sftpPass
     * @param proxyIP
     * @param proxyPort
     * @param authType
     * @param privateKey
     * @param passphrase
     * @param sshProperties
     */
    public SFTPConnectorConfig(String sftpServer,
                               Integer sftpPort,
                               String sftpPath,
                               String sftpUser,
                               String sftpPass,
                               String proxyIP,
                               Integer proxyPort,
                               String authType,
                               String privateKey,
                               String passphrase,
                               String sshProperties
    ) {
        this.sftpServer = sftpServer;
        this.sftpPort = sftpPort;
        this.sftpPath = sftpPath;
        this.sftpUser = sftpUser;
        this.sftpPass = sftpPass;
        this.proxyIP = proxyIP;
        this.proxyPort = proxyPort;
        this.authType = authType;
        this.privateKey = privateKey;
        this.passphrase = passphrase;
        this.sshProperties = sshProperties;
    }

    /**
     * Private Key
     *
     * @return
     */
    public byte[] getPrivateKey() {
        return privateKey.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Passphrase
     *
     * @return
     */
    public byte[] getPassphrase() {
        return Strings.isNullOrEmpty(passphrase) ? new byte[0] : passphrase.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * SSH properties
     *
     * @return Key/Value object with ssh properties
     */
    public Map<String, String> getSSHProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("StrictHostKeyChecking", NO);
        if (sshProperties == null || sshProperties.isEmpty()) {
            return properties;
        }

        KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
        for ( KeyValue<String, String> keyVal : kvParser.parse(sshProperties) ) {
            String key = keyVal.getKey();
            String val = keyVal.getValue();
            properties.put(key, val);
        }
        return properties;
    }
}
