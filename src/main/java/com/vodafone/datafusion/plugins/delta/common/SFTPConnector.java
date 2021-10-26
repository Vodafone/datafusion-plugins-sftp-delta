package com.vodafone.datafusion.plugins.delta.common;

import com.google.common.base.Strings;
import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;

/**
 * Class to connect to SFTP server.
 */
public class SFTPConnector implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SFTPConnector.class);
    private final Session session;
    private final Channel channel;

    private String connID;

    /**
     * Informs if connection is done
     *
     * @param host      connection host
     * @param port      connection port
     * @param userName  connection user
     * @return
     */
    public boolean isConnected(String host, int port, String userName) {
        String cID = host + port + userName;

        return connID.equals(cID);
    }

    /**
     * Connector Object for password authentication
     *
     * @param host      connection host
     * @param port      connection port
     * @param userName  connection user
     * @param password  connection password
     * @param proxyIP   connection proxyIP
     * @param proxyPort connection proxyPort
     * @param sessionProperties session properties
     * @throws Exception
     */
    public SFTPConnector(String host, int port, String userName, String password, String proxyIP,Integer proxyPort,Map<String, String> sessionProperties)
            throws Exception {
        LOG.debug("[SFTP Delta] Connecting to SFTP server via password.");
        JSch jsch = new JSch();
        this.session = jsch.getSession(userName, host, port);
        session.setPassword(password);
        LOG.debug("[SFTP Delta] Connection properties: {}", sessionProperties);
        Properties properties = new Properties();
        properties.putAll(sessionProperties);
        session.setConfig(properties);
        if(!Strings.isNullOrEmpty(proxyIP) && proxyPort!=null){
            session.setProxy(new ProxyHTTP(proxyIP, proxyPort));
        }
        LOG.debug("[SFTP Delta] Connecting to Host: {}, Port: {}, with User: {}", host, port, userName);
        session.connect(TIMEOUT);
        channel = session.openChannel(CONN_SFTP);
        channel.connect();
        LOG.debug("[SFTP Delta] Connected.");
        connID = host + port + userName;
    }

    /**
     * Connection object for SSH PrivateKey authentication
     *
     * @param host          connection host
     * @param port          connection port
     * @param userName      connection user
     * @param privateKey    connection private key
     * @param proxyIP  connection proxyIP
     * @param proxyPort  connection proxyPort
     * @param passphrase    passphrase
     * @param sessionProperties session properties
     * @throws Exception
     */
    public SFTPConnector(String host, int port, String userName, byte[] privateKey,String proxyIP,Integer proxyPort,
                         byte[] passphrase, Map<String, String> sessionProperties) throws JSchException {
        LOG.debug("[SFTP Delta] Connecting to SFTP server private key.");
        JSch jsch = new JSch();
        jsch.addIdentity(KEY, privateKey, null, passphrase);
        this.session = jsch.getSession(userName, host, port);
        LOG.debug("[SFTP Delta] Connection properties: {}", sessionProperties);
        Properties properties = new Properties();
        properties.putAll(sessionProperties);
        session.setConfig(properties);
        if(!Strings.isNullOrEmpty(proxyIP) && proxyPort!=null){
            session.setProxy(new ProxyHTTP(proxyIP, proxyPort));
        }
        LOG.debug("[SFTP Delta] Connecting to Host: {}, Port: {}, with User: {}", host, port, userName);
        session.connect(TIMEOUT);
        channel = session.openChannel(CONN_SFTP);
        channel.connect();
        LOG.debug("[SFTP Delta] Connected.");
        connID = host + port + userName;
    }

    /**
     * Get the established sftp channel to perform operations.
     */
    public ChannelSftp getSftpChannel() {
        LOG.debug("[SFTP Delta] Getting SFTP channel.");
        return (ChannelSftp) channel;
    }

    @Override
    public void close() {
        if (channel != null) {
            try {
                channel.disconnect();
                LOG.debug("[SFTP Delta] Closed SFTP channel.");
            } catch (Throwable t) {
                LOG.warn("[SFTP Delta] Error disconnecting sftp channel.", t);
            }
        }

        if (session != null) {
            try {
                session.disconnect();
                LOG.debug("[SFTP Delta] Closed SFTP session.");
            } catch (Throwable t) {
                LOG.warn("[SFTP Delta] Error disconnecting sftp session.", t);
            }
        }
    }
}
