package com.vodafone.datafusion.plugins.delta.encryption;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import org.apache.log4j.helpers.LogLog;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.NoSuchProviderException;
import java.util.Iterator;


public class PGPCertUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PGPCertUtil.class);

    public static PGPPublicKey readPublicKey(String fileName, ChannelSftp sftpChannel) throws IOException, PGPException, SftpException {
        InputStream keyIn = new BufferedInputStream(sftpChannel.get(fileName));
        PGPPublicKey pubKey = readPublicKey(keyIn);
        keyIn.close();
        return pubKey;
    }

    /**
     * A simple routine that opens a key ring file and loads the first available key
     * suitable for encryption.
     *
     * @param input data stream containing the public key data
     * @return the first public key found.
     * @throws IOException
     * @throws PGPException
     */
    static PGPPublicKey readPublicKey(InputStream input) throws IOException, PGPException {
        PGPPublicKeyRingCollection pgpPub = new PGPPublicKeyRingCollection(
                PGPUtil.getDecoderStream(input), new JcaKeyFingerprintCalculator());

        Iterator keyRingIter = pgpPub.getKeyRings();
        while (keyRingIter.hasNext()) {
            PGPPublicKeyRing keyRing = (PGPPublicKeyRing) keyRingIter.next();

            Iterator keyIter = keyRing.getPublicKeys();
            while (keyIter.hasNext()) {
                PGPPublicKey key = (PGPPublicKey) keyIter.next();

                if (key.isEncryptionKey()) {
                    return key;
                }
            }
        }
        LOG.error("[SFTP Delta] Can't find encryption key in key ring.");
        throw new IllegalArgumentException("Can't find encryption key in key ring.");
    }

    /**
     * Search a secret key ring collection for a secret key corresponding to keyID if it
     * exists.
     *
     * @param pgpSec a secret key ring collection.
     * @param keyID  keyID we want.
     * @param pass   passphrase to decrypt secret key with.
     * @return the private key.
     * @throws PGPException
     * @throws NoSuchProviderException
     */
    static PGPPrivateKey findSecretKey(PGPSecretKeyRingCollection pgpSec, long keyID, char[] pass)
            throws PGPException, NoSuchProviderException {
        PGPSecretKey pgpSecKey = pgpSec.getSecretKey(keyID);

        if (pgpSecKey == null) {
            return null;
        }
        return pgpSecKey.extractPrivateKey(new JcePBESecretKeyDecryptorBuilder().setProvider("BC").build(pass));
    }

    public static PGPSecretKey readSecretKey(String fileName) throws IOException, PGPException, IllegalArgumentException {
        InputStream keyIn = new BufferedInputStream(new FileInputStream(fileName));
        PGPSecretKey secKey = readSecretKey(keyIn);
        keyIn.close();
        return secKey;
    }

    /**
     * A simple routine that opens a key ring file and loads the first available key
     * suitable for signature generation.
     *
     * @param input stream to read the secret key ring collection from.
     * @return a secret key.
     * @throws IOException  on a problem with using the input stream.
     * @throws PGPException if there is an issue parsing the input stream.
     */
    static PGPSecretKey readSecretKey(InputStream input) throws IOException, PGPException, IllegalArgumentException {
        PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(
                PGPUtil.getDecoderStream(input), new JcaKeyFingerprintCalculator());

        Iterator keyRingIter = pgpSec.getKeyRings();
        while (keyRingIter.hasNext()) {
            PGPSecretKeyRing keyRing = (PGPSecretKeyRing) keyRingIter.next();

            Iterator keyIter = keyRing.getSecretKeys();
            while (keyIter.hasNext()) {
                PGPSecretKey key = (PGPSecretKey) keyIter.next();

                if (key.isSigningKey()) {
                    return key;
                }
            }
        }
        throw new IllegalArgumentException("Can't find signing key in key ring.");
    }
}
