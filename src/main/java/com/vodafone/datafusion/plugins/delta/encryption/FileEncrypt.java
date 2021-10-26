package com.vodafone.datafusion.plugins.delta.encryption;

import com.google.cloud.storage.Blob;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import com.vodafone.datafusion.plugins.delta.common.FileMetaData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.operator.KeyFingerPrintCalculator;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.bc.BcPublicKeyDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyKeyEncryptionMethodGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Iterator;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.MD5;

public class FileEncrypt {
    private static final Logger LOG = LoggerFactory.getLogger(FileEncrypt.class);

    static Configuration conf;
    static PGPPrivateKey sKey = null;
    static ChannelSftp channelSftp = null;

    static {
        conf = new Configuration();

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    public static InputStream encryptPipeFile(ChannelSftp sftpChannel, FileMetaData fileMetaData, PGPPublicKey encKey, Integer bufferSize) throws IOException {
        PipedOutputStream outPipe = new PipedOutputStream();
        PipedInputStream inPipe = new PipedInputStream();
        inPipe.connect(outPipe);
        channelSftp = sftpChannel;

        new Thread(
                () -> {
                    try {
                        encryptFile(outPipe, fileMetaData, encKey, bufferSize, false, true);
                    } catch (IOException e) {
                        LOG.error("[SFTP Delta] Encryption IO error: " + e.getMessage());
                    } catch (NoSuchProviderException e) {
                        LOG.error("[SFTP Delta] Encryption Provider error: " + e.getMessage());
                    } finally {
                        try {
                            outPipe.close();
                        } catch (IOException e) {
                            LOG.error("[SFTP Delta] Encryption close error: " + e.getMessage());
                        }
                    }
                })
                .start();
        return inPipe;
    }

    public static void decryptFileIS(ChannelSftp sftpChannel, Blob blob, String privateKeyPath, char[] passwd, String filename) throws Exception {
        final KeyFingerPrintCalculator FP_CALC = new BcKeyFingerprintCalculator();
        MessageDigest messageDigest = MessageDigest.getInstance(MD5);
        InputStream inputStream = new DigestInputStream(new ByteArrayInputStream(blob.getContent()), messageDigest);
        inputStream = PGPUtil.getDecoderStream(inputStream);
        InputStream unc = null;
        InputStream clear = null;
        InputStream keyIn = null;
        try {
            PGPObjectFactory pgpF = new PGPObjectFactory(inputStream, FP_CALC);
            PGPEncryptedDataList enc;
            Object inObj = pgpF.nextObject();

            // the first object might be a PGP marker packet.
            if (inObj instanceof PGPEncryptedDataList) {
                enc = (PGPEncryptedDataList) inObj;
            } else {
                enc = (PGPEncryptedDataList) pgpF.nextObject();
            }

            // find the secret key
            Iterator it = enc.getEncryptedDataObjects();

            PGPPublicKeyEncryptedData pbe = null;

            if (null != privateKeyPath && !privateKeyPath.isEmpty()) {
                try {
                    keyIn = sftpChannel.get(privateKeyPath);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Error reading private key.");
                }
            }
            while (it.hasNext()) {
                pbe = (PGPPublicKeyEncryptedData) it.next();
                LOG.debug("[SFTP Delta] Private Key id=" + pbe.getKeyID());
                sKey = findSecretKey(keyIn, pbe.getKeyID(), passwd);
            }
            keyIn.close();

            if (sKey == null) {
                LOG.error("[SFTP Delta] Secret key not found.");
                throw new IllegalArgumentException("[SFTP Delta] Secret key not found.");
            }

            clear = pbe.getDataStream(new BcPublicKeyDataDecryptorFactory(sKey));
            PGPObjectFactory plainFact = new PGPObjectFactory(clear, FP_CALC);
            Object message = plainFact.nextObject();

            if (message instanceof PGPLiteralData) {
                PGPLiteralData ld = (PGPLiteralData) message;
                unc = ld.getInputStream();
                sftpChannel.put(unc, filename);
            } else if (message instanceof PGPOnePassSignatureList) {
                throw new PGPException("[SFTP Delta] Encrypted file contains a signed message - not literal data.");
            } else {
                throw new PGPException("[SFTP Delta] File content is not a simple encrypted file - type unknown.");
            }

            if (pbe.isIntegrityProtected()) {
                if (!pbe.verify()) {
                    LOG.debug("[SFTP Delta] failed integrity check");
                } else {
                    LOG.debug("[SFTP Delta] integrity check passed");
                }
            } else {
                LOG.debug("[SFTP Delta] no integrity check");
            }
        } catch (Exception exception) {
            LOG.error("[SFTP Delta] Decrypt error: " + exception.getMessage());
            throw exception;
        } finally {
            if(null != keyIn) keyIn.close();
            if(null != inputStream) inputStream.close();
            if(null != clear) clear.close();
            if(null != unc) unc.close();
        }
    }

    private static void encryptFile(OutputStream out, FileMetaData fileMetaData, PGPPublicKey encKey, Integer bufferSize, boolean armor, boolean withIntegrityCheck) throws IOException, NoSuchProviderException {
        if (armor) {
            out = new ArmoredOutputStream(out);
        }

        try {
            PGPEncryptedDataGenerator cPk = new PGPEncryptedDataGenerator(new JcePGPDataEncryptorBuilder(PGPEncryptedData.CAST5)
                    .setWithIntegrityPacket(withIntegrityCheck).setSecureRandom(new SecureRandom()).setProvider(new BouncyCastleProvider()));

            cPk.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(encKey).setProvider(new BouncyCastleProvider()));

            OutputStream cOut = cPk.open(out, new byte[bufferSize]);

            writeFileToLiteralData(cOut, PGPLiteralData.BINARY, fileMetaData, new byte[bufferSize]);

            cOut.close();

            if (armor) {
                out.close();
            }
        } catch (PGPException e) {
            LOG.error("[SFTP Delta] Encryption error: " + e);
            if (e.getUnderlyingException() != null) {
                e.getUnderlyingException().printStackTrace();
            }
        } catch (SftpException e) {
            LOG.error("[SFTP Delta] Encryption Sftp error: " + e);
        }
    }

    private static void writeFileToLiteralData(OutputStream outStream, char literalDataType, FileMetaData fileMetaData, byte[] bytesArray) throws IOException, SftpException {
        PGPLiteralDataGenerator literalDataGenerator = new PGPLiteralDataGenerator();
        OutputStream outputStream = literalDataGenerator.open(outStream, literalDataType, fileMetaData.getPath().getName(), new Date(fileMetaData.getLastModifiedTime()), bytesArray);
        pipeFileContents(fileMetaData, outputStream, bytesArray.length);
    }

    private static void pipeFileContents(FileMetaData fileMetadata, OutputStream outputStream, int bytesArraySize) throws IOException, SftpException {
        InputStream inputStream;
        if (null != channelSftp) {
            inputStream = channelSftp.get(fileMetadata.getPath().toString());
        } else {
            inputStream = fileMetadata.getFileSystem().open(fileMetadata.getPath());
        }
        byte[] bytes = new byte[bytesArraySize];

        int bufferBytes;
        while ((bufferBytes = inputStream.read(bytes)) > 0) {
            outputStream.write(bytes, 0, bufferBytes);
        }

        outputStream.close();
        inputStream.close();
    }

    private static PGPPrivateKey findSecretKey(InputStream keyIn, long keyID, char[] pass)
            throws PGPException {
        PGPPrivateKey privateKey;
        try {
            PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(keyIn), new BcKeyFingerprintCalculator());
            PGPSecretKey pgpSecKey = pgpSec.getSecretKey(keyID);
            if (pgpSecKey == null) {
                return null;
            }
            PBESecretKeyDecryptor secretKeyDecryptor = new JcePBESecretKeyDecryptorBuilder()
                    .setProvider(new BouncyCastleProvider()).build(pass);

            privateKey = pgpSecKey.extractPrivateKey(secretKeyDecryptor);
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Private Key could not be read.");
            throw new PGPException("Private Key could not be read");
        }
        return privateKey;
    }
}
