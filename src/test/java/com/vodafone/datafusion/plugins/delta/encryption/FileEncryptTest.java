package com.vodafone.datafusion.plugins.delta.encryption;

import com.jcraft.jsch.ChannelSftp;
import com.vodafone.datafusion.plugins.delta.common.FileMetaData;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.operator.KeyFingerPrintCalculator;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.bc.BcPublicKeyDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.junit.Test;

import java.io.*;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileEncryptTest {

    static Configuration conf;
    static {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    @Test
    public void encryptPipeFile() throws Exception {
        String publicKeyFile = "src/test/java/com/vodafone/datafusion/plugins/delta/data/PGP1D0.pkr";
        String fullSourcePath = "src/test/java/com/vodafone/datafusion/plugins/delta/data/in/test1.csv";


        PGPPublicKey encKey = PGPCertUtil.readPublicKey(new FileInputStream(publicKeyFile));

        int bufferSize = 15 * 1024 * 1024;
        ChannelSftp channelSftp = null;
        FileMetaData fileMetaData = new FileMetaData(channelSftp, fullSourcePath, conf);

        assertEquals(FileEncrypt.encryptPipeFile(channelSftp, fileMetaData, encKey, bufferSize).getClass(),
                java.io.PipedInputStream.class);
    }

    @Test
    public void decryptFile() throws Exception {
        final KeyFingerPrintCalculator FP_CALC = new BcKeyFingerprintCalculator();
        String privateKeyFile = "src/test/java/com/vodafone/datafusion/plugins/delta/data/PGP1D0.skr";
        String encryptedFile = "src/test/java/com/vodafone/datafusion/plugins/delta/data/encrypted/test1.csv.pgp";
        String filename = "src/test/java/com/vodafone/datafusion/plugins/delta/data/out/test1.csv";
        String passwd = "12345";

        InputStream keyIn = new BufferedInputStream(new FileInputStream(privateKeyFile));
        InputStream in = new FileInputStream(encryptedFile);

        in = PGPUtil.getDecoderStream(in);

        PGPObjectFactory pgpF = new PGPObjectFactory(in, new BcKeyFingerprintCalculator());
        PGPEncryptedDataList enc;
        Object o = pgpF.nextObject();

        if (o instanceof PGPEncryptedDataList) {
            enc = (PGPEncryptedDataList) o;
        } else {
            enc = (PGPEncryptedDataList) pgpF.nextObject();
        }

        // find the secret key
        Iterator it = enc.getEncryptedDataObjects();
        PGPPrivateKey sKey = null;
        PGPPublicKeyEncryptedData pbe = null;
        while (sKey == null && it.hasNext()) {
            pbe = (PGPPublicKeyEncryptedData) it.next();
            System.out.println("pbe id=" + pbe.getKeyID());
            sKey = findSecretKey(keyIn, pbe.getKeyID(), passwd.toCharArray());
        }
        if (sKey == null) {
            throw new IllegalArgumentException("secret key for message not found.");
        }

        InputStream clear = pbe.getDataStream(new BcPublicKeyDataDecryptorFactory(sKey));
        PGPObjectFactory plainFact = new PGPObjectFactory(clear, FP_CALC);
        Object message = plainFact.nextObject();
        if (message instanceof PGPCompressedData) {
            PGPCompressedData cData = (PGPCompressedData) message;
            PGPObjectFactory pgpFact = new PGPObjectFactory(cData.getDataStream(), FP_CALC);
            message = pgpFact.nextObject();
        }

        try (BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(filename))) {
            if (message instanceof PGPLiteralData) {
                PGPLiteralData ld = (PGPLiteralData) message;
                InputStream unc = ld.getInputStream();
                int ch;
                while ((ch = unc.read()) >= 0) {
                    bout.write(ch);
                }

            } else if (message instanceof PGPOnePassSignatureList) {
                throw new PGPException("encrypted message contains a signed message - not literal data.");
            } else {
                throw new PGPException("message is not a simple encrypted file - type unknown.");
            }
            bout.flush();
        }

        if (pbe.isIntegrityProtected()) {
            if (!pbe.verify()) {
                System.err.println("message failed integrity check");
            } else {
                System.err.println("message integrity check passed");
            }
        } else {
            System.err.println("no message integrity check");
        }
    }

    @Test
    public void compareFiles() throws IOException {
        File file1 = new File("src/test/java/com/vodafone/datafusion/plugins/delta/data/in/test1.csv");
        File file2 = new File("src/test/java/com/vodafone/datafusion/plugins/delta/data/out/test1.csv");

        assertTrue("The files differ!", FileUtils.contentEquals(file1, file2));
    }

    private static PGPPrivateKey findSecretKey(InputStream keyIn, long keyID, char[] pass)
            throws IOException, PGPException {
        PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(keyIn), new BcKeyFingerprintCalculator());
        PGPSecretKey pgpSecKey = pgpSec.getSecretKey(keyID);
        if (pgpSecKey == null) {
            return null;
        }
        PBESecretKeyDecryptor secretKeyDecryptor = new JcePBESecretKeyDecryptorBuilder()
                .setProvider(new BouncyCastleProvider()).build(pass);

        return pgpSecKey.extractPrivateKey(secretKeyDecryptor);
    }
}
