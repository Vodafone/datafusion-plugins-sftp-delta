package com.vodafone.datafusion.plugins.delta.encryption;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.io.FileInputStream;

public class PGPCertUtilTest {

    @Test
    public void readPublicKey() throws Exception {
        String publicKeyFile = "src/test/java/com/vodafone/datafusion/plugins/delta/data/PGP1D0.pkr";

        assertEquals(PGPCertUtil.readPublicKey(new FileInputStream(publicKeyFile)).getClass().getName(),
                "org.bouncycastle.openpgp.PGPPublicKey");
    }
}
