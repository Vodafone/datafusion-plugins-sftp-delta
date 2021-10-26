package com.vodafone.datafusion.plugins.sftpdelta.common;

import com.google.auth.oauth2.GoogleCredentials;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import static com.vodafone.datafusion.plugins.sftpdelta.common.constants.Constants.*;

public class SFTPDeltaUtilsTest {

    /*
    @Test
    public void testReadGCSFile () throws Exception {
        GoogleCredentials credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(TOKEN_JSON.getBytes()));
        String fileContent = SFTPDeltaUtils.readGCSFile(credentials, PROJECT_ID, BUCKET, BUCKET_OBJECT);
        assertEquals(fileContent,"1624299840");
    }

    @Test
    public void testUpdateGCSFile () throws Exception {
        GoogleCredentials credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(TOKEN_JSON.getBytes()));
        SFTPDeltaUtils.updateGSFile(credentials, PROJECT_ID, BUCKET, BUCKET_OBJECT, "1624299841");

        String fileContent = SFTPDeltaUtils.readGCSFile(credentials, PROJECT_ID, BUCKET, BUCKET_OBJECT);
        assertEquals(fileContent,"1624299841");
    }
     */
}
