/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

import com.clearstorydata.dataflow.util.GcsUrlSigningUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by ianlcsd on 6/24/15.
 */
public class GcsUrlSigningUtilTest {
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();


    private byte[] loadKey() throws Exception {

        byte[] tmpBytes = GcsUrlSigningUtil.loadKeyStoreFromClasspath("test.p12");
        InputStream inputStream = new ByteArrayInputStream(tmpBytes);

        File tempFile = testFolder.newFile("test.p12");
        OutputStream outStream = new FileOutputStream(tempFile);

        try {
            byte[] buffer = new byte[8 * 1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }
        } finally {
            outStream.close();
            inputStream.close();
        }

        byte[] bytes = GcsUrlSigningUtil.loadKeyStoreFromFile(tempFile.getAbsolutePath());
        return bytes;
    }


    @Test
    public void testGetSigningUrl() throws Exception {
        byte[] bytes = loadKey();
        Assert.assertNotNull(bytes);
        long expiration = 1435180293260L / 1000;

        String expected = "https://storage.googleapis"
                + ".com/bucketName/objectName?GoogleAccessId=serviceAccountEmail&Expires=1435180293"
                + "&Signature=aCDVWzX4ZKMRtUa5Pazcvvpf72iePB2Mozwu1lShaIliG"
                + "%2BLLFZPpNZ0T5ziJROWleLi5BVmL%2FkUwTJAG4yz7wCPRHt2CSlFbrEo6WmitqN%2FCtyS"
                + "%2BuXfo85CAEeL5lewcbWT1qSTEoDylCYLG4PEF2dtGwATp8q6fMposvCoXHm8%3D";
        String actual = GcsUrlSigningUtil.getSigningUrl(
                "GET",
                "bucketName",
                "objectName",
                "serviceAccountEmail",
                bytes,
                expiration
        );
        Assert.assertEquals(expected, actual);
    }
}
