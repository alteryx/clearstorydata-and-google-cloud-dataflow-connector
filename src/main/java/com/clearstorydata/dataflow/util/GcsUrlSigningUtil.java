/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

package com.clearstorydata.dataflow.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.utils.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URLEncoder;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Signature;

/**
 * Created by ianlcsd on 6/12/15.
 */
public class GcsUrlSigningUtil {

    // expire in three hours
    private static final long EXPIRATION_HOURS_IN_SECONDS = 3 * 3600;
    private static final char[] DEFAULT_SECRET = "notasecret".toCharArray();


    /**
     * Sign the url composed from the parameters.
     * @param verb http verb
     * @param bucketName Gcs bucket name
     * @param objectName Gcs object name
     * @param serviceAccountEmail the service account email from google cloud platform
     * @param keyStore the byte[] of the key store
     * @param expiration expirtion time in epoch second
     * @return the signed utl
     * @throws Exception if issues encountered when singing
     */
    public static String getSigningUrl(
            String verb,
            String bucketName,
            String objectName,
            String serviceAccountEmail,
            byte[] keyStore,
            long expiration
    ) throws Exception {
        PrivateKey key = loadKeyFromKeyStore(keyStore);

        String urlSignature = signString(
                verb + "\n\n\n" + expiration + "\n" + "/" + bucketName
                        + "/" + objectName, key);

        String signedUrl = "https://storage.googleapis.com/" + bucketName + "/" + objectName
                + "?GoogleAccessId=" + serviceAccountEmail
                + "&Expires=" + expiration
                + "&Signature=" + URLEncoder.encode(urlSignature, "UTF-8");

        return signedUrl;

    }


    /**
     * Loading the Pkcs12 keystore from classpath.
     *
     * @param classpath classpath to the ket store
     * @return byte[] for the key store
     * @throws Exception possible exception when rading the key file
     */
    public static byte[] loadKeyStoreFromClasspath(String classpath) throws
            Exception {
        InputStream is = null;
        try {
            is = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(classpath);
            byte[] bytes = IOUtils.toByteArray(is);
            return bytes;
        } finally {
            is.close();
        }
    }

    /**
     * Loading the Pkcs12 keystore from the specified filename.
     *
     * @param filename key store file name.
     *                 The file addressed by the filename has to be locally
     *                 available
     * @return byte[] for the key store
     * @throws Exception possible exception when rading the key file
     */
    public static byte[] loadKeyStoreFromFile(String filename) throws
            Exception {
        InputStream is = null;
        try {
            is = new FileInputStream(new File(filename));
            byte[] bytes =  IOUtils.toByteArray(is);
            return bytes;
        } finally {
            is.close();
        }
    }

    private static PrivateKey loadKeyFromKeyStore(byte[] keyStore) throws
            Exception {
        InputStream is = null;
        try {
            is = new ByteArrayInputStream(keyStore);
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(is, DEFAULT_SECRET);
            return (PrivateKey) ks.getKey("privatekey", DEFAULT_SECRET);
        } finally {
            is.close();
        }
    }

    private static String signString(String stringToSign, PrivateKey key) throws Exception {
        if (key == null) {
            throw new Exception("Private Key not provided");
        }
        Signature signer = Signature.getInstance("SHA256withRSA");
        signer.initSign(key);
        signer.update(stringToSign.getBytes("UTF-8"));
        byte[] rawSignature = signer.sign();
        return new String(Base64.encodeBase64(rawSignature, false), "UTF-8");
    }
}
