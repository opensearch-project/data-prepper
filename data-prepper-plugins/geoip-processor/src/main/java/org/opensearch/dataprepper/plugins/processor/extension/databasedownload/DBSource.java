/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.net.ssl.HostnameVerifier;
import java.io.File;
import java.io.UncheckedIOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

public interface DBSource {

    public static final Logger LOG = LoggerFactory.getLogger(DBSource.class);
    public String tempFolderPath = System.getProperty("java.io.tmpdir")+ File.separator +"GeoIP";
    public String tarFolderPath = tempFolderPath + "/tar";
    public String downloadTarFilepath = tarFolderPath + "/out.tar.gz";
    public void initiateDownload(List<String> config) throws Exception;

    /**
     * create Folder If Not Exist
     * @param outputFilePath Output File Path
     * @return File
     */
    static File createFolderIfNotExist(String outputFilePath) {
        final File destFile = new File(outputFilePath);
        try {
            if (!destFile.exists()) {
                destFile.mkdirs();
            }
        }
        catch (UncheckedIOException ex) {
            LOG.info("Create Folder If NotExist Exception {0}", ex);
        }
        return destFile;
    }

    /**
     * Delete Directory
     * @param file file
     */
    static void deleteDirectory(File file) {

        if (file.exists()) {
            for (final File subFile : file.listFiles()) {
                if (subFile.isDirectory()) {
                    deleteDirectory(subFile);
                }
                subFile.delete();
            }
            file.delete();
        }
    }

    /**
     * initiateSSL
     * @throws NoSuchAlgorithmException NoSuchAlgorithmException
     * @throws KeyManagementException KeyManagementException
     */
    default void initiateSSL() throws NoSuchAlgorithmException, KeyManagementException {
        final TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                    public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {
                        return;
                    }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) throws CertificateException {
                        return;
                    }
                }
        };

        final SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        final HostnameVerifier hostnameVerifier = new HostnameVerifier() {
            public boolean verify(String urlHostName, SSLSession session) {
                return true;
            }
        };
        HttpsURLConnection.setDefaultHostnameVerifier(hostnameVerifier);
    }
}
