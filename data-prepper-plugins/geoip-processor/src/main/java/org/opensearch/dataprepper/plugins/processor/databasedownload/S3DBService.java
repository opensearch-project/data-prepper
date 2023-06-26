/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databasedownload;


import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorConfig;
import org.opensearch.dataprepper.plugins.processor.configuration.DatabasePathURLConfig;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Implementation class for Download through S3
 */
public class S3DBService implements DBSource {

    /**
     * S3DBService constructor for initialisation of attributes
     * @param geoIPProcessorConfig geoIPProcessorConfig
     */
    public S3DBService(GeoIPProcessorConfig geoIPProcessorConfig) {
       //TODO
    }

    /**
     * Initialisation of Download through Url
     * @param s3URLs s3URLs
     */
    public void initiateDownload(List<DatabasePathURLConfig> s3URLs)  {
        //TODO : Initialisation of Download through Url
    }

    /**
     * Initiate SLL for X509TrustManager
     * @throws NoSuchAlgorithmException NoSuchAlgorithmException
     * @throws KeyManagementException KeyManagementException
     */
    @Override
    public void initiateSSL() throws NoSuchAlgorithmException, KeyManagementException {
        //TODO : Initiate SLL for X509TrustManager
    }

    /**
     * Build Request And DownloadFile
     * @param bucketName bucketName
     */
    @Override
    public void buildRequestAndDownloadFile(String bucketName)  {
        //TODO : Build Request And DownloadFile
    }

    /**
     * Create S3TransferManager instance
     * @return S3TransferManager
     */
    public S3TransferManager createCustomTransferManager() {
        //TODO : Create S3TransferManager instance
        return null;
    }
}
