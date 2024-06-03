/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http;

import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

/**
 * HttpServerConfig is an interface for the Http based source configurations to be shared across different types of Http based sources
*/
public interface HttpServerConfig {

    int getDefaultPort();

    String getDefaultPath();

    boolean isPathValid();

    int getPort();

    String getPath();

    CompressionOption getCompression();

    boolean isSslCertAndKeyFileInS3();

    boolean isSslCertificateFileValid();

    boolean isSslKeyFileValid();

    boolean isAcmCertificateArnValid();

    boolean isAwsRegionValid();

    int getRequestTimeoutInMillis();

    int getBufferTimeoutInMillis();

    int getThreadCount();

    int getMaxConnectionCount();

    int getMaxPendingRequests();

    boolean isSsl();

    String getSslCertificateFile();

    String getSslKeyFile();

    boolean isUseAcmCertificateForSsl();

    String getAcmCertificateArn();

    String getAcmPrivateKeyPassword();

    int getAcmCertificateTimeoutMillis();

    String getAwsRegion();

    PluginModel getAuthentication();

    boolean hasHealthCheckService();

    boolean isUnauthenticatedHealthCheck();

    ByteCount getMaxRequestLength();
}
