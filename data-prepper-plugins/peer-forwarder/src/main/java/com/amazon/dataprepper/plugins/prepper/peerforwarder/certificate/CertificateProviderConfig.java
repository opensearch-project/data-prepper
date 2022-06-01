/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.acm.model.InvalidArnException;

public class CertificateProviderConfig {
    private static final String S3_PREFIX = "s3://";

    private final boolean useAcmCertForSSL;
    private final String acmCertificateArn;
    private final String awsRegion;
    private final long acmCertIssueTimeOutMillis;
    private final String sslKeyCertChainFile;

    public CertificateProviderConfig(final boolean useAcmCertForSSL, final String acmCertificateArn, final String awsRegion, final long acmCertIssueTimeOutMillis, final String sslKeyCertChainFile) {
        this.useAcmCertForSSL = useAcmCertForSSL;
        this.acmCertificateArn = acmCertificateArn;
        if(acmCertificateArn != null) {
            try {
                Arn.fromString(acmCertificateArn);
            } catch(Exception e) {
                throw InvalidArnException.builder().message("Invalid ARN format for acmCertificateArn").build();
            }
        }
        this.awsRegion = awsRegion;
        this.acmCertIssueTimeOutMillis = acmCertIssueTimeOutMillis;
        this.sslKeyCertChainFile = sslKeyCertChainFile;
    }

    public boolean useAcmCertForSSL() {
        return useAcmCertForSSL;
    }

    public String getAcmCertificateArn() {
        return acmCertificateArn;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public long getAcmCertIssueTimeOutMillis() {
        return acmCertIssueTimeOutMillis;
    }

    public String getSslKeyCertChainFile() {
        return sslKeyCertChainFile;
    }

    public boolean isSslCertFileInS3() {
        return sslKeyCertChainFile.toLowerCase().startsWith(S3_PREFIX);
    }
}
