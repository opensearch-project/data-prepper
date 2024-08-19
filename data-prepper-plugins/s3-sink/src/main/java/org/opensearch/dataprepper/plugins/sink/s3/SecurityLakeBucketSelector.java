/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.apache.commons.lang3.RandomStringUtils;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import software.amazon.awssdk.services.securitylake.SecurityLakeClient;
import software.amazon.awssdk.services.securitylake.model.AwsIdentity;
import software.amazon.awssdk.services.securitylake.model.CreateCustomLogSourceRequest;
import software.amazon.awssdk.services.securitylake.model.CustomLogSourceProvider;
import software.amazon.awssdk.services.securitylake.model.CustomLogSourceConfiguration;
import software.amazon.awssdk.services.securitylake.model.CreateCustomLogSourceResponse;
import software.amazon.awssdk.services.securitylake.model.CustomLogSourceCrawlerConfiguration;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@DataPrepperPlugin(name = "aws_security_lake", pluginType = S3BucketSelector.class, pluginConfigurationType = SecurityLakeBucketSelectorConfig.class)
public class SecurityLakeBucketSelector implements S3BucketSelector {
    private final SecurityLakeBucketSelectorConfig securityLakeBucketSelectorConfig;

    private S3SinkConfig s3SinkConfig;

    private String pathPrefix;

    private String sourceLocation;

    @DataPrepperPluginConstructor
    public SecurityLakeBucketSelector(final SecurityLakeBucketSelectorConfig securityLakeBucketSelectorConfig) {
        this.securityLakeBucketSelectorConfig = securityLakeBucketSelectorConfig;
    }

    public void initialize(S3SinkConfig s3SinkConfig) {
        this.s3SinkConfig = s3SinkConfig;
        SecurityLakeClient securityLakeClient = SecurityLakeClient.create();
        String arn = s3SinkConfig.getAwsAuthenticationOptions().getAwsStsRoleArn();
        String principal = arn.split(":")[4];
        String sourceName = securityLakeBucketSelectorConfig.getSourceName() != null ? securityLakeBucketSelectorConfig.getSourceName() : RandomStringUtils.randomAlphabetic(7);
        CreateCustomLogSourceResponse response =
                securityLakeClient.createCustomLogSource(
                        CreateCustomLogSourceRequest.builder()
                                .sourceName(sourceName+"_KK_"+RandomStringUtils.randomAlphabetic(4))
                                .eventClasses(List.of(securityLakeBucketSelectorConfig.getLogClass()))
                                .sourceVersion(securityLakeBucketSelectorConfig.getSourceVersion())
                                .configuration(CustomLogSourceConfiguration.builder()
                                        .crawlerConfiguration(CustomLogSourceCrawlerConfiguration.builder()
                                                .roleArn(arn)
                                                .build())
                                        .providerIdentity(AwsIdentity.builder()
                                                .externalId(securityLakeBucketSelectorConfig.getExternalId())
                                                .principal(principal)
                                                .build())
                                        .build())
                                .build());
        CustomLogSourceProvider provider = response.source().provider();
        this.sourceLocation = provider.location();
        final String region=s3SinkConfig.getAwsAuthenticationOptions().getAwsRegion().toString();
        final String accountId=arn.split(":")[4];
        final LocalDate now = LocalDate.now();
        final String eventDay = String.format("%d%02d%02d", now.getYear(), now.getMonthValue(), now.getDayOfMonth());
        int locIdx = sourceLocation.indexOf("/ext/");
        pathPrefix = sourceLocation.substring(locIdx+1)+"region="+region+"/accountId="+accountId+"/eventDay="+eventDay+"/";
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    @Override
    public Map<String, String> getMetadata(int eventCount) {
        return Map.of("asl_rows", Integer.toString(eventCount));
    }

    @Override
    public String getBucketName() {
        int locIndex = sourceLocation.indexOf("/ext/");
        return sourceLocation.substring(5, locIndex);
    }
}
