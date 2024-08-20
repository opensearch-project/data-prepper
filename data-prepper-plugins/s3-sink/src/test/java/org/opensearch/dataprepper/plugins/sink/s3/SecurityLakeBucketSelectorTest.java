/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;


import software.amazon.awssdk.regions.Region;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.services.securitylake.SecurityLakeClient;
import software.amazon.awssdk.services.securitylake.model.CreateCustomLogSourceRequest;
import software.amazon.awssdk.services.securitylake.model.CustomLogSourceProvider;
import software.amazon.awssdk.services.securitylake.model.CustomLogSourceResource;
import software.amazon.awssdk.services.securitylake.model.CreateCustomLogSourceResponse;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class SecurityLakeBucketSelectorTest {

    @Mock
    SecurityLakeBucketSelectorConfig securityLakeBucketSelectorConfig;

    @Mock
    S3SinkConfig s3SinkConfig;

    @Mock
    AwsAuthenticationOptions awsOptions;

    static final String accountId = "123456789123";

    static final String regionStr = "us-west-2";

    @BeforeEach
    void setup() {
        s3SinkConfig = mock(S3SinkConfig.class);
        securityLakeBucketSelectorConfig = mock(SecurityLakeBucketSelectorConfig.class);
        awsOptions = mock(AwsAuthenticationOptions.class);
        when(awsOptions.getAwsRegion()).thenReturn(Region.of(regionStr));
        when(awsOptions.getAwsStsRoleArn()).thenReturn("arn:aws:iam::"+accountId+":role/Admin");
        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsOptions);
        when(securityLakeBucketSelectorConfig.getSourceName()).thenReturn(RandomStringUtils.randomAlphabetic(5));
        when(securityLakeBucketSelectorConfig.getLogClass()).thenReturn(RandomStringUtils.randomAlphabetic(5));
        when(securityLakeBucketSelectorConfig.getSourceVersion()).thenReturn(RandomStringUtils.randomAlphabetic(5));
        when(securityLakeBucketSelectorConfig.getExternalId()).thenReturn(RandomStringUtils.randomAlphabetic(5));
    }

    private SecurityLakeBucketSelector createObjectUnderTest() {
        return new SecurityLakeBucketSelector(securityLakeBucketSelectorConfig);
    }

    @Test
    public void test_securityLakeBucketSelector() {
        final SecurityLakeClient securityLakeClient = mock(SecurityLakeClient.class);
        CreateCustomLogSourceRequest createCustomLogSourceRequest = mock(CreateCustomLogSourceRequest.class);
        CreateCustomLogSourceResponse createCustomLogSourceResponse = mock(CreateCustomLogSourceResponse.class);
        CustomLogSourceResource customLogSourceResource = mock(CustomLogSourceResource.class);
        CustomLogSourceProvider customLogSourceProvider = mock(CustomLogSourceProvider.class);
        when(createCustomLogSourceResponse.source()).thenReturn(customLogSourceResource);
        when(customLogSourceResource.provider()).thenReturn(customLogSourceProvider);
        String testLocation = "/aws/bucket1/ext/location1/";
        when(customLogSourceProvider.location()).thenReturn(testLocation);
        when(securityLakeClient.createCustomLogSource(any(CreateCustomLogSourceRequest.class))).thenReturn(createCustomLogSourceResponse);
        try (final MockedStatic<SecurityLakeClient> securityLakeClientMockedStatic = mockStatic(SecurityLakeClient.class)) {
            securityLakeClientMockedStatic.when(() -> SecurityLakeClient.create())
                    .thenReturn(securityLakeClient);
            SecurityLakeBucketSelector securityLakeBucketSelector = createObjectUnderTest();
            securityLakeBucketSelector.initialize(s3SinkConfig);
            assertThat(securityLakeBucketSelector.getBucketName(), equalTo("bucket1"));
            int index = testLocation.indexOf("/ext/");
            LocalDate today = LocalDate.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            String formattedDate = today.format(formatter);
            String expectedPathPrefix = testLocation.substring(index+1)+"region="+regionStr+"/accountId="+accountId+"/eventDay="+formattedDate+"/";
            assertThat(securityLakeBucketSelector.getPathPrefix(), equalTo(expectedPathPrefix));
        }
    }
}


