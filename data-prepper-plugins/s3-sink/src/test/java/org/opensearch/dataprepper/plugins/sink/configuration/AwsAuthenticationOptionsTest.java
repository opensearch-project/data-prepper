/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

class AwsAuthenticationOptionsTest {

    private AwsAuthenticationOptions awsAuthenticationOptions;

    @BeforeEach
    void setUp() {
        awsAuthenticationOptions = new AwsAuthenticationOptions();
    }

    @Test
    void getAwsRegion_returns_null_when_region_is_null() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsAuthenticationOptions, "awsRegion", null);
        assertThat(awsAuthenticationOptions.getAwsRegion(), nullValue());
    }
    private void reflectivelySetField(final AwsAuthenticationOptions awsAuthenticationOptions, final String fieldName,
            final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = AwsAuthenticationOptions.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(awsAuthenticationOptions, value);
        } finally {
            field.setAccessible(false);
        }
    }
}