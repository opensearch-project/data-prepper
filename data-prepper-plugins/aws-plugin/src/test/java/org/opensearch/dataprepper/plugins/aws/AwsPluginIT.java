/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AwsPluginIT {
    @Mock
    private AwsPluginConfig awsPluginConfig;

    @Mock
    private ExtensionPoints extensionPoints;

    @Mock
    private ExtensionProvider.Context context;

    @Mock
    private AwsStsConfiguration awsDefaultStsConfiguration;

    @BeforeEach
    void setup() {
        when(awsPluginConfig.getDefaultStsConfiguration()).thenReturn(awsDefaultStsConfiguration);
    }

    private AwsPlugin createObjectUnderTest() {
        return new AwsPlugin(awsPluginConfig);
    }

    @Test
    void test_AwsPlugin_with_STS_role() {
        createObjectUnderTest().apply(extensionPoints);

        final ArgumentCaptor<ExtensionProvider<AwsCredentialsSupplier>> extensionProviderArgumentCaptor = ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());

        final ExtensionProvider<AwsCredentialsSupplier> extensionProvider = extensionProviderArgumentCaptor.getValue();

        final Optional<AwsCredentialsSupplier> optionalSupplier = extensionProvider.provideInstance(context);
        assertThat(optionalSupplier, notNullValue());
        assertThat(optionalSupplier.isPresent(), equalTo(true));

        final AwsCredentialsSupplier awsCredentialsSupplier = optionalSupplier.get();

        final String stsRole = createStsRole();
        final AwsCredentialsOptions awsCredentialsOptions1 = AwsCredentialsOptions.builder()
                .withStsRoleArn(stsRole)
                .withRegion(Region.US_EAST_1)
                .build();

        final AwsCredentialsProvider awsCredentialsProvider1 = awsCredentialsSupplier.getProvider(awsCredentialsOptions1);

        assertThat(awsCredentialsProvider1, instanceOf(StsAssumeRoleCredentialsProvider.class));

        final AwsCredentialsOptions awsCredentialsOptions2 = AwsCredentialsOptions.builder()
                .withStsRoleArn(stsRole)
                .withRegion(Region.US_EAST_1)
                .build();

        final AwsCredentialsProvider awsCredentialsProvider2 = awsCredentialsSupplier.getProvider(awsCredentialsOptions2);

        assertThat(awsCredentialsProvider2, sameInstance(awsCredentialsProvider1));
    }

    @Test
    void test_AwsPlugin_without_STS_role() {
        when(awsDefaultStsConfiguration.getAwsStsRoleArn()).thenReturn(null);

        createObjectUnderTest().apply(extensionPoints);

        final ArgumentCaptor<ExtensionProvider<AwsCredentialsSupplier>> extensionProviderArgumentCaptor = ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());

        final ExtensionProvider<AwsCredentialsSupplier> extensionProvider = extensionProviderArgumentCaptor.getValue();

        final Optional<AwsCredentialsSupplier> optionalSupplier = extensionProvider.provideInstance(context);
        assertThat(optionalSupplier, notNullValue());
        assertThat(optionalSupplier.isPresent(), equalTo(true));

        final AwsCredentialsSupplier awsCredentialsSupplier = optionalSupplier.get();

        final AwsCredentialsOptions awsCredentialsOptions1 = AwsCredentialsOptions.builder()
                .withRegion(Region.US_EAST_1)
                .build();

        final AwsCredentialsProvider awsCredentialsProvider1 = awsCredentialsSupplier.getProvider(awsCredentialsOptions1);

        assertThat(awsCredentialsProvider1, instanceOf(DefaultCredentialsProvider.class));

        final AwsCredentialsOptions awsCredentialsOptions2 = AwsCredentialsOptions.builder()
                .withRegion(Region.US_EAST_1)
                .build();

        final AwsCredentialsProvider awsCredentialsProvider2 = awsCredentialsSupplier.getProvider(awsCredentialsOptions2);

        assertThat(awsCredentialsProvider2, sameInstance(awsCredentialsProvider1));
    }

    @Test
    void test_AwsPlugin_without_STS_role_and_with_default_role_uses_default_role() {
        when(awsDefaultStsConfiguration.getAwsStsRoleArn()).thenReturn(createStsRole());

        createObjectUnderTest().apply(extensionPoints);

        final ArgumentCaptor<ExtensionProvider<AwsCredentialsSupplier>> extensionProviderArgumentCaptor = ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());

        final ExtensionProvider<AwsCredentialsSupplier> extensionProvider = extensionProviderArgumentCaptor.getValue();

        final Optional<AwsCredentialsSupplier> optionalSupplier = extensionProvider.provideInstance(context);
        assertThat(optionalSupplier, notNullValue());
        assertThat(optionalSupplier.isPresent(), equalTo(true));

        final AwsCredentialsSupplier awsCredentialsSupplier = optionalSupplier.get();

        final AwsCredentialsOptions awsCredentialsOptions1 = AwsCredentialsOptions.builder()
                .withRegion(Region.US_EAST_1)
                .build();

        final AwsCredentialsProvider awsCredentialsProvider1 = awsCredentialsSupplier.getProvider(awsCredentialsOptions1);

        assertThat(awsCredentialsProvider1, instanceOf(StsAssumeRoleCredentialsProvider.class));

        final AwsCredentialsOptions awsCredentialsOptions2 = AwsCredentialsOptions.builder()
                .withRegion(Region.US_EAST_1)
                .build();

        final AwsCredentialsProvider awsCredentialsProvider2 = awsCredentialsSupplier.getProvider(awsCredentialsOptions2);

        assertThat(awsCredentialsProvider2, sameInstance(awsCredentialsProvider1));
    }

    @Test
    void test_AwsPlugin_without_STS_role_and_without_default_role_uses_default_role() {

        createObjectUnderTest().apply(extensionPoints);

        final ArgumentCaptor<ExtensionProvider<AwsCredentialsSupplier>> extensionProviderArgumentCaptor = ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints).addExtensionProvider(extensionProviderArgumentCaptor.capture());

        final ExtensionProvider<AwsCredentialsSupplier> extensionProvider = extensionProviderArgumentCaptor.getValue();

        final Optional<AwsCredentialsSupplier> optionalSupplier = extensionProvider.provideInstance(context);
        assertThat(optionalSupplier, notNullValue());
        assertThat(optionalSupplier.isPresent(), equalTo(true));

        final AwsCredentialsSupplier awsCredentialsSupplier = optionalSupplier.get();

        final AwsCredentialsOptions awsCredentialsOptions1 = AwsCredentialsOptions.builder()
                .withRegion(Region.US_EAST_1)
                .withUseDefaultCredentialsProvider(true)
                .build();

        final AwsCredentialsProvider awsCredentialsProvider1 = awsCredentialsSupplier.getProvider(awsCredentialsOptions1);

        assertThat(awsCredentialsProvider1, instanceOf(DefaultCredentialsProvider.class));

        final AwsCredentialsOptions awsCredentialsOptions2 = AwsCredentialsOptions.builder()
                .withRegion(Region.US_EAST_1)
                .withUseDefaultCredentialsProvider(true)
                .build();

        final AwsCredentialsProvider awsCredentialsProvider2 = awsCredentialsSupplier.getProvider(awsCredentialsOptions2);

        assertThat(awsCredentialsProvider2, instanceOf(DefaultCredentialsProvider.class));
        assertThat(awsCredentialsProvider2, sameInstance(awsCredentialsProvider1));
    }

    private String createStsRole() {
        return String.format("arn:aws:iam::123456789012:role/%s", UUID.randomUUID());
    }
}
