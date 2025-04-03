/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.xrayotlp;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.xrayotlp.configuration.AwsAuthenticationConfiguration;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class XRayOTLPSinkConfigTest {

    @Test
    void testGetAwsConfiguration() {
        final XRayOTLPSinkConfig config = new XRayOTLPSinkConfig();
        final AwsAuthenticationConfiguration awsConfig = new AwsAuthenticationConfiguration();
        config.awsAuthenticationConfiguration = awsConfig;

        assertThat(config.awsAuthenticationConfiguration, notNullValue());
        assertThat(config.awsAuthenticationConfiguration, equalTo(awsConfig));
    }

    @Test
    void testGetAwsConfiguration_whenNull() {
        final XRayOTLPSinkConfig config = new XRayOTLPSinkConfig();
        assertThat(config.awsAuthenticationConfiguration, is(nullValue()));
    }

    @Test
    void testAwsConfiguration_withRegion() {
        final XRayOTLPSinkConfig config = new XRayOTLPSinkConfig();
        final AwsAuthenticationConfiguration awsConfig = new AwsAuthenticationConfiguration();
        awsConfig.awsRegion = "us-west-2";
        config.awsAuthenticationConfiguration = awsConfig;

        assertThat(config.awsAuthenticationConfiguration.getAwsRegion(), equalTo(Region.US_WEST_2));
    }

    @Test
    void testAwsConfiguration_withStsRole() {
        final XRayOTLPSinkConfig config = new XRayOTLPSinkConfig();
        final AwsAuthenticationConfiguration awsConfig = new AwsAuthenticationConfiguration();
        awsConfig.awsStsRoleArn = "arn:aws:iam::123456789012:role/MyRole";
        config.awsAuthenticationConfiguration = awsConfig;

        assertThat(config.awsAuthenticationConfiguration.awsStsRoleArn,
                equalTo("arn:aws:iam::123456789012:role/MyRole"));
    }

    @Test
    void testAwsConfiguration_withCompleteConfig() {
        final XRayOTLPSinkConfig config = new XRayOTLPSinkConfig();
        final AwsAuthenticationConfiguration awsConfig = new AwsAuthenticationConfiguration();
        awsConfig.awsRegion = "us-west-2";
        awsConfig.awsStsRoleArn = "arn:aws:iam::123456789012:role/MyRole";
        awsConfig.awsStsExternalId = "MyExternalId";
        config.awsAuthenticationConfiguration = awsConfig;

        assertThat(config.awsAuthenticationConfiguration.getAwsRegion(), equalTo(Region.US_WEST_2));
        assertThat(config.awsAuthenticationConfiguration.awsStsRoleArn,
                equalTo("arn:aws:iam::123456789012:role/MyRole"));
        assertThat(config.awsAuthenticationConfiguration.awsStsExternalId, equalTo("MyExternalId"));
    }
}
