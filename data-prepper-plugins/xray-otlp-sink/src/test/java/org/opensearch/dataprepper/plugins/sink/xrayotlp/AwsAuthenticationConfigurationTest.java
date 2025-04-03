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
import static org.hamcrest.MatcherAssert.assertThat;

class AwsAuthenticationConfigurationTest {

    @Test
    void testGetRegion() {
        final AwsAuthenticationConfiguration config = new AwsAuthenticationConfiguration();
        config.awsRegion = "us-west-2";

        assertThat(config.getAwsRegion(), notNullValue());
        assertThat(config.getAwsRegion(), equalTo(Region.US_WEST_2));
    }

    @Test
    void testGetStsRoleArn() {
        final AwsAuthenticationConfiguration config = new AwsAuthenticationConfiguration();
        final String roleArn = "arn:aws:iam::123456789012:role/MyRole";
        config.awsStsRoleArn = roleArn;

        assertThat(config.awsStsRoleArn, equalTo(roleArn));
    }

    @Test
    void testGetStsExternalId() {
        final AwsAuthenticationConfiguration config = new AwsAuthenticationConfiguration();
        final String externalId = "myExternalId";
        config.awsStsExternalId = externalId;

        assertThat(config.awsStsExternalId, equalTo(externalId));
    }
}
