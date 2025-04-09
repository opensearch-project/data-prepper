/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.xrayotlp.configuration;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class AwsAuthenticationConfigurationTest {

    @Test
    void testGetRegion_whenRegionIsNull_returnNull() {
        final AwsAuthenticationConfiguration config = AwsAuthenticationConfiguration.builder().build();

        assertThat(config.getAwsRegion(), nullValue());
    }

    @Test
    void testAwsAuthenticationConfigurationFields() {
        final String expectedRegion = "us-west-2";
        final String expectedRoleArn = "arn:aws:iam::123456789012:role/MyRole";
        final String expectedExternalId = "myExternalId";

        final AwsAuthenticationConfiguration config = AwsAuthenticationConfiguration.builder()
                .awsRegion(expectedRegion)
                .awsStsRoleArn(expectedRoleArn)
                .awsStsExternalId(expectedExternalId)
                .build();

        assertThat(config.getAwsRegion(), notNullValue());
        assertThat(config.getAwsRegion(), equalTo(Region.US_WEST_2));
        assertThat(config.getAwsStsRoleArn(), equalTo(expectedRoleArn));
        assertThat(config.getAwsStsExternalId(), equalTo(expectedExternalId));
    }
}
