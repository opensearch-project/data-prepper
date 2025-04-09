/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.xrayotlp.configuration;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class XRayOTLPSinkConfigTest {
    @Test
    void testAwsAuthenticationConfiguration_withAllFields() {
        final String expectedRegion = "us-west-2";
        final String expectedRoleArn = "arn:aws:iam::123456789012:role/MyRole";
        final String expectedExternalId = "external-id-123";

        AwsAuthenticationConfiguration awsConfig = AwsAuthenticationConfiguration.builder()
                .awsRegion(expectedRegion)
                .awsStsRoleArn(expectedRoleArn)
                .awsStsExternalId(expectedExternalId)
                .build();

        XRayOTLPSinkConfig config = XRayOTLPSinkConfig.builder()
                .awsAuthenticationConfiguration(awsConfig)
                .build();

        assertThat(config.getAwsRegion(), equalTo(Region.of(expectedRegion)));
        assertThat(config.getStsRoleArn(), equalTo(expectedRoleArn));
        assertThat(config.getStsExternalId(), equalTo(expectedExternalId));
    }
}
