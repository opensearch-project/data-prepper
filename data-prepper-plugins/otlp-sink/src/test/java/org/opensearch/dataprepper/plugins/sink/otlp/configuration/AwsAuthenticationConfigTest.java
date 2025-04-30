/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AwsAuthenticationConfigTest {

    private final String expectedRoleArn = "arn:aws:iam::123456789012:role/MyRole";
    private final String expectedExternalId = "external-id-123";
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    void testDeserializationFromYaml() throws Exception {
        final String yaml = String.join("\n",
                "sts_role_arn: " + expectedRoleArn,
                "sts_external_id: " + expectedExternalId
        );

        AwsAuthenticationConfig config = mapper.readValue(yaml, AwsAuthenticationConfig.class);

        assertEquals(expectedRoleArn, config.getAwsStsRoleArn());
        assertEquals(expectedExternalId, config.getAwsStsExternalId());
    }
}
