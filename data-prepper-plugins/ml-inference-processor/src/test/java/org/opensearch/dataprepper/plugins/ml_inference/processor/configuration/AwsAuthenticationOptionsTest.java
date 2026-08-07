/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class AwsAuthenticationOptionsTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void getJobStsRoleArn_whenSet_returnsValue() throws Exception {
        final AwsAuthenticationOptions options = OBJECT_MAPPER.readValue(
                "{\"region\":\"us-east-1\",\"job_sts_role_arn\":\"arn:aws:iam::123456789012:role/JobRole\"}",
                AwsAuthenticationOptions.class);

        assertThat(options.getJobStsRoleArn(), is("arn:aws:iam::123456789012:role/JobRole"));
    }

    @Test
    void getJobStsRoleArn_whenNotSet_returnsNull() throws Exception {
        final AwsAuthenticationOptions options = OBJECT_MAPPER.readValue(
                "{\"region\":\"us-east-1\"}",
                AwsAuthenticationOptions.class);

        assertThat(options.getJobStsRoleArn(), is(nullValue()));
    }
}
