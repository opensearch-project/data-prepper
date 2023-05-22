/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.api;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public interface AwsCredentialsSupplier {
    AwsCredentialsProvider getProvider(AwsCredentialsOptions options);
}
