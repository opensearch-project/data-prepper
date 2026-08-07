/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.aws.api;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * An interface available to plugins which provides the default AWS credentials and region.
 */
public interface AwsContext {
    AwsCredentialsProvider getOrDefault();

    Region getRegionOrDefault();
}
