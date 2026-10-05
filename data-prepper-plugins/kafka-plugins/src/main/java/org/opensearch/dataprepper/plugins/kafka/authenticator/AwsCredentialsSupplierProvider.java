/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.authenticator;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;

public class AwsCredentialsSupplierProvider {
    private static final AwsCredentialsSupplierProvider singleton = new AwsCredentialsSupplierProvider();

    public static AwsCredentialsSupplierProvider getInstance() {
        return singleton;
    }

    // set once before Kafka client construction; volatile suffices for cross-thread visibility
    private volatile AwsCredentialsSupplier awsCredentialsSupplier;

    // Used for testing only
    protected AwsCredentialsSupplierProvider() {}

    public AwsCredentialsSupplier getAwsCredentialsSupplier() {
        return awsCredentialsSupplier;
    }

    public void set(final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }
}
