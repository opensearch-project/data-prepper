/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.dataprepper.aws.api;

public interface AwsCredentialsConfig {
    String getRegion();
    String getStsRoleArn();

    AwsCredentialsOptions toCredentialsOptions();
}
