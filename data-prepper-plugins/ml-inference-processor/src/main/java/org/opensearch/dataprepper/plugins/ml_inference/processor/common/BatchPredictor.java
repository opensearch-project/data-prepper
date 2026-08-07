/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import org.opensearch.dataprepper.common.utils.RetryUtil;

/**
 * Strategy for submitting a single Bedrock BATCH_PREDICT request for a given S3 input URI.
 * Implementations differ in transport: direct connector vs. ml-commons proxy.
 */
interface BatchPredictor {
    RetryUtil.RetryResult predict(String s3Uri);
}
