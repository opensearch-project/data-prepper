/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.util;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.Locale;

public class TestIDGenerator {
    public static String generateRandomTestID() {
        return RandomStringUtils.random(8, true, false).toLowerCase(Locale.ROOT);
    }
}
