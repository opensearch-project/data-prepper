/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;

/**
 * Loads Application Default Credentials. Exists so tests do not depend on the machine's ADC.
 */
@FunctionalInterface
interface ApplicationDefaultCredentialsLoader {
    GoogleCredentials load() throws IOException;
}
