/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.file;

import java.io.InputStream;

public interface DynamicFileReader {
    InputStream getFile();
}
