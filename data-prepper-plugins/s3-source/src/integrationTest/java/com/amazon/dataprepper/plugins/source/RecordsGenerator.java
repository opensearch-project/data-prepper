/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import java.io.IOException;
import java.io.OutputStream;

public interface RecordsGenerator {
    void write(int numberOfRecords, OutputStream outputStream) throws IOException;
}
