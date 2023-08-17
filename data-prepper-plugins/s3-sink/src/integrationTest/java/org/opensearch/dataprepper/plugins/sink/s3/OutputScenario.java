/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.model.codec.OutputCodec;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Represents a scenario for the output format.
 */
public interface OutputScenario {
    /**
     * Gets the codec this scenario uses.
     *
     * @return The {@link OutputCodec}
     */
    OutputCodec getCodec();

    /**
     * Validates the data against all the events provided.
     *
     * @param allEventData The collection of all the expected event maps.
     * @param actualContentFile The actual file which has been downloaded and decompressed as part of the test
     * @throws IOException Some IOException
     */
    void validate(List<Map<String, Object>> allEventData, File actualContentFile) throws IOException;
}
