/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * Returns true if the approach to this output is to compress internally to the data.
     * For example, Parquet should not have whole-file compression. The compression only
     * should happen inside individual row groups.
     * @return True if the compression is internal to the format; false if whole-file compression is ok.
     */
    default boolean isCompressionInternal() {
        return false;
    }

    default Set<BufferTypeOptions> getIncompatibleBufferTypes() {
        return Collections.emptySet();
    }

    /**
     * Validates the data against the sample events provided.
     *
     * @param expectedRecords     The total expected records
     * @param sampleEventData     The collection of all the sample event data as maps. This is the first N values.
     * @param actualContentFile   The actual file which has been downloaded and decompressed as part of the test
     * @param compressionScenario The compression scenario. This should only be needed when {@link #isCompressionInternal()} is true.
     * @throws IOException Some IOException
     */
    void validate(int expectedRecords, List<Map<String, Object>> sampleEventData, File actualContentFile, CompressionScenario compressionScenario) throws IOException;
}
