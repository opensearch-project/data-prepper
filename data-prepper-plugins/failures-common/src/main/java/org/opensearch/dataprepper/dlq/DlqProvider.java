/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package main.java.org.opensearch.dataprepper.dlq;

import java.util.Optional;

/**
 * An interface for providing {@link DlqWriter}s.
 * <p>
 * Plugin authors can use this interface for providing {@link DlqWriter}s
 *
 * @since 2.2
 */
public interface DlqProvider {


    /**
     * Allows implementors to provide a {@link DlqWriter}. This may be optional, in which case it is not used.
     * @since 2.2
     */
    default Optional<DlqWriter> getDlqWriter() {
        return Optional.empty();
    }
}
