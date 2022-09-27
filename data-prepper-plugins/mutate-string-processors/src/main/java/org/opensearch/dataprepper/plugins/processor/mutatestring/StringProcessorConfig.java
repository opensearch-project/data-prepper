/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import java.util.List;

public interface StringProcessorConfig<T> {
    List<T> getIterativeConfig();
}
