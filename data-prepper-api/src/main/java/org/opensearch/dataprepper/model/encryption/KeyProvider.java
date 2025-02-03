/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.encryption;

import java.util.function.Function;

public interface KeyProvider extends Function<String, byte[]> {
}
