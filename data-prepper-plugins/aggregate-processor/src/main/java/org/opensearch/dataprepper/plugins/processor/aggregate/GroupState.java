/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import java.util.Map;

/**
 * Implementing classes will be shared between all Events that belong to this GroupState.
 * @see DefaultGroupState
 * @since 1.3
 */
public interface GroupState extends Map<Object, Object> {

}
