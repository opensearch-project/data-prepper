/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
