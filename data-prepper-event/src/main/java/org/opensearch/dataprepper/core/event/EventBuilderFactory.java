/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

abstract class EventBuilderFactory {
    abstract Class<?> getEventClass();

    abstract DefaultBaseEventBuilder createNew();
}
