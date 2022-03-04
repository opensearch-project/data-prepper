/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.drop;

enum HandleFailedEventsOption {
    skip,
    skip_silently,
    drop,
    drop_silently,
}
