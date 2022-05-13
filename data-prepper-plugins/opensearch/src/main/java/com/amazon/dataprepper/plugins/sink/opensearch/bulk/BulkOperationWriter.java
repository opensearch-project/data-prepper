/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.opensearch.bulk;

import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.common.unit.ByteSizeValue;

/**
 * Based on low-level REST client's <code>org.opensearch.action.index.IndexRequest::toString</code> method.
 */
public class BulkOperationWriter {
    private static final int MAX_SOURCE_LENGTH_IN_TOSTRING = 2048;

    public static String bulkOperationToString(BulkOperation bulkOperation) {
        String index = bulkOperation.index().index();
        String source = extractDocumentSource(bulkOperation);
        String id = bulkOperation.index().id();

        String sSource = "_na_";
        try {
            if (source.length() > MAX_SOURCE_LENGTH_IN_TOSTRING) {
                sSource = "n/a, actual length: ["
                        + new ByteSizeValue(source.length()).toString()
                        + "], max length: "
                        + new ByteSizeValue(MAX_SOURCE_LENGTH_IN_TOSTRING).toString();
            } else {
                sSource = source;
            }
        } catch (Exception e) {
            // ignore
        }
        return "index {[" + index + "][" + id + "], source[" + sSource + "]}";
    }

    private static String extractDocumentSource(BulkOperation bulkOperation) {
        final JsonData document = (JsonData) bulkOperation.index().document();

        return document.toJson().toString();
    }
}
