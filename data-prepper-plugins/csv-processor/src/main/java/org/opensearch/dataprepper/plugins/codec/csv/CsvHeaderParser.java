/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class CsvHeaderParser {

    public static List<String> headerParser(final String location) throws Exception {
        try(final CSVReader reader = new CSVReader(new FileReader(location))) {
            final List<String> headerList = new ArrayList<>();
            final String[] header = reader.readNext();
            if (header != null) {
                for (final String columnName : header) {
                    headerList.add(columnName);
                }
            } else {
                throw new Exception("Header not found in CSV Header file.");
            }
            return headerList;
        } catch (FileNotFoundException e) {
            throw new Exception("CSV Header file not found.");
        }
    }
}
