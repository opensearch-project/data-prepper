/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.stream;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class StreamUtils {
    public static Statement parseSparqlStatement(final String stmt) throws IOException {
        final InputStream inputStream = new ByteArrayInputStream(stmt.getBytes(StandardCharsets.UTF_8));
        final Model parse = Rio.parse(inputStream, RDFFormat.NQUADS);
        return new ArrayList<>(parse).get(0);
    }

    public static String getSparqlSubject(final String stmt) throws IOException {
        return parseSparqlStatement(stmt).getSubject().stringValue();
    }
}
