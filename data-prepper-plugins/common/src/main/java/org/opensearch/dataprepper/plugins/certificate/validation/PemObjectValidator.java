package org.opensearch.dataprepper.plugins.certificate.validation;

import org.bouncycastle.util.io.pem.PemReader;

import java.io.IOException;
import java.io.StringReader;

public class PemObjectValidator {
    public static boolean isPemObject(final String certificate) {
        try (PemReader reader = new PemReader(new StringReader(certificate))) {
            return reader.readPemObject() != null;
        } catch (IOException e) {
            return false;
        }
    }
}
