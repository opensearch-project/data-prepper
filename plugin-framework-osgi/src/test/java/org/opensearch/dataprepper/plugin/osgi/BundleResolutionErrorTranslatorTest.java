/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi;

import org.junit.jupiter.api.Test;
import org.osgi.framework.BundleException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class BundleResolutionErrorTranslatorTest {

    @Test
    void translate_with_null_exception_returns_generic_message() {
        final String result = BundleResolutionErrorTranslator.translate("my.plugin", null);

        assertThat(result, is(notNullValue()));
        assertThat(result, containsString("my.plugin"));
        assertThat(result, containsString("unknown error"));
    }

    @Test
    void translate_with_empty_message_returns_generic_message() {
        final BundleException ex = new BundleException("");

        final String result = BundleResolutionErrorTranslator.translate("my.plugin", ex);

        assertThat(result, containsString("my.plugin"));
        assertThat(result, containsString("no details available"));
    }

    @Test
    void translate_with_null_message_returns_generic_message() {
        final BundleException ex = new BundleException("", BundleException.RESOLVE_ERROR, null);

        final String result = BundleResolutionErrorTranslator.translate("my.plugin", ex);

        assertThat(result, is(notNullValue()));
    }

    @Test
    void translateMessage_parses_felix_version_mismatch_format() {
        final String felixMessage = "Unable to resolve org.opensearch.dataprepper.plugin.jackson "
                + "revision 1.0: missing requirement [org.opensearch.dataprepper.plugin.jackson "
                + "revision 1.0] osgi.wiring.package; "
                + "(&(osgi.wiring.package=com.fasterxml.jackson.core)"
                + "(version>=2.17.0)(!(version>=3.0.0)))";

        final String result = BundleResolutionErrorTranslator.translateMessage("jackson-plugin", felixMessage);

        assertThat(result, containsString("jackson-plugin"));
        assertThat(result, containsString("com.fasterxml.jackson.core"));
        assertThat(result, containsString("2.17.0"));
        assertThat(result, containsString("3.0.0"));
        assertThat(result, containsString("could not be resolved"));
    }

    @Test
    void translateMessage_parses_felix_missing_package_no_version() {
        final String felixMessage = "Unable to resolve org.opensearch.dataprepper.plugin.custom "
                + "revision 1.0: missing requirement [org.opensearch.dataprepper.plugin.custom "
                + "revision 1.0] osgi.wiring.package; "
                + "(osgi.wiring.package=org.example.nonexistent)";

        final String result = BundleResolutionErrorTranslator.translateMessage("custom-plugin", felixMessage);

        assertThat(result, containsString("custom-plugin"));
        assertThat(result, containsString("org.example.nonexistent"));
        assertThat(result, containsString("not exported"));
    }

    @Test
    void translateMessage_parses_filter_syntax_format() {
        final String felixMessage = "some prefix osgi.wiring.package "
                + "filter:=\"(&(osgi.wiring.package=org.opensearch.dataprepper.model.event)"
                + "(version>=2.15.0)(!(version>=3.0.0)))\"";

        final String result = BundleResolutionErrorTranslator.translateMessage("event-plugin", felixMessage);

        assertThat(result, containsString("event-plugin"));
        assertThat(result, containsString("org.opensearch.dataprepper.model.event"));
        assertThat(result, containsString("2.15.0"));
        assertThat(result, containsString("3.0.0"));
    }

    @Test
    void translateMessage_falls_back_to_generic_for_unrecognized_format() {
        final String weirdMessage = "Something entirely unexpected happened during resolution";

        final String result = BundleResolutionErrorTranslator.translateMessage("weird-plugin", weirdMessage);

        assertThat(result, containsString("weird-plugin"));
        assertThat(result, containsString("could not be resolved"));
        assertThat(result, containsString(weirdMessage));
    }

    @Test
    void translateMessage_handles_multiline_felix_message() {
        final String multiline = "Unable to resolve org.opensearch.dataprepper.plugin.foo\n"
                + "  revision 2.0:\n"
                + "  missing requirement\n"
                + "  [org.opensearch.dataprepper.plugin.foo revision 2.0]\n"
                + "  osgi.wiring.package;\n"
                + "  (&(osgi.wiring.package=org.apache.commons.lang3)"
                + "(version>=3.14.0)(!(version>=4.0.0)))";

        final String result = BundleResolutionErrorTranslator.translateMessage("foo-plugin", multiline);

        assertThat(result, containsString("foo-plugin"));
        assertThat(result, containsString("org.apache.commons.lang3"));
        assertThat(result, containsString("3.14.0"));
        assertThat(result, containsString("4.0.0"));
    }

    @Test
    void translateMessage_with_null_returns_generic() {
        final String result = BundleResolutionErrorTranslator.translateMessage("null-msg-plugin", null);

        assertThat(result, containsString("null-msg-plugin"));
        assertThat(result, containsString("no details available"));
    }

    @Test
    void translate_from_bundle_exception_with_real_message() {
        final BundleException ex = new BundleException(
                "Unable to resolve test.bundle revision 1.0: missing requirement "
                        + "[test.bundle revision 1.0] osgi.wiring.package; "
                        + "(&(osgi.wiring.package=org.slf4j)(version>=2.0.0)(!(version>=3.0.0)))",
                BundleException.RESOLVE_ERROR);

        final String result = BundleResolutionErrorTranslator.translate("test.bundle", ex);

        assertThat(result, containsString("test.bundle"));
        assertThat(result, containsString("org.slf4j"));
        assertThat(result, containsString("2.0.0"));
        assertThat(result, containsString("3.0.0"));
    }

    @Test
    void translateMessage_handles_missing_requirement_with_ampersand_prefix() {
        final String felixMessage = "Unable to resolve plugin.xyz revision 1.0: "
                + "missing requirement [plugin.xyz revision 1.0] "
                + "osgi.wiring.package; (&(osgi.wiring.package=io.micrometer.core)"
                + "(version>=1.10.0)(!(version>=2.0.0)))";

        final String result = BundleResolutionErrorTranslator.translateMessage("plugin.xyz", felixMessage);

        assertThat(result, containsString("plugin.xyz"));
        assertThat(result, containsString("io.micrometer.core"));
        assertThat(result, containsString("[1.10.0,2.0.0)"));
    }
}
