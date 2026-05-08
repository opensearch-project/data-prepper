/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class ScriptManagerMinifyTest {

    @Test
    void minifyScript_removes_comment_lines() {
        final String source = "// This is a comment\nctx._source.field = params.value;\n// Another comment\nctx.op = 'none';";
        final String result = ScriptManager.minifyScript(source);
        assertThat(result, equalTo("ctx._source.field = params.value;\nctx.op = 'none';"));
    }

    @Test
    void minifyScript_removes_blank_lines() {
        final String source = "ctx._source.a = 1;\n\n\nctx._source.b = 2;";
        final String result = ScriptManager.minifyScript(source);
        assertThat(result, equalTo("ctx._source.a = 1;\nctx._source.b = 2;"));
    }

    @Test
    void minifyScript_trims_leading_whitespace() {
        final String source = "  ctx._source.a = 1;\n    ctx._source.b = 2;";
        final String result = ScriptManager.minifyScript(source);
        assertThat(result, equalTo("ctx._source.a = 1;\nctx._source.b = 2;"));
    }

    @Test
    void minifyScript_handles_mixed_comments_blanks_and_code() {
        final String source = "// Header comment\n\nString table = params.table;\n// Inline explanation\nlong version = Long.parseLong(params.version);\n\n// Footer\n";
        final String result = ScriptManager.minifyScript(source);
        assertThat(result, equalTo("String table = params.table;\nlong version = Long.parseLong(params.version);"));
    }

    @Test
    void minifyScript_returns_null_for_null_input() {
        assertThat(ScriptManager.minifyScript(null), nullValue());
    }

    @Test
    void minifyScript_preserves_inline_comments_after_code() {
        final String source = "ctx._source.a = 1; // keep this line";
        final String result = ScriptManager.minifyScript(source);
        assertThat(result, equalTo("ctx._source.a = 1; // keep this line"));
    }

    @Test
    void minifyScript_single_line_no_comments() {
        final String source = "ctx._source.counter += 1;";
        final String result = ScriptManager.minifyScript(source);
        assertThat(result, equalTo("ctx._source.counter += 1;"));
    }
}
