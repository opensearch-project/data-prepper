/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import org.opensearch.dataprepper.model.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Enriches GenAI agent traces by propagating attributes from child spans to root
 * and stripping conflicting flattened sub-keys.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/semconv/gen-ai/">OTel GenAI Semantic Conventions v1.39.0</a>
 */
final class GenAiEnrichmentHelper {
    private static final Logger LOG = LoggerFactory.getLogger(GenAiEnrichmentHelper.class);

    private static final String GEN_AI_SYSTEM_KEY = "gen_ai.system";
    private static final String GEN_AI_PROVIDER_NAME_KEY = "gen_ai.provider.name";
    private static final String GEN_AI_AGENT_NAME_KEY = "gen_ai.agent.name";
    private static final String GEN_AI_REQUEST_MODEL_KEY = "gen_ai.request.model";
    private static final String GEN_AI_INPUT_TOKENS_KEY = "gen_ai.usage.input_tokens";
    private static final String GEN_AI_OUTPUT_TOKENS_KEY = "gen_ai.usage.output_tokens";
    private static final String GEN_AI_OPERATION_NAME_KEY = "gen_ai.operation.name";
    /**
     * Marks the outermost GenAI span of a trace: the root when it is a GenAI span, else the agent
     * span below a non-GenAI root (for example an agent behind an HTTP handler), so consumers can
     * list the trace by that span. A source sets it to {@code true} to choose the AI roots itself,
     * or to {@code false} to keep a span from being marked.
     */
    static final String DERIVED_GEN_AI_IS_ROOT_KEY = "derived.gen_ai.is_root";
    private static final String ATTRIBUTES_PREFIX = "attributes/";

    private static final String[] PROPAGATED_STRING_KEYS = {
            GEN_AI_SYSTEM_KEY, GEN_AI_PROVIDER_NAME_KEY, GEN_AI_AGENT_NAME_KEY, GEN_AI_REQUEST_MODEL_KEY
    };

    private static final String[] FLATTENED_PARENT_KEYS = {
            "llm.input_messages", "llm.output_messages",
            "gen_ai.prompt", "gen_ai.completion"
    };

    private GenAiEnrichmentHelper() {}

    /**
     * Enriches a batch of spans: normalizes vendor attributes, strips flattened sub-keys,
     * marks the outermost GenAI spans of each trace, then
     * propagates select gen_ai attributes from children to the root spans that are AI roots.
     */
    static void enrichBatch(final List<Span> spans) {
        enrichBatch(spans, GenAiTraceState.NONE);
    }

    /**
     * Same as {@link #enrichBatch(List)}, using and updating {@code traceState} so spans that
     * arrive after their ancestors are not mistaken for a trace's outermost GenAI span, and so
     * roots can be enriched from children that were processed in earlier batches.
     */
    static void enrichBatch(final List<Span> spans, final GenAiTraceState traceState) {
        enrichBatch(spans, traceState, GenAiRootPropagation.AI_ROOT);
    }

    /**
     * Same as {@link #enrichBatch(List, GenAiTraceState)}, propagating attributes to the roots
     * {@code propagation} selects.
     */
    static void enrichBatch(final List<Span> spans, final GenAiTraceState traceState,
                            final GenAiRootPropagation propagation) {
        enrichBatch(spans, traceState, propagation, traceId -> Optional.empty(), span -> false);
    }

    /**
     * Same as {@link #enrichBatch(List, GenAiTraceState, GenAiRootPropagation)}, deciding whether a
     * GenAI span is its trace's outermost through its whole known ancestor chain: the spans of the
     * batch, then the parent links recorded in {@code traceState}, ending at the trace's root span
     * id that {@code rootSpanIdLookup} returns. A span whose chain reaches a span id that is not known
     * yet cannot be decided: when {@code mayPark} accepts it, it is neither marked nor recorded as
     * marked, and is returned so the caller can hold it until its missing ancestor arrives;
     * otherwise it is treated as outermost, as when nothing beyond the first parent was known.
     * Unresolved spans are still recorded in the trace state, covered with their parent links, so
     * their descendants are known to be below a GenAI span whatever happens to them.
     *
     * @return the spans to hold back, still in {@code spans}; empty when nothing was held back
     */
    static List<Span> enrichBatch(final List<Span> spans, final GenAiTraceState traceState,
                                  final GenAiRootPropagation propagation,
                                  final Function<String, Optional<String>> rootSpanIdLookup,
                                  final Predicate<Span> mayPark) {
        for (final Span span : spans) {
            normalizeAttributes(span);
            stripFlattenedSubkeys(span);
        }

        final Map<String, List<Span>> spansByTrace = new LinkedHashMap<>();
        for (final Span span : spans) {
            spansByTrace.computeIfAbsent(span.getTraceId(), k -> new ArrayList<>()).add(span);
        }

        final List<Span> parked = new ArrayList<>();
        for (final Map.Entry<String, List<Span>> trace : spansByTrace.entrySet()) {
            final TraceBatchView batchView = new TraceBatchView(trace.getValue());
            if (!batchView.hasGenAiData()) {
                // Spans arriving once the trace has GenAI data still link a later span to its ancestors.
                traceState.updateIfPresent(trace.getKey(), batchView::recordParentLinks);
                continue;
            }
            final Optional<String> rootSpanId = trace.getKey() == null
                    ? Optional.empty() : rootSpanIdLookup.apply(trace.getKey());
            // Read, mark and record under the trace's lock: spans of one trace can be in batches
            // that pipeline workers process at the same time.
            traceState.update(trace.getKey(), state -> enrichTrace(batchView, state, propagation, rootSpanId, mayPark, parked));
        }
        return parked;
    }

    /**
     * Enriches one trace: records the batch into the trace's accumulated state first, then marks
     * the outermost GenAI spans, then fills the root from the state. Reading the root's data from
     * the state rather than from its batchmates means a root processed without its children, in
     * any batch after they were recorded, is still enriched. When the trace's root is a GenAI span
     * or marked at the source, in this batch or recorded in the state, every other span descends
     * from it, so only the root can be marked and no span needs to wait for its ancestors.
     */
    private static void enrichTrace(final TraceBatchView batchView, final GenAiTraceState.TraceState state,
                                    final GenAiRootPropagation propagation, final Optional<String> rootSpanId,
                                    final Predicate<Span> mayPark, final List<Span> parked) {
        final Set<String> known = new HashSet<>(state.coveredSpanIds());
        final SpanInfo root = batchView.root();
        final boolean batchHasGenAiRoot = root != null && (root.genAi || Boolean.TRUE.equals(root.marker));
        final List<SpanInfo> outermost = new ArrayList<>();
        if (batchHasGenAiRoot || state.isGenAiRootSeen()) {
            if (root != null && root.genAi) {
                outermost.add(root);
            }
        } else {
            final boolean mayHoldBack = !batchView.hasSourceMarker();
            for (final SpanInfo info : batchView.spans) {
                if (!info.genAi) {
                    continue;
                }
                final Resolution resolution = batchView.resolveOutermost(info, known, state, rootSpanId);
                if (resolution == Resolution.UNRESOLVED && mayHoldBack
                        && !Boolean.FALSE.equals(info.marker) && mayPark.test(info.span)) {
                    parked.add(info.span);
                } else if (resolution != Resolution.NOT_OUTERMOST) {
                    outermost.add(info);
                }
            }
            outermost.sort(BY_START_TIME);
        }
        if (batchHasGenAiRoot) {
            state.recordGenAiRoot();
        }
        // Recording reads the spans' own token counts, before marking adds aggregates to agent roots.
        recordBatch(batchView, known, state);
        markAgentRoots(batchView, outermost);
        if (root != null && propagatesTo(root, propagation)) {
            enrichRootSpanFromState(root, state);
        }
    }

    /**
     * Whether the outermost question for a held-back span can now be decided from the trace state
     * alone, for example because its missing ancestor was processed by another pipeline worker.
     */
    static boolean isOutermostResolvable(final Span span, final GenAiTraceState traceState,
                                         final Function<String, Optional<String>> rootSpanIdLookup) {
        final TraceBatchView batchView = new TraceBatchView(List.of(span));
        final SpanInfo info = batchView.spans.get(0);
        final Optional<String> rootSpanId = span.getTraceId() == null
                ? Optional.empty() : rootSpanIdLookup.apply(span.getTraceId());
        return traceState.query(span.getTraceId(), state -> state.isGenAiRootSeen()
                || batchView.resolveOutermost(info, state.coveredSpanIds(), state, rootSpanId) != Resolution.UNRESOLVED);
    }

    /**
     * Records a batch of one trace's spans into the trace's accumulated state: the covered span
     * ids, the parent link of every span, reported token usage (topmost reporters first, so an
     * agent's total is not counted on top of the calls it already contains) and the propagated
     * gen_ai.* values, preferring the outermost agent spans as root enrichment always has.
     */
    private static void recordBatch(final TraceBatchView batchView, final Set<String> known,
                                    final GenAiTraceState.TraceState state) {
        final Set<String> covered = batchView.coveredSpanIds(known);
        batchView.recordParentLinks(state);
        for (final String spanId : covered) {
            state.cover(spanId);
        }

        final List<SpanInfo> tokenSpans = new ArrayList<>();
        for (final SpanInfo info : batchView.spans) {
            if (info.spanId() != null && info.hasTokens()) {
                tokenSpans.add(info);
            }
        }
        tokenSpans.sort(Comparator.comparingInt(info -> state.ancestorDepth(info.spanId())));
        for (final SpanInfo info : tokenSpans) {
            state.recordTokenUsage(info.spanId(),
                    info.inputTokens != null ? info.inputTokens : 0L,
                    info.outputTokens != null ? info.outputTokens : 0L);
        }

        final SpanInfo root = batchView.root();
        final List<SpanInfo> ordered = new ArrayList<>();
        for (final SpanInfo info : batchView.spans) {
            if (info != root) {
                ordered.add(info);
            }
        }
        // Outermost among the children: GenAI spans with no GenAI ancestor below the root.
        final Set<SpanInfo> outer = new HashSet<>();
        for (final SpanInfo info : ordered) {
            if (info.genAi && !batchView.hasAncestor(info, parent -> parent != root && (parent.genAi || parent.marked), known)) {
                outer.add(info);
            }
        }
        ordered.sort(Comparator.<SpanInfo>comparingInt(info -> outer.contains(info) ? 0 : info.genAi ? 1 : 2)
                .thenComparing(BY_START_TIME));
        for (final SpanInfo info : ordered) {
            for (final String key : PROPAGATED_STRING_KEYS) {
                final Object value = info.attributes.get(key);
                if (value != null) {
                    state.recordPropagatedValue(key, value.toString());
                }
            }
        }
    }

    /**
     * Whether the root gets its children's GenAI attributes and token totals. With
     * {@code ai_root}, only a root that is its trace's AI root: a GenAI span, or a span marked
     * {@code true} at the source. A non-GenAI root, such as the HTTP span an agent runs behind,
     * would otherwise carry a model, agent name and tokens it did not produce, and every token
     * would be counted again by consumers that sum usage across spans.
     */
    private static boolean propagatesTo(final SpanInfo root, final GenAiRootPropagation propagation) {
        switch (propagation) {
            case ALL_ROOTS:
                return true;
            case NONE:
                return false;
            case AI_ROOT:
            default:
                return root.genAi || Boolean.TRUE.equals(root.marker);
        }
    }

    /** One span with the attributes this helper reads, deserialized once. */
    private static final class SpanInfo {
        private final Span span;
        private final Map<String, Object> attributes;
        private final boolean genAi;
        private final Boolean marker;
        private final Long inputTokens;
        private final Long outputTokens;
        private final Instant startTime;
        private boolean marked;

        private SpanInfo(final Span span) {
            this.span = span;
            final Map<String, Object> attrs = span.getAttributes();
            this.attributes = attrs == null ? Map.of() : attrs;
            this.genAi = attributes.get(GEN_AI_OPERATION_NAME_KEY) != null;
            this.marker = toBoolean(attributes.get(DERIVED_GEN_AI_IS_ROOT_KEY));
            this.marked = Boolean.TRUE.equals(marker);
            this.inputTokens = toLong(attributes.get(GEN_AI_INPUT_TOKENS_KEY));
            this.outputTokens = toLong(attributes.get(GEN_AI_OUTPUT_TOKENS_KEY));
            this.startTime = toInstant(span.getStartTime());
        }

        private String spanId() {
            return span.getSpanId();
        }

        private String parentSpanId() {
            return span.getParentSpanId();
        }

        private boolean hasTokens() {
            return inputTokens != null || outputTokens != null;
        }

        private boolean isRoot() {
            return isNoParent(parentSpanId());
        }
    }

    private static boolean isNoParent(final String parentSpanId) {
        return parentSpanId == null || parentSpanId.isEmpty() || "0000000000000000".equals(parentSpanId);
    }

    /** Whether a GenAI span is its trace's outermost, as far as the spans known so far tell. */
    private enum Resolution {
        OUTERMOST,
        NOT_OUTERMOST,
        UNRESOLVED
    }

    private static final Comparator<SpanInfo> BY_START_TIME = Comparator.comparing(
            (SpanInfo info) -> info.startTime, Comparator.nullsLast(Comparator.naturalOrder()));

    /**
     * The fragment of one trace present in the current batch. It may hold only the spans of the
     * trace that are in this batch, so its parent/child tree can be partial: parents, children or
     * the root may be missing.
     */
    private static final class TraceBatchView {
        private final List<SpanInfo> spans = new ArrayList<>();
        private final Map<String, SpanInfo> bySpanId = new HashMap<>();
        private final Map<String, List<SpanInfo>> childrenByParent = new HashMap<>();

        private TraceBatchView(final Collection<Span> traceSpans) {
            for (final Span span : traceSpans) {
                final SpanInfo info = new SpanInfo(span);
                spans.add(info);
                if (info.spanId() != null) {
                    bySpanId.put(info.spanId(), info);
                }
                if (info.parentSpanId() != null) {
                    childrenByParent.computeIfAbsent(info.parentSpanId(), k -> new ArrayList<>()).add(info);
                }
            }
        }

        private int size() {
            return spans.size();
        }

        private boolean hasGenAiData() {
            for (final SpanInfo info : spans) {
                if (info.genAi || info.marker != null || info.hasTokens()) {
                    return true;
                }
                for (final String key : PROPAGATED_STRING_KEYS) {
                    if (info.attributes.containsKey(key)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private SpanInfo root() {
            for (final SpanInfo info : spans) {
                if (info.isRoot()) {
                    return info;
                }
            }
            return null;
        }

        private boolean hasSourceMarker() {
            for (final SpanInfo info : spans) {
                if (Boolean.TRUE.equals(info.marker)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Whether an ancestor of {@code info} matches: looked up in the batch, then, for the first
         * parent outside it, in {@code known} (span ids covered in earlier batches).
         */
        private boolean hasAncestor(final SpanInfo info, final Predicate<SpanInfo> matches, final Set<String> known) {
            final Set<String> seen = new HashSet<>();
            String parentId = info.parentSpanId();
            SpanInfo parent = bySpanId.get(parentId);
            while (parent != null && seen.add(parent.spanId())) {
                if (matches.test(parent)) {
                    return true;
                }
                parentId = parent.parentSpanId();
                parent = bySpanId.get(parentId);
            }
            return parentId != null && known.contains(parentId);
        }

        private boolean hasGenAiAncestor(final SpanInfo info, final Set<String> known) {
            return hasAncestor(info, parent -> parent.genAi || parent.marked, known);
        }

        /**
         * Whether {@code info} is outermost: walks up its ancestors through the batch, then the
         * parent links recorded in {@code state}. Reaching a GenAI, marked or {@code known} covered
         * span means it is not outermost; reaching the trace's root span id, or a span with no
         * parent, without passing one means it is; reaching a span id known nowhere leaves it
         * unresolved.
         */
        private Resolution resolveOutermost(final SpanInfo info, final Set<String> known,
                                            final GenAiTraceState.TraceState state, final Optional<String> rootSpanId) {
            final Set<String> seen = new HashSet<>();
            String parentId = info.parentSpanId();
            while (!isNoParent(parentId)) {
                if (!seen.add(parentId)) {
                    return Resolution.OUTERMOST;
                }
                final SpanInfo parent = bySpanId.get(parentId);
                if (parent != null) {
                    if (parent.genAi || parent.marked) {
                        return Resolution.NOT_OUTERMOST;
                    }
                    parentId = parent.parentSpanId();
                    continue;
                }
                if (known.contains(parentId)) {
                    return Resolution.NOT_OUTERMOST;
                }
                if (rootSpanId.isPresent() && rootSpanId.get().equals(parentId)) {
                    return Resolution.OUTERMOST;
                }
                final String grandparentId = state.parentSpanId(parentId);
                if (grandparentId == null) {
                    return Resolution.UNRESOLVED;
                }
                parentId = grandparentId;
            }
            return Resolution.OUTERMOST;
        }

        /** Records the parent link of every span of the batch. */
        private void recordParentLinks(final GenAiTraceState.TraceState state) {
            for (final SpanInfo info : spans) {
                if (info.spanId() != null && !isNoParent(info.parentSpanId())) {
                    state.recordParentLink(info.spanId(), info.parentSpanId());
                }
            }
        }

        /** Span ids of the batch that are, or are below, a GenAI span or a marked span. */
        private Set<String> coveredSpanIds(final Set<String> known) {
            final Set<String> covered = new HashSet<>();
            for (final SpanInfo info : spans) {
                if (info.spanId() != null && (info.genAi || info.marked || hasGenAiAncestor(info, known))) {
                    covered.add(info.spanId());
                }
            }
            return covered;
        }

        private List<SpanInfo> descendantsOf(final SpanInfo ancestor) {
            final List<SpanInfo> descendants = new ArrayList<>();
            final List<SpanInfo> pending = new ArrayList<>(childrenByParent.getOrDefault(ancestor.spanId(), List.of()));
            final Set<String> seen = new HashSet<>();
            seen.add(ancestor.spanId());
            while (!pending.isEmpty()) {
                final SpanInfo descendant = pending.remove(pending.size() - 1);
                if (!seen.add(descendant.spanId())) {
                    continue;
                }
                descendants.add(descendant);
                pending.addAll(childrenByParent.getOrDefault(descendant.spanId(), List.of()));
            }
            return descendants;
        }

        /**
         * Sums token usage over {@code subset}, counting only spans with no ancestor in the subset
         * that also reports usage. Agent spans such as invoke_agent often report the total of their
         * own LLM calls, so adding every span with usage would count those calls twice.
         */
        private TokenTotals sumTopmostTokens(final Collection<SpanInfo> subset) {
            final Set<SpanInfo> members = new HashSet<>(subset);
            final TokenTotals totals = new TokenTotals();
            for (final SpanInfo info : subset) {
                if (!info.hasTokens()
                        || hasAncestor(info, parent -> members.contains(parent) && parent.hasTokens(), Set.of())) {
                    continue;
                }
                totals.found = true;
                totals.input += info.inputTokens != null ? info.inputTokens : 0;
                totals.output += info.outputTokens != null ? info.outputTokens : 0;
            }
            return totals;
        }
    }

    private static final class TokenTotals {
        private long input;
        private long output;
        private boolean found;
    }

    /**
     * Marks the outermost GenAI spans of a trace with {@code derived.gen_ai.is_root} and
     * aggregates their descendants' tokens onto them. Skipped when a span is already marked
     * {@code true}, so a span marked by the SDK or the user is the only AI root; a span set to
     * {@code false} is never marked.
     */
    private static void markAgentRoots(final TraceBatchView batchView, final List<SpanInfo> outermost) {
        if (batchView.hasSourceMarker()) {
            LOG.debug("Trace {} already has a span marked {} = true; not marking its spans",
                    batchView.spans.get(0).span.getTraceId(), DERIVED_GEN_AI_IS_ROOT_KEY);
            return;
        }
        for (final SpanInfo agentRoot : outermost) {
            if (Boolean.FALSE.equals(agentRoot.marker)) {
                continue;
            }
            agentRoot.span.put(ATTRIBUTES_PREFIX + DERIVED_GEN_AI_IS_ROOT_KEY, true);
            agentRoot.marked = true;
            LOG.debug("Marked span {} as the GenAI root of trace {}", agentRoot.spanId(), agentRoot.span.getTraceId());
            if (agentRoot.hasTokens()) {
                continue;
            }
            final TokenTotals tokens = batchView.sumTopmostTokens(batchView.descendantsOf(agentRoot));
            if (tokens.found) {
                agentRoot.span.put(ATTRIBUTES_PREFIX + GEN_AI_INPUT_TOKENS_KEY, tokens.input);
                agentRoot.span.put(ATTRIBUTES_PREFIX + GEN_AI_OUTPUT_TOKENS_KEY, tokens.output);
            }
        }
    }

    /**
     * Propagates gen_ai.* string attributes and aggregated token counts from the trace's
     * accumulated state to the root. Skips the attributes the root already has. The state carries
     * the first-seen value of each attribute, preferring the outermost GenAI spans within each
     * batch, so the trace gets the outer agent's name and model whatever order the spans arrive in.
     */
    private static void enrichRootSpanFromState(final SpanInfo root, final GenAiTraceState.TraceState state) {
        for (final String key : PROPAGATED_STRING_KEYS) {
            if (!root.attributes.containsKey(key)) {
                final String value = state.propagatedValue(key);
                if (value != null) {
                    root.span.put(ATTRIBUTES_PREFIX + key, value);
                    LOG.debug("Propagated {} = {} to root span {}", key, value, root.spanId());
                }
            }
        }

        if (!root.hasTokens() && state.hasTokenTotals()) {
            root.span.put(ATTRIBUTES_PREFIX + GEN_AI_INPUT_TOKENS_KEY, state.inputTokenTotal());
            root.span.put(ATTRIBUTES_PREFIX + GEN_AI_OUTPUT_TOKENS_KEY, state.outputTokenTotal());
            LOG.debug("Aggregated tokens (input={}, output={}) to root span {}",
                    state.inputTokenTotal(), state.outputTokenTotal(), root.spanId());
        }
    }

    /**
     * Whether a span is itself a GenAI span or is marked {@code true} at the source: the test that
     * makes a span covered on its own, without a GenAI ancestor. Reads the attributes as they
     * arrived, before vendor normalization.
     */
    static boolean isGenAiOrMarkedSpan(final Span span) {
        final Map<String, Object> attributes = span.getAttributes();
        if (attributes == null) {
            return false;
        }
        return attributes.get(GEN_AI_OPERATION_NAME_KEY) != null
                || Boolean.TRUE.equals(toBoolean(attributes.get(DERIVED_GEN_AI_IS_ROOT_KEY)));
    }

    /**
     * Whether a trace root is a GenAI root that is still missing a model or token usage the
     * accumulated trace state cannot provide yet, so holding it for late children could complete
     * it. Reads the attributes as they arrived, before vendor normalization.
     */
    static boolean rootAwaitsGenAiEnrichment(final Span rootSpan, final GenAiTraceState traceState) {
        if (!isGenAiOrMarkedSpan(rootSpan)) {
            return false;
        }
        final Map<String, Object> attributes = rootSpan.getAttributes();
        final boolean needsModel = !attributes.containsKey(GEN_AI_REQUEST_MODEL_KEY);
        final boolean needsTokens = !attributes.containsKey(GEN_AI_INPUT_TOKENS_KEY)
                && !attributes.containsKey(GEN_AI_OUTPUT_TOKENS_KEY);
        if (!needsModel && !needsTokens) {
            return false;
        }
        return !traceState.query(rootSpan.getTraceId(), state ->
                (!needsModel || state.propagatedValue(GEN_AI_REQUEST_MODEL_KEY) != null)
                        && (!needsTokens || state.hasTokenTotals()));
    }

    /** A token count from a number or a numeric string; anything else is treated as absent. */
    private static Long toLong(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return (long) Double.parseDouble(((String) value).trim());
            } catch (final NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static Boolean toBoolean(final Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            final String text = ((String) value).trim();
            if ("true".equalsIgnoreCase(text)) {
                return Boolean.TRUE;
            }
            if ("false".equalsIgnoreCase(text)) {
                return Boolean.FALSE;
            }
        }
        return null;
    }

    /** Start times as instants: ISO-8601 strings with 0 to 9 fractional digits do not sort as text. */
    private static Instant toInstant(final String time) {
        if (time == null) {
            return null;
        }
        try {
            return Instant.parse(time);
        } catch (final DateTimeParseException e) {
            return null;
        }
    }

    /**
     * Normalizes vendor-specific attributes to OTel GenAI Semantic Conventions.
     * Copies values to the standard gen_ai.* key, keeping originals intact.
     * Skips if the target attribute already exists on the span. When several source
     * attributes map to the same target, the one listed first in the mappings file wins.
     */
    static void normalizeAttributes(final Span span) {
        final Map<String, Object> attrs = span.getAttributes();
        if (attrs == null) {
            return;
        }

        // Pick one source per target first: span.put does not update attrs, so checking attrs
        // while writing would let every later source overwrite the earlier one.
        final Map<String, Map.Entry<String, Object>> sources = new HashMap<>();
        for (final Map.Entry<String, Object> entry : attrs.entrySet()) {
            final GenAiAttributeMappings.MappingTarget target =
                    GenAiAttributeMappings.getLookupTable().get(entry.getKey());
            if (target == null || attrs.containsKey(target.getKey())) {
                continue;
            }
            final Map.Entry<String, Object> current = sources.get(target.getKey());
            if (current == null || target.getPriority()
                    < GenAiAttributeMappings.getLookupTable().get(current.getKey()).getPriority()) {
                sources.put(target.getKey(), entry);
            }
        }

        for (final Map.Entry<String, Object> entry : sources.values()) {
            final GenAiAttributeMappings.MappingTarget target =
                    GenAiAttributeMappings.getLookupTable().get(entry.getKey());

            Object value = entry.getValue();
            if (value instanceof String) {
                final String strVal = (String) value;
                if ("gen_ai.operation.name".equals(target.getKey())) {
                    final String mapped = GenAiAttributeMappings.getOperationNameValues()
                            .get(strVal.toLowerCase());
                    if (mapped != null) {
                        value = mapped;
                    }
                }
                if (target.isWrapAsArray()) {
                    value = "[\"" + value + "\"]";
                }
            }

            span.put(ATTRIBUTES_PREFIX + target.getKey(), value);
        }
    }

    /**
     * Removes flattened sub-keys (e.g. "llm.input_messages.0.message.content") that
     * conflict with parent string values (e.g. "llm.input_messages"), preventing OpenSearch mapping failures.
     * Only strips when the parent string value exists. If only sub-keys exist, they are preserved.
     */
    static void stripFlattenedSubkeys(final Span span) {
        final Map<String, Object> attrs = span.getAttributes();
        if (attrs == null) {
            return;
        }

        final List<String> toRemove = new ArrayList<>();
        for (final String parentKey : FLATTENED_PARENT_KEYS) {
            if (!attrs.containsKey(parentKey)) {
                continue;
            }
            for (final String key : attrs.keySet()) {
                if (key.startsWith(parentKey + ".") && key.length() > parentKey.length() + 1
                        && Character.isDigit(key.charAt(parentKey.length() + 1))) {
                    toRemove.add(key);
                }
            }
        }

        for (final String key : toRemove) {
            try {
                span.delete(ATTRIBUTES_PREFIX + key);
            } catch (final Exception e) {
                LOG.warn("Failed to delete flattened sub-key {}: {}", key, e.getMessage());
            }
        }
    }
}
