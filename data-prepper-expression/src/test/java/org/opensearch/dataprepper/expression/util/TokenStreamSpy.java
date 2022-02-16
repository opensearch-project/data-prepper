/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.misc.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class TokenStreamSpy extends CommonTokenStream {
    private static final Logger LOG = LoggerFactory.getLogger(TokenStreamSpy.class);

    private final Integer position = 0;

    public TokenStreamSpy(final TokenSource tokenSource) {
        super(tokenSource);
    }

    @Override
    protected Token LB(final int k) {
        final Token token = super.LB(k);
        LOG.info("LB({}) @ {} -> {}", k, position, token.getText());
        return token;
    }

    @Override
    protected int adjustSeekIndex(final int i) {
        LOG.trace("adjustSeekIndex {}", i);
        return super.adjustSeekIndex(i);
    }

    @Override
    public int getNumberOfOnChannelTokens() {
        LOG.trace("getNumberOfOnChannelTokens");
        return super.getNumberOfOnChannelTokens();
    }

    @Override
    public TokenSource getTokenSource() {
        LOG.trace("getTokenSource");
        return super.getTokenSource();
    }

    @Override
    public int index() {
        LOG.trace("index");
        return super.index();
    }

    @Override
    public int mark() {
        LOG.trace("mark");
        return super.mark();
    }

    @Override
    public void release(final int marker) {
        LOG.trace("release {}", marker);
        super.release(marker);
    }

    @Override
    public void reset() {
        LOG.trace("reset");
        super.reset();
    }

    @Override
    public void seek(final int index) {
        LOG.trace("seek {}", index);
        super.seek(index);
    }

    @Override
    public int size() {
        LOG.trace("size");
        return super.size();
    }

    @Override
    protected boolean sync(final int i) {
        LOG.trace("sync {}", i);
        return super.sync(i);
    }

    @Override
    protected int fetch(final int n) {
        LOG.trace("fetch {}", n);
        return super.fetch(n);
    }

    @Override
    public Token get(final int i) {
        LOG.trace("get {}", i);
        return super.get(i);
    }

    @Override
    public List<Token> get(final int start, final int stop) {
        LOG.trace("get {} {}", start, stop);
        return super.get(start, stop);
    }

    @Override
    public int LA(final int i) {
        LOG.trace("LA i");
        return super.LA(i);
    }

    @Override
    protected void setup() {
        LOG.trace("setup");
        super.setup();
    }

    @Override
    public void setTokenSource(final TokenSource tokenSource) {
        LOG.trace("setTokenSource {}", tokenSource);
        super.setTokenSource(tokenSource);
    }

    @Override
    public List<Token> getTokens() {
        LOG.trace("getTokens");
        return super.getTokens();
    }

    @Override
    public List<Token> getTokens(final int start, final int stop) {
        LOG.trace("getTokens {} {}", start, stop);
        return super.getTokens(start, stop);
    }

    @Override
    public List<Token> getTokens(final int start, final int stop, final Set<Integer> types) {
        LOG.trace("getTokens {} {} {}", start, stop, types);
        return super.getTokens(start, stop, types);
    }

    @Override
    public List<Token> getTokens(final int start, final int stop, final int ttype) {
        LOG.trace("getTokens {} {} {}", start, stop, ttype);
        return super.getTokens(start, stop, ttype);
    }

    @Override
    protected int nextTokenOnChannel(final int i, final int channel) {
        LOG.trace("nextTokenOnChannel {} {}", i, channel);
        return super.nextTokenOnChannel(i, channel);
    }

    @Override
    protected int previousTokenOnChannel(final int i, final int channel) {
        LOG.trace("previousTokenOnChannel {} {}", i, channel);
        return super.previousTokenOnChannel(i, channel);
    }

    @Override
    public List<Token> getHiddenTokensToRight(final int tokenIndex, final int channel) {
        LOG.trace("getHiddenTokensToRight {} {}", tokenIndex, channel);
        return super.getHiddenTokensToRight(tokenIndex, channel);
    }

    @Override
    public List<Token> getHiddenTokensToRight(final int tokenIndex) {
        LOG.trace("getHiddenTokensToRight {}", tokenIndex);
        return super.getHiddenTokensToRight(tokenIndex);
    }

    @Override
    public List<Token> getHiddenTokensToLeft(final int tokenIndex, final int channel) {
        LOG.trace("getHiddenTokensToLeft {} {}", tokenIndex, channel);
        return super.getHiddenTokensToLeft(tokenIndex, channel);
    }

    @Override
    public List<Token> getHiddenTokensToLeft(final int tokenIndex) {
        LOG.trace("getHiddenTokensToLeft {}", tokenIndex);
        return super.getHiddenTokensToLeft(tokenIndex);
    }

    @Override
    protected List<Token> filterForChannel(final int from, final int to, final int channel) {
        LOG.trace("filterForChannel {} {} {}", from, to, channel);
        return super.filterForChannel(from, to, channel);
    }

    @Override
    public String getSourceName() {
        LOG.trace("getSourceName");
        return super.getSourceName();
    }

    @Override
    public String getText() {
        LOG.trace("getText");
        return super.getText();
    }

    @Override
    public String getText(final Interval interval) {
        LOG.trace("getText {}", interval);
        return super.getText(interval);
    }

    @Override
    public String getText(final RuleContext ctx) {
        LOG.trace("getText {}", ctx);
        return super.getText(ctx);
    }

    @Override
    public String getText(final Token start, final Token stop) {
        LOG.trace("getText {} {}", start, stop);
        return super.getText(start, stop);
    }

    @Override
    public void fill() {
        LOG.trace("fill");
        super.fill();
    }

    @Override
    protected void finalize() throws Throwable {
        LOG.trace("finalize");
        super.finalize();
    }

    @Override
    public Token LT(final int k) {
        final Token token = super.LT(k);
        if (k >= 1) {
            LOG.info("LT({}) -> {}", position, token.getText());
        }
        else if (k == -1) {
            LOG.info("LT({}) -> {}", position + k, token.getText());
        }
//            else {
//                throw new RuntimeException("Unexpected K = " + k);
//            }
        return token;
    }

    @Override
    public void consume() {
        LOG.trace("consume");
//            final Integer beforeP = super.p;
//            final Integer beforePosition = position;
//            position++;
        super.consume();
//            LOG.warn("Consume, position {} -> {}, p {} -> {}", beforePosition, position, beforeP, super.p);
    }
}
