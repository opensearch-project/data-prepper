package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

public abstract class GrammarTest {
    //region ParseTree child classes
    protected static final Class<? extends ParseTree> CONDITIONAL_EXPRESSION = DataPrepperExpressionParser.ConditionalExpressionContext.class;
    protected static final Class<? extends ParseTree> CONDITIONAL_OPERATOR = DataPrepperExpressionParser.ConditionalOperatorContext.class;
    protected static final Class<? extends ParseTree> EQUALITY_OPERATOR_EXPRESSION =
            DataPrepperExpressionParser.EqualityOperatorExpressionContext.class;
    protected static final Class<? extends ParseTree> EQUALITY_OPERATOR = DataPrepperExpressionParser.EqualityOperatorContext.class;
    protected static final Class<? extends ParseTree> REGEX_OPERATOR_EXPRESSION =
            DataPrepperExpressionParser.RegexOperatorExpressionContext.class;
    protected static final Class<? extends ParseTree> RELATIONAL_OPERATOR_EXPRESSION =
            DataPrepperExpressionParser.RelationalOperatorExpressionContext.class;
    protected static final Class<? extends ParseTree> RELATIONAL_OPERATOR = DataPrepperExpressionParser.RelationalOperatorContext.class;
    //endregion

    protected ErrorListener errorListener;

    protected ParserRuleContext parseExpression(final String expression) {
        errorListener = new ErrorListener();

        final CodePointCharStream stream = CharStreams.fromString(expression);
        final DataPrepperExpressionLexer lexer = new DataPrepperExpressionLexer(stream);
        lexer.addErrorListener(errorListener);

        final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        final DataPrepperExpressionParser parser = new DataPrepperExpressionParser(tokenStream);
        parser.addErrorListener(errorListener);

        final ParserRuleContext context = parser.expression();

        final ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(errorListener, context);

        return context;
    }
}
