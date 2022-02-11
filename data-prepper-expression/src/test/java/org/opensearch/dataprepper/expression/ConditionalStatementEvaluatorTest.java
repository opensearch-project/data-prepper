package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class ConditionalStatementEvaluatorTest {

    @Mock
    private Parser<ParseTree> parser;
    @Mock
    private Evaluator<ParseTree, Event> evaluator;
    @InjectMocks
    private ConditionalStatementEvaluator statementEvaluator;

    @Test
    void testThrowsNotImplementedException() {
        assertThrows(
                RuntimeException.class,
                () -> statementEvaluator.evaluate(null, null));
    }
}