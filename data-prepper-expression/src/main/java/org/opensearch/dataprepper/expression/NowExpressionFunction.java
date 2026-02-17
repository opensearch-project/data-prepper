package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;

import javax.inject.Named;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;

@Named
public class NowExpressionFunction implements ExpressionFunction {

    static final String NOW_FUNCTION_NAME = "now";

    @Override
    public String getFunctionName() {
        return NOW_FUNCTION_NAME;
    }

    @Override
    public Object evaluate(final List<Object> args, final Event event, final Function<Object, Object> convertLiteralType) {
        if (!args.isEmpty()) {
            throw new RuntimeException("now() does not take any arguments");
        }
        return Instant.now().toEpochMilli();
    }
}
