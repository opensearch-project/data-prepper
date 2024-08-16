package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;

import javax.inject.Named;
import java.util.List;
import java.util.function.Function;

@Named
public class StartsWithExpressionFunction implements ExpressionFunction {
    private static final int NUMBER_OF_ARGS = 2;

    static final String STARTS_WITH_FUNCTION_NAME = "startsWith";
    @Override
    public String getFunctionName() {
        return STARTS_WITH_FUNCTION_NAME;
    }

    @Override
    public Object evaluate(
            final List<Object> args,
            final Event event,
            final Function<Object, Object> convertLiteralType) {

        if (args.size() != NUMBER_OF_ARGS) {
            throw new RuntimeException("startsWith() takes exactly two arguments");
        }

        String[] strArgs = new String[NUMBER_OF_ARGS];
        for (int i = 0; i < NUMBER_OF_ARGS; i++) {
            Object arg = args.get(i);
            if (!(arg instanceof String)) {
                throw new RuntimeException(String.format("startsWith() takes only string type arguments. \"%s\" is not of type string", arg));
            }
            String stringOrKey = (String) arg;
            if (stringOrKey.charAt(0) == '"') {
                strArgs[i] = stringOrKey.substring(1, stringOrKey.length()-1);
            } else if (stringOrKey.charAt(0) == '/') {
                Object obj = event.get(stringOrKey, Object.class);
                if (obj == null) {
                    return false;
                }
                if (!(obj instanceof String)) {
                    throw new RuntimeException(String.format("startsWith() only operates on string types. The value at \"%s\" is \"%s\" which is not a string type.", stringOrKey, obj));
                }
                strArgs[i] = (String)obj;
            } else {
                throw new RuntimeException(String.format("Arguments to startsWith() must be a literal string or a Json Pointer. \"%s\" is not string literal or json pointer", stringOrKey));
            }
        }
        return strArgs[0].startsWith(strArgs[1]);
    }
}
