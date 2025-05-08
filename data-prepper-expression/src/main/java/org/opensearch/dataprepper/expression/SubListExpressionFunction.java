package org.opensearch.dataprepper.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.opensearch.dataprepper.model.event.Event;

import javax.inject.Named;

@Named
public class SubListExpressionFunction implements ExpressionFunction {

	@Override
	public String getFunctionName() {
		return "subList";
	}

	@Override
	public Object evaluate(List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
		if (args.size() != 3) {
			throw new IllegalArgumentException("subList() takes 3 arguments");
		}
		if (!(args.get(0) instanceof String)) {
			throw new IllegalArgumentException("subList() takes 1st argument as string type");
		}
		int startIndex, endIndex;
		try {
			if (args.get(1) instanceof Integer) {
				startIndex = (Integer) args.get(1);
			} else {
				String str = (String) args.get(1);
				startIndex = Integer.parseInt(str.substring(1, str.length() - 1));
			}
			if (args.get(2) instanceof Integer) {
				endIndex = (Integer) args.get(2);
			} else {
				String str = (String) args.get(2);
				endIndex = Integer.parseInt(str.substring(1, str.length() - 1));
			}
		} catch (NumberFormatException | ClassCastException e) {
			throw new IllegalArgumentException("subList() takes 2nd and 3rd arguments as integers");
		}

		String key = (String)args.get(0);
		final Object value = event.get(key, Object.class);
		if (value == null) return null;
		if (!(value instanceof List)) {
			throw new RuntimeException(key + " is not of list type");
		}
		List<?> sourceList = (List<?>)value;

		if (endIndex == -1) endIndex = sourceList.size();

		if (startIndex < 0 || startIndex >= sourceList.size()) {
			throw new RuntimeException("subList() start index should be between 0 and list length (inclusive)");
		}
		
		if (endIndex < 0 || endIndex > sourceList.size()) {
			throw new RuntimeException("subList() end index should be between 0 and list length or -1 for list length (exclusive)");
		}
		if (startIndex > endIndex) {
			throw new RuntimeException("subList() start index should be less than or equal to end index");
		}
		return new ArrayList<>(sourceList.subList(startIndex, endIndex));
	}

}
