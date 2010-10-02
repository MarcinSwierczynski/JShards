package shards;

import java.util.Date;

public enum Type {
	STRING, NUMBER, DATE;

	public static Type fromValue(Object value) {
		if (value instanceof Number) {
			return NUMBER;
		} else if (value instanceof Date) {
			return DATE;
		} else if (value instanceof String) {
			return STRING;
		} else {
			return null;
		}
	}
}
