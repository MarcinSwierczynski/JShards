package shards;

import utils.Assert;
import utils.Calculation;

public enum FunctionType {

	SUM, MIN, MAX, AVG, COUNT;

	private Calculation calculation = new Calculation();

	public static FunctionType fromValue(String name) {
		Assert.notNull(name);
		for (FunctionType type : values()) {
			if (name.toUpperCase().equals(type.name())) {
				return type;
			}
		}
		return null;
	}

	public Object aggregate(Object val1, Object val2) {
		switch (this) {
		case SUM:
			return calculation.add(val1, val2);
		case MIN:
			Comparable c1 = (Comparable) val1;
			Comparable c2 = (Comparable) val2;
			int result = c1.compareTo(c2);
			return result <= 0 ? c1 : c2;
		case MAX:
			c1 = (Comparable) val1;
			c2 = (Comparable) val2;
			result = c1.compareTo(c2);
			return result >= 0 ? c1 : c2;
		case COUNT:
			return calculation.add(val1, val2);
		}
		throw new WrongShardsQueryException(String.format("Function %s(%s,%s) could not be run", this, val1, val2));
	}

}
