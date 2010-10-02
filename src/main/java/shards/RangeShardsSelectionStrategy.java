package shards;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.*;

public class RangeShardsSelectionStrategy implements ShardsSelectionStrategy {
	private String column;
	private List<ShardRange> ranges = Lists.newArrayList();

	public void setColumn(String column) {
		this.column = column;
	}

	public void setRanges(List<Map<String, Object>> ranges) {
		for (Map<String, Object> range : ranges) {
			double from = getDouble(range, "from", Double.MIN_VALUE);
			double to = getDouble(range, "to", Double.MAX_VALUE);
			String shard = getString(range, "shard");
			addRange(from, to, shard);
		}
	}

	private String getString(Map<String, Object> range, String param) {
		Object value = range.get(param);
		if (value == null) {
			throw new ConfigurationProblemException("Parameter " + param + " has not been specified.");
		} else {
			return value.toString();
		}
	}

	private double getDouble(Map<String, Object> range, String param, double defaultValue) {
		Object value = range.get(param);
		if (value == null) {
			return defaultValue;
		} else {
			try {
				return ((Number) value).doubleValue();
			} catch (ClassCastException e) {
				throw new ConfigurationProblemException("Parameter " + param + " is not a number: " + value, e);
			}
		}
	}

	public Set<String> selectShards(ParameterInfo param) {
		if (column.equals(param.getColumn())) {
			if (Type.NUMBER.equals(param.getType())) {
				double value = param.getValueAsDouble();
				Set<String> selectedShards = Sets.newHashSet();
				for (ShardRange range : ranges) {
					if (range.includeElements(value, param.getOperator())) {
						selectedShards.add(range.shard);
					}
				}
				if (selectedShards.size() == 0)
					throw new WrongShardsQueryException("Given value " + value + " is out of range");
				return selectedShards;
			} else {
				throw new WrongShardsQueryException("Given value " + param.getValue() + " has to be a number");
			}
		}
		return allShards();
	}

	// TODO Cacheowac wynik tej metody - tylko raz ma byc tworzone
	private Set<String> allShards() {
		Iterable<String> allShards = Iterables.transform(ranges, new Function<ShardRange, String>() {
			public String apply(ShardRange range) {
				return range.shard;
			}
		});

		return Sets.newHashSet(allShards);
	}

	public void addRange(double from, double to, String shard) {
		ShardRange range = new ShardRange(from, to, shard);
		ranges.add(range);
	}

	class ShardRange {
		private final double from;
		private final double to;
		private String shard;

		public ShardRange(double from, double to, String shard) {
			this.from = from;
			this.to = to;
			this.shard = shard;
		}
		
		public boolean isWithinRange(double value) {
			return from <= value && value <= to;
		}
		
		public boolean includeElements(double value, Operator operator) {
			switch (operator) {
			case EQUAL:
				return isWithinRange(value);
			case NOT_EQUAL:
				return !(from == value && to == value);
			case LESS_EQUAL:
				return from <= value;
			case LESS:
				return from < value;
			case MORE_EQUAL:
				return to >= value;
			case MORE:
				return to > value;
			}
			return true;
		}
	}

}
