package shards;

import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public abstract class HashShardsSelectionStrategy implements ShardsSelectionStrategy, ConfigurationAware {

	protected LinkedHashSet<String> allShards;
	protected String column;

	public void setColumn(String column) {
		this.column = column;
	}

	public Set<String> selectShards(ParameterInfo param) {
		if (column.equals(param.getColumn())) {
			Object value = param.getValue();
			String selectedShard = calculate(value);
			return ImmutableSet.of(selectedShard);
		} else {
			return allShards;
		}
	}

	public abstract String calculate(Object value);

	public void setConfiguration(Configuration configuration) {
		allShards = Sets.newLinkedHashSet(configuration.getShardsNames());
		if (column == null) {
			throw new ConfigurationProblemException("Column not specified in " + getClass().getName());
		}
	}

}
