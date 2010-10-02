package shards;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * ShardsSelectionStrategy for testing
 */
public class DummyShardsSelectionStrategy implements ShardsSelectionStrategy {

	public Set<String> selectShards(ParameterInfo param) {
		return ImmutableSet.of("dummy");
	}

}
