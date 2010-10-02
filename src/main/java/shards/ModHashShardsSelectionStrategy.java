package shards;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Strategy using MOD function
 */
public class ModHashShardsSelectionStrategy extends HashShardsSelectionStrategy {

	@Override
	public String calculate(Object value) {
		if (value != null) {
			if (value instanceof Long) {
				value = ((Long) value).doubleValue();
			}
			int size = allShards.size();
			int hashCode = ("" + value).hashCode();
			int no = hashCode % size;
			String shard = Iterables.get(allShards, no);
			return shard;
		} else {
			return allShards.iterator().next();
		}
	}
	
	public static void main(String[] args) {
	    final ModHashShardsSelectionStrategy modHashShardsSelectionStrategy = new ModHashShardsSelectionStrategy();
	    modHashShardsSelectionStrategy.setColumn("asd");
	    modHashShardsSelectionStrategy.setConfiguration(new Configuration() {
			
			public Map<String, StrategyInfo> getStrategy() {
				return null;
			}
			
			public Set<String> getShardsNames() {
				return ImmutableSet.of("s1", "s2");
			}
			
			public List<String> getDrivers() {
				return Lists.newArrayList("", "");
			}
			
			public List<ConnectionInfo> getConnections() {
				return Lists.newArrayList();
			}
		});
		final String c = modHashShardsSelectionStrategy.calculate(6l);
		System.out.println(c);
    }
	
}
