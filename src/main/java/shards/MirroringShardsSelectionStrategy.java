package shards;

import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class MirroringShardsSelectionStrategy implements ShardsSelectionStrategy, ConfigurationAware {
	private Set<String> allShards = Sets.newHashSet();
	private Random random = new Random();
	
	public Set<String> selectShards(ParameterInfo param) {
		if(allShards.size() == 0) {
			throw new ConfigurationProblemException("No shards");
		}
		if(param.getOperation() == Operation.SELECT) { 
			String shard = chooseRandomShard();
			return ImmutableSet.of(shard);
		} else { 
			return allShards;
		}
	}

	private String chooseRandomShard() {
		int count = allShards.size();
		int shardNo = random.nextInt(count); 
		String shard = Iterables.get(allShards, shardNo);
		return shard;
	}

	public void setConfiguration(Configuration configuration) {
		this.allShards = configuration.getShardsNames();
	}

}