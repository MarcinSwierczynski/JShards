package shards;

import java.util.Set;

public interface ShardsSelectionStrategy {
	
	Set<String> selectShards(ParameterInfo param);

}
