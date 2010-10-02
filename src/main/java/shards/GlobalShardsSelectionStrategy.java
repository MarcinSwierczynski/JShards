package shards;

import java.util.Map;
import java.util.Set;

public class GlobalShardsSelectionStrategy implements ShardsSelectionStrategy, ConfigurationAware {
	private Configuration configuration;
	private ShardsSelectionStrategy mirroringStrategy;

	public GlobalShardsSelectionStrategy() {
		mirroringStrategy = new MirroringShardsSelectionStrategy();
	}

	public Set<String> selectShards(ParameterInfo param) {
		String paramTable = param.getTable();
		ShardsSelectionStrategy strategy = getStrategyForTable(paramTable);
		if (strategy != null) {
			return strategy.selectShards(param);
		} else { // global table - use mirroring
			return mirroringStrategy.selectShards(param);
		}
	}

	private ShardsSelectionStrategy getStrategyForTable(String tableName) {
		if(tableName == null) {
			return null;
		}
		Map<String, StrategyInfo> strategies = configuration.getStrategy();
		StrategyInfo strategyInfo = strategies.get(tableName);
		if (strategyInfo != null) {
			return strategyInfo.getStrategy();
		}
		return null;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
		StrategyInitializer strategyInitializer = new StrategyInitializer(configuration);
		strategyInitializer.init(mirroringStrategy);
	}
	
}
