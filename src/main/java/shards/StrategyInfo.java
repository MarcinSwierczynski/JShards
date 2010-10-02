package shards;

public class StrategyInfo {
	private final String table;
	private final ShardsSelectionStrategy strategy;
	
	public StrategyInfo(String table, ShardsSelectionStrategy strategy) {
		this.table = table;
		this.strategy = strategy;
	}

	public String getTable() {
		return table;
	}

	public ShardsSelectionStrategy getStrategy() {
		return strategy;
	}

}
