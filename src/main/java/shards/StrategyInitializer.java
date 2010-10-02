package shards;

public class StrategyInitializer {
	private Configuration configuration;

	public StrategyInitializer(Configuration configuration) {
		this.configuration = configuration;
	}
	
	public void init(ShardsSelectionStrategy strategy) {
		if(strategy instanceof ConfigurationAware) {
			ConfigurationAware configAware = (ConfigurationAware) strategy;
			configAware.setConfiguration(configuration);
		}
	}
}
