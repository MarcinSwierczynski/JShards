package shards;

import java.io.*;
import java.util.*;

import org.yaml.snakeyaml.*;
import org.yaml.snakeyaml.constructor.Constructor;

import com.google.common.collect.Lists;

public class ConfigurationLoader {
	public Configuration load(String yamlFilePath) throws IOException, ClassNotFoundException {
		//TODO prevent selecting more then one strategy for single table - now it will use the first one
		Yaml yaml = prepareYaml();
		FileReader io = new FileReader(yamlFilePath);
		ConfigurationBean configuration = (ConfigurationBean) yaml.load(io);

		List<StrategyInfo> strategies = getStrategiesList(yaml, configuration);
		
		ConfigurationTO confTO = new ConfigurationTO();
		confTO.setDrivers(configuration.getDrivers());
		confTO.setStrategy(strategies);
		confTO.setConnections(configuration.getConnections());
		
		initStrategies(strategies, confTO);
		return confTO;
	}

	private void initStrategies(List<StrategyInfo> strategies, Configuration config) {
		StrategyInitializer initializer = new StrategyInitializer(config);
		for (StrategyInfo strategyInfo : strategies) {
			initializer.init(strategyInfo.getStrategy());
		}
	}

	private Yaml prepareYaml() {
		Constructor constructor = prepareConstructor();
		Loader loader = new Loader(constructor);
		Yaml yaml = new Yaml(loader);
		return yaml;
	}

	private Constructor prepareConstructor() {
		Constructor constructor = new Constructor(ConfigurationBean.class);
		TypeDescription configurationDescription = new TypeDescription(ConfigurationBean.class);
		configurationDescription.putListPropertyType("connections", ConnectionInfo.class);
		constructor.addTypeDescription(configurationDescription);
		return constructor;
	}

	private List<StrategyInfo> getStrategiesList(Yaml yaml, ConfigurationBean configuration) throws ClassNotFoundException {
		List<StrategyInfo> strategiesList = Lists.newArrayList();
		
		List<Map<String, Object>> strategies = configuration.getStrategy();
		if (strategies != null) {
			for (int i = 0; i < strategies.size(); i++) {
				Map<String, Object> strategy = strategies.get(i);
				
				String table = strategy.get("table").toString();
				ShardsSelectionStrategy strategyObject = getStrategyObject(yaml, strategy);
				
				StrategyInfo strategyInfo = new StrategyInfo(table, strategyObject);
				strategiesList.add(strategyInfo);
			}
		}
		
		return strategiesList;
	}

	private ShardsSelectionStrategy getStrategyObject(Yaml yaml, Map<String, Object> strategy) throws ClassNotFoundException {
		String strategyParams = getStrategyParams(yaml, strategy);
		String strategyClassName = (String) strategy.get("class");
		Loader loader = new Loader(new Constructor(Class.forName(strategyClassName)));
		Yaml strategyParamsYaml = new Yaml(loader);
		return (ShardsSelectionStrategy) strategyParamsYaml.load(new StringReader(strategyParams));
	}

	private String getStrategyParams(Yaml yaml, Map<String, Object> strategy) {
		StringWriter writer = new StringWriter();
		Object params = strategy.get("params");
		if(params != null) {
			yaml.dump(params, writer);
		}
		return writer.toString();
	}
}
