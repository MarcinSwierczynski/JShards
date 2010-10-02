package shards;

import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ConfigurationTO implements Configuration {
	private List<ConnectionInfo> connections = Lists.newArrayList();
	private List<String> drivers = Lists.newArrayList();
	private Map<String, StrategyInfo> strategy = Maps.newHashMap();

	public void setDrivers(List<String> drivers) {
		this.drivers = drivers;
	}

	public void setConnections(List<ConnectionInfo> connections) {
		this.connections = connections;
	}

	public void setStrategy(List<StrategyInfo> strategy) {
		for (StrategyInfo strategyInfo : strategy) {
			this.strategy.put(strategyInfo.getTable(), strategyInfo);
		}
	}

	public List<ConnectionInfo> getConnections() {
		return connections;
	}

	public List<String> getDrivers() {
		return drivers;
	}

	public Map<String, StrategyInfo> getStrategy() {
		return strategy;
	}

	public Set<String> getShardsNames() {
		Set<String> shardsNames = Sets.newLinkedHashSet();
		List<ConnectionInfo> connections = getConnections();
		for (ConnectionInfo connectionInfo : connections) {
			String shardsName = connectionInfo.getName();
			shardsNames.add(shardsName);
		}
		return shardsNames;
	}

}
