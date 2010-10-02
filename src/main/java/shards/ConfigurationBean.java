package shards;

import java.util.*;

public class ConfigurationBean {
	private List<String> drivers;
	private List<ConnectionInfo> connections;
	private List<Map<String,Object>> strategy;

	public List<String> getDrivers() {
		return drivers;
	}

	public void setDrivers(List<String> drivers) {
		this.drivers = drivers;
	}

	public List<ConnectionInfo> getConnections() {
		return connections;
	}

	public void setConnections(List<ConnectionInfo> connections) {
		this.connections = connections;
	}

	public List<Map<String, Object>> getStrategy() {
		return strategy;
	}

	public void setStrategy(List<Map<String, Object>> strategy) {
		this.strategy = strategy;
	}
	
}
