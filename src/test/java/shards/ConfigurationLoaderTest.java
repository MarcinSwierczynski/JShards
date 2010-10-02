package shards;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import shards.QueryVisitor.ParameterInfoBean;

import com.google.common.collect.Sets;

public class ConfigurationLoaderTest {
	@Test
	public void load() throws ClassNotFoundException, IOException {
		ConfigurationLoader configLoader = new ConfigurationLoader();
		Configuration configuration = configLoader.load("src/test/resources/config.yml");
		
		List<String> drivers = configuration.getDrivers();
		assertEquals(1, drivers.size());
		assertEquals("org.postgresql.Driver", drivers.get(0));
		
		List<ConnectionInfo> connections = configuration.getConnections();
		assertEquals(2, connections.size());
		assertEquals("shard1", connections.get(0).getName());
		assertEquals("jdbc:postgresql://dev:5432/shards2", connections.get(1).getUrl());
		
		Map<String, StrategyInfo> strategies = configuration.getStrategy();
		assertEquals(1, strategies.size());
		assertEquals("shards", strategies.get("shards").getTable());
		assertEquals(Sets.newHashSet("shard1", "shard2"), strategies.get("shards").getStrategy().selectShards(new ParameterInfoBean()));
	}
	
}