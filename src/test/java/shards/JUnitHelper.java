package shards;

import java.util.List;

import com.google.common.collect.Lists;

public class JUnitHelper {

	public static Configuration mockConfiguration() {
		ConfigurationTO config = new ConfigurationTO();

		ConnectionInfo connInfo = new ConnectionInfo();
		connInfo.setName("shard1");
		ConnectionInfo connInfo2 = new ConnectionInfo();
		connInfo2.setName("shard2");
		List<ConnectionInfo> connections = Lists.newArrayList(connInfo, connInfo2);

		config.setConnections(connections);

		return config;
	}

}
