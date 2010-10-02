package shards;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Configuration {

	List<String> getDrivers();
	List<ConnectionInfo> getConnections();
	Set<String> getShardsNames();
	Map<String, StrategyInfo> getStrategy();

}