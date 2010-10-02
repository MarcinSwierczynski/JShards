package shards;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import utils.Assert;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ConnectionsHolder {

	private Map<String, Connection> connections = Maps.newLinkedHashMap();

	public ConnectionsHolder() {
	}

	public void add(String name, Connection connection) {
		connections.put(name, connection);
	}

	public Connection firstConnection() {
		Assert.isTrue(connections.size() > 0);
		return connections.values().iterator().next();
	}

	public <T> List<T> foreach(ConnectionCallback<T> callback) throws SQLException {
		List<T> results = Lists.newArrayList();
		for (String name : connections.keySet()) {
			results.add(callback.handle(name, connections.get(name)));
		}
		return results;
	}

	public <T> List<T> foreach(Set<String> shards, ConnectionCallback<T> callback) throws SQLException {
		List<T> results = Lists.newArrayList();
		for (String name : connections.keySet()) {
			if(shards.contains(name)) {
				results.add(callback.handle(name, connections.get(name)));
			}
		}
		return results;
	}
	
	public List<Connection> getConnections(Set<String> shards) {
		List<Connection> connections = Lists.newArrayList();
		
		for (String connectionName : this.connections.keySet()) {
			if(shards.contains(connectionName)) {
				connections.add(this.connections.get(connectionName));
			}
		}
		
		return connections;
	}
	
	public Connection getConnection(String shard) {
	    return connections.get(shard);
	}
	
	public static interface ConnectionCallback<T> {
		T handle(String name, Connection connection) throws SQLException;
	}

	public Collection<Connection> getAllConnections() {
		return connections.values();
	}

}
