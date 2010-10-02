package shards;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import shards.ConnectionsHolder.ConnectionCallback;
import utils.Assert;

public class ShardsConnection implements Connection {

    private ConnectionsHolder connections;

    private Connection firstConnection;

    private Configuration configuration;

    public ShardsConnection(ConnectionsHolder connections, Configuration configuration) {
        Assert.notNull(connections);
        this.connections = connections;
        this.firstConnection = connections.firstConnection();
        this.configuration = configuration;
    }
    
    public ConnectionsHolder getConnections() {
        return connections;
    }
    
    public Configuration getConfiguration() {
        return configuration;
    }
    
    public void clearWarnings() throws SQLException {
        connections.foreach(new ConnectionCallback<Object>() {
			public Object handle(String name, Connection connection) throws SQLException {
				connection.clearWarnings();
				return null;
			}
        });
    }

    public void close() throws SQLException {
        connections.foreach(new ConnectionCallback<Object>() {
            public Object handle(String name, Connection connection) throws SQLException {
                connection.close();
                return null;
            }
        });
    }

    public void commit() throws SQLException {
        connections.foreach(new ConnectionCallback<Object>() {
            public Object handle(String name, Connection connection) throws SQLException {
                connection.commit();
                return null;
            }
        });
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return firstConnection.createArrayOf(typeName, elements);
    }

    public Blob createBlob() throws SQLException {
        return firstConnection.createBlob();
    }

    public Clob createClob() throws SQLException {
        return firstConnection.createClob();
    }

    public NClob createNClob() throws SQLException {
        return firstConnection.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return firstConnection.createSQLXML();
    }

    public Statement createStatement() throws SQLException {
        return new ShardsStatement(this);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new ShardsStatementHoldability(this, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new ShardsStatementConcurrency(this, resultSetType, resultSetConcurrency);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return firstConnection.createStruct(typeName, attributes);
    }

    public boolean getAutoCommit() throws SQLException {
        return firstConnection.getAutoCommit();
    }

    public String getCatalog() throws SQLException {
        return firstConnection.getCatalog();
    }

    public Properties getClientInfo() throws SQLException {
        return firstConnection.getClientInfo();
    }

    public String getClientInfo(String name) throws SQLException {
        return firstConnection.getClientInfo(name);
    }

    public int getHoldability() throws SQLException {
        return firstConnection.getHoldability();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return firstConnection.getMetaData();
    }

    public int getTransactionIsolation() throws SQLException {
        return firstConnection.getTransactionIsolation();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return firstConnection.getTypeMap();
    }

    public SQLWarning getWarnings() throws SQLException {
        Collection<Connection> allConnections = connections.getAllConnections();
        SQLWarning allWarnings = null;
        for (Connection connection : allConnections) {
			SQLWarning warnings = connection.getWarnings();
			if(warnings != null) {
				if(allWarnings != null) {
					allWarnings.setNextWarning(warnings);
				} else {
					allWarnings = warnings;
				}
			}
		}
        return allWarnings;
    }

    public boolean isClosed() throws SQLException {
        return firstConnection.isClosed();
    }

    public boolean isReadOnly() throws SQLException {
        return firstConnection.isReadOnly();
    }

    public boolean isValid(int timeout) throws SQLException {
        return firstConnection.isValid(timeout);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return firstConnection.isWrapperFor(iface);
    }

    public String nativeSQL(String sql) throws SQLException {
        return firstConnection.nativeSQL(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return firstConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return firstConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return firstConnection.prepareCall(sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new ShardsPreparedStatementHoldability(this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new ShardsPreparedStatementConcurrency(this, sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new ShardsPreparedStatementAutoGeneratedKeys(this, sql, autoGeneratedKeys);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new ShardsPreparedStatementColumnIndexes(this, sql, columnIndexes);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new ShardsPreparedStatementColumnNames(this, sql, columnNames);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new ShardsPreparedStatement(this, sql);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        firstConnection.releaseSavepoint(savepoint);
    }

    public void rollback() throws SQLException {
        connections.foreach(new ConnectionCallback<Object>() {
            public Object handle(String name, Connection connection) throws SQLException {
                connection.rollback();
                return null;
            }
        });
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        firstConnection.rollback(savepoint);
    }

    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        connections.foreach(new ConnectionCallback<Object>() {
            public Object handle(String name, Connection connection) throws SQLException {
                connection.setAutoCommit(autoCommit);
                return null;
            }
        });
    }

    public void setCatalog(String catalog) throws SQLException {
        firstConnection.setCatalog(catalog);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        firstConnection.setClientInfo(properties);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        firstConnection.setClientInfo(name, value);
    }

    public void setHoldability(int holdability) throws SQLException {
        firstConnection.setHoldability(holdability);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        firstConnection.setReadOnly(readOnly);
    }

    public Savepoint setSavepoint() throws SQLException {
        return firstConnection.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return firstConnection.setSavepoint(name);
    }

    public void setTransactionIsolation(int level) throws SQLException {
        firstConnection.setTransactionIsolation(level);
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        firstConnection.setTypeMap(map);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return firstConnection.unwrap(iface);
    }

}
