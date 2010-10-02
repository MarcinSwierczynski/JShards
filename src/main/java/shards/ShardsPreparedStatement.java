package shards;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Set;

import shards.ConnectionsHolder.ConnectionCallback;
import utils.ShardsIterables;

import com.google.common.collect.LinkedListMultimap;

public class ShardsPreparedStatement extends ShardsStatement implements PreparedStatement {

	private String sql;
	private PreparedStatementParametersList parameters;
	
	public ShardsPreparedStatement(ShardsConnection connection, String sql) throws SQLException {
		super(connection);
		this.sql = sql;
		parameters = new PreparedStatementParametersList();
		poolable = true;
	}
	
	private synchronized PreparedStatement createStatement(String shard, Connection connection, String rewrittenQuery) throws SQLException {
	    PreparedStatement preparedStatement = (PreparedStatement) statements.get(shard);
        if (preparedStatement == null) {
    		preparedStatement = internalCreateStatement(connection, rewrittenQuery);
    	    statements.put(shard, preparedStatement);
        }
		parameters.populate(preparedStatement);
		setStatementFields(preparedStatement);
	    return preparedStatement;
	}

    protected PreparedStatement internalCreateStatement(Connection connection, String rewrittenQuery) throws SQLException {
        return connection.prepareStatement(rewrittenQuery);
    }

	public void addBatch() throws SQLException {
	    ParseResult result = parser.parse(sql, globalStrategy, parameters);
	    Set<String> shardsUsedInQuery = result.getSelectedShards();
	    for (String shard : shardsUsedInQuery) {
            Connection connection = connections.getConnection(shard);
            String queryToExecute = result.getRewrittenQuery() != null ? result.getRewrittenQuery() : sql;
            PreparedStatement statement = createStatement(shard, connection, queryToExecute);
            statement.addBatch();
        }
	    batchExecutor.addQuery(shardsUsedInQuery);
	}

	public void clearParameters() throws SQLException {
	    this.parameters.clear();
	}

	public boolean execute() throws SQLException {
	    ParseResult parseResult = parser.parse(sql, globalStrategy, parameters);
        final String rewrittenQuery = parseResult.getRewrittenQuery();
        Set<String> shards = parseResult.getSelectedShards();
        return execute(sql, rewrittenQuery, parseResult, shards);
	}
	
	@Override
	protected boolean executeQueryOnSingleConnectionAndCollectResults(String queryToExecute, LinkedListMultimap<Integer, Result> results, String shard, Connection connection)
	        throws SQLException {
	    PreparedStatement statement = createStatement(shard, connection, queryToExecute);
        boolean resultSetReturned = statement.execute();
        collectResults(results, resultSetReturned, statement);
        return resultSetReturned;
	}

	public ResultSet executeQuery() throws SQLException {
	    ParseResult result = parser.parse(sql, globalStrategy, parameters);
        final String rewrittenQuery = result.getRewrittenQuery();
        Set<String> shards = result.getSelectedShards();

        List<ResultSet> selected = connections.foreach(shards, new ConnectionCallback<ResultSet>() {
            public ResultSet handle(String name, Connection connection) throws SQLException {
                return createStatement(name, connection, rewrittenQuery).executeQuery();
            }
        });
        return mergeResultSets(sql, result, selected);
	}

	public int executeUpdate() throws SQLException {
	    ParseResult result = parser.parse(sql, globalStrategy, parameters);
        Set<String> shards = result.getSelectedShards();

        List<Integer> updated = connections.foreach(shards, new ConnectionCallback<Integer>() {
            public Integer handle(String name, Connection connection) throws SQLException {
                return createStatement(name, connection, sql).executeUpdate();
            }
        });
        return ShardsIterables.sum(updated);
	}

	public ResultSetMetaData getMetaData() throws SQLException {
	    PreparedStatement preparedStatement = createPrepareStatementForFistShard();
        return preparedStatement.getMetaData();
	}

    private PreparedStatement createPrepareStatementForFistShard() throws SQLException {
        ParseResult result = parser.parse(sql, globalStrategy, parameters);
        Set<String> shards = result.getSelectedShards();
        String firstShard = shards.iterator().next();
        Connection firstConnection = connections.getConnection(firstShard);
        PreparedStatement preparedStatement = createStatement(firstShard, firstConnection, sql);
        return preparedStatement;
    }

	public ParameterMetaData getParameterMetaData() throws SQLException {
	    PreparedStatement preparedStatement = createPrepareStatementForFistShard();
	    return preparedStatement.getParameterMetaData();
	}

	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
	    setObject(parameterIndex, x, "setBigDecimal", BigDecimal.class);
	}

	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
	    // TODO Parser nie potrafi obsługiwać zapytań typu: SELECT * FROM tabela WHERE ? AND ?
	    setObject(parameterIndex, x, "setBoolean", boolean.class);
	}

	public void setByte(int parameterIndex, byte x) throws SQLException {
	    setObject(parameterIndex, x, "setByte", byte.class);
	}

	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setClob(int parameterIndex, Clob x) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setDate(int parameterIndex, Date x) throws SQLException {
		setObject(parameterIndex, x, "setDate", Date.class);
	}

	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
	    setObjects(parameterIndex, new Object[] { x, cal }, "setDate", new Class<?>[] { Date.class, Calendar.class });
	}

	public void setDouble(int parameterIndex, double x) throws SQLException {
		setObject(parameterIndex, x, "setDouble", double.class);
	}

	public void setFloat(int parameterIndex, float x) throws SQLException {
		setObject(parameterIndex, x, "setFloat", float.class);
	}

	public void setInt(int parameterIndex, int x) throws SQLException {
	    setObject(parameterIndex, x, "setInt", int.class);
	}

	public void setLong(int parameterIndex, long x) throws SQLException {
		setObject(parameterIndex, x, "setLong", long.class);
	}

	private void setObject(int parameterIndex, Object x, String methodName, Class<?> methodParamType) {
		PreparedStatementParameter preparedStatementParameter = new PreparedStatementParameter(methodName, x, methodParamType);
		parameters.addParameter(parameterIndex, preparedStatementParameter);
	}
	
	private void setObjects(int parameterIndex, Object[] params, String methodName, Class<?>[] methodParamTypes) {
	    PreparedStatementParameter preparedStatementParameter = new PreparedStatementParameter(methodName, params, methodParamTypes);
	    parameters.addParameter(parameterIndex, preparedStatementParameter);
	}

	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setNString(int parameterIndex, String value) throws SQLException {
		setObject(parameterIndex, value, "setNString", String.class);
	}

	public void setNull(int parameterIndex, int sqlType) throws SQLException {
	    setObject(parameterIndex, sqlType, "setNull", int.class);
	}

	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
	    // TODO Dla PostgreSQL ważne jest jaki jest typ dla null.
	    setObjects(parameterIndex, new Object[] { sqlType, typeName }, "setNull", new Class<?>[] { int.class, String.class });
	}

	public void setObject(int parameterIndex, Object x) throws SQLException {
		setObject(parameterIndex, x, "setObject", Object.class);
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObjects(parameterIndex, new Object[] { x, targetSqlType }, "setObject", new Class<?>[] { Object.class, int.class });
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObjects(parameterIndex, new Object[] { x, targetSqlType, scaleOrLength }, "setObject", new Class<?>[] { Object.class, int.class, int.class });
	}

	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}
	
	public void setShort(int parameterIndex, short x) throws SQLException {
		setObject(parameterIndex, x, "setShort", short.class);
	}

	public void setString(int parameterIndex, String x) throws SQLException {
	    setObject(parameterIndex, x, "setString", String.class);
	}

	public void setTime(int parameterIndex, Time x) throws SQLException {
	    setObject(parameterIndex, x, "setTime", Time.class);
	}

	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setObjects(parameterIndex, new Object[] { x, cal }, "setTime", new Class<?>[] { Time.class, Calendar.class });
	}

	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
	    setObject(parameterIndex, x, "setTimestamp", Timestamp.class);
	}

	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
	    setObjects(parameterIndex, new Object[] { x, cal }, "setTimestamp", new Class<?>[] { Timestamp.class, Calendar.class });
	}

	public void setURL(int parameterIndex, URL x) throws SQLException {
	    setObject(parameterIndex, x, "setURL", URL.class);
	}

	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException("This paramater support has not been implemented yet");
	}

}
