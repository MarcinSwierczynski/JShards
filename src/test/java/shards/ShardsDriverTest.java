package shards;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ShardsDriverTest {

    private Connection connection;
    
    @Before
    public void setUp() throws ClassNotFoundException, SQLException {
    	connection = createConnection();
    	connection.setAutoCommit(false);
    }
    
    @After
    public void tearDown() throws SQLException {
        if (connection != null) {
            connection.rollback();
            connection.close();
        }
    }
    
    private Connection createConnection() throws ClassNotFoundException, SQLException {
        Class.forName(ShardsDriver.class.getName());
        String url = "jdbc:shards:src/test/resources/config.yml";
        Connection connection = DriverManager.getConnection(url, "shards", "shards");
        assertNotNull(connection);
        assertTrue(connection instanceof ShardsConnection);
        connection.setAutoCommit(false);
        return connection;
    }
    
    private void prepareDatabase() throws SQLException {
        Statement statement = connection.createStatement();
        try {
            statement.executeUpdate("DELETE FROM shards");
            statement.executeUpdate("INSERT INTO shards (id,value) VALUES (1, 'test')");
            statement.executeUpdate("INSERT INTO shards (id,value) VALUES (2, 'atest')");
            statement.executeUpdate("INSERT INTO shards (id,value) VALUES (3, 'test')");
            statement.executeUpdate("INSERT INTO shards (id,value) VALUES (4, 'atest')");
            statement.executeUpdate("INSERT INTO shards (id,value) VALUES (5, 'test')");
            statement.executeUpdate("INSERT INTO shards (id,value) VALUES (6, 'test')");
        } finally {
        statement.close();
        }
    }
    
	@Test
	public void sanityTests() throws ClassNotFoundException, SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate("DELETE FROM shards");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (1, 'test')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (2, 'atest')");
		ResultSet resultSet = statement.executeQuery("SELECT * FROM shards ORDER BY value");
		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getInt(1));
		assertEquals("atest", resultSet.getString(2));
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		assertEquals("test", resultSet.getString(2));
		assertEquals(2, resultSet.getMetaData().getColumnCount());
		assertFalse(resultSet.next());
	}
	
	@Test
	public void multipleAsterisks() throws ClassNotFoundException, SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate("DELETE FROM shards");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (1, 'test')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (2, 'atest')");
		ResultSet resultSet = statement.executeQuery("SELECT *, *, value FROM shards ORDER BY id");
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		assertEquals("test", resultSet.getString(2));
		assertEquals(1, resultSet.getInt(3));
		assertEquals("test", resultSet.getString(4));
		assertEquals("test", resultSet.getString(5));
		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getInt(1));
		assertEquals("atest", resultSet.getString(2));
		assertEquals(2, resultSet.getInt(3));
		assertEquals("atest", resultSet.getString(4));
		assertEquals("atest", resultSet.getString(5));
		assertEquals(5, resultSet.getMetaData().getColumnCount());
		assertFalse(resultSet.next());
	}
	
	@Test
	public void asterisksWithAvg() throws ClassNotFoundException, SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate("DELETE FROM shards");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (1, 'test')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (2, 'atest')");
		ResultSet resultSet = statement.executeQuery("SELECT *, avg(id) FROM shards GROUP BY id, value ORDER BY id");
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		assertEquals("test", resultSet.getString(2));
		assertEquals(1d, resultSet.getDouble(3), 0);
		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getInt(1));
		assertEquals("atest", resultSet.getString(2));
		assertEquals(2d, resultSet.getDouble(3), 0);
		assertEquals(3, resultSet.getMetaData().getColumnCount());
		assertFalse(resultSet.next());
	}
	
	@Test
	public void groupBy() throws ClassNotFoundException, SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate("DELETE FROM shards");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (1, 'test')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (2, 'atest')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (3, 'test')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (4, 'atest')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (5, 'atest')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (6, 'test')");
		ResultSet resultSet = statement.executeQuery("SELECT avg(id) as a, count(value), avg(id), sum(id) FROM shards GROUP BY value ORDER BY value");

		assertTrue(resultSet.next());
		assertEquals(11.0/3, resultSet.getDouble(1), 0.01);
		assertEquals(3, resultSet.getInt(2));
		assertEquals(11.0/3, resultSet.getDouble(3), 0.01);
		assertEquals(11, resultSet.getInt(4));
		assertEquals(4, resultSet.getMetaData().getColumnCount());
		assertTrue(resultSet.next());
		assertEquals(10.0/3, resultSet.getDouble(1), 0.01);
		assertEquals(3, resultSet.getInt(2));
		assertEquals(10.0/3, resultSet.getDouble(3), 0.01);
		assertEquals(10, resultSet.getInt(4));
		assertFalse(resultSet.next());
	}
	
	@Test
	public void orderByIndex() throws ClassNotFoundException, SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate("DELETE FROM shards");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (1, 'test')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (2, 'atest')");
		ResultSet resultSet = statement.executeQuery("SELECT id, value FROM shards ORDER BY 2");
		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getInt(1));
		assertEquals("atest", resultSet.getString(2));
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		assertEquals("test", resultSet.getString(2));
		assertFalse(resultSet.next());
	}
	
	@Test
	public void orderByDesc() throws ClassNotFoundException, SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate("DELETE FROM shards");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (1, 'test')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (2, 'atest')");
		ResultSet resultSet = statement.executeQuery("SELECT id, value FROM shards ORDER BY value DESC");
		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getMetaData().getColumnCount());
		assertEquals(1, resultSet.getInt(1));
		assertEquals("test", resultSet.getString(2));
		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getInt(1));
		assertEquals("atest", resultSet.getString(2));
		assertFalse(resultSet.next());
	}
	
	@Test
	public void groupByIndex() throws ClassNotFoundException, SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate("DELETE FROM shards");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (1, 'test')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (2, 'atest')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (3, 'test')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (4, 'atest')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (5, 'atest')");
		statement.executeUpdate("INSERT INTO shards (id,value) VALUES (6, 'test')");
		ResultSet resultSet = statement.executeQuery("SELECT avg(id) as a, count(value), avg(id), sum(id), value FROM shards GROUP BY 5 ORDER BY 5");

		assertTrue(resultSet.next());
		assertEquals(11.0/3, resultSet.getDouble(1), 0.01);
		assertEquals(3, resultSet.getInt(2));
		assertEquals(11.0/3, resultSet.getDouble(3), 0.01);
		assertEquals(11, resultSet.getInt(4));
		assertEquals(5, resultSet.getMetaData().getColumnCount());
		assertTrue(resultSet.next());
		assertEquals(10.0/3, resultSet.getDouble(1), 0.01);
		assertEquals(3, resultSet.getInt(2));
		assertEquals(10.0/3, resultSet.getDouble(3), 0.01);
		assertEquals(10, resultSet.getInt(4));
		assertFalse(resultSet.next());
	}
	
	@Test
	public void having() throws SQLException, ClassNotFoundException {
	    prepareDatabase();
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("SELECT count(id) FROM shards GROUP BY value HAVING count(id) < 3");

		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getInt(1));
		assertFalse(resultSet.next());
		
		resultSet = statement.executeQuery("SELECT count(id) FROM shards GROUP BY value HAVING count(id) <= 2");

		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getInt(1));
		assertFalse(resultSet.next());
		
		resultSet = statement.executeQuery("SELECT count(id) FROM shards GROUP BY value HAVING count(id) > 3");

		assertTrue(resultSet.next());
		assertEquals(4, resultSet.getInt(1));
		assertFalse(resultSet.next());
		
		resultSet = statement.executeQuery("SELECT count(id) FROM shards GROUP BY value HAVING count(id) >= 4");

		assertTrue(resultSet.next());
		assertEquals(4, resultSet.getInt(1));
		assertFalse(resultSet.next());
		
		resultSet = statement.executeQuery("SELECT count(id) FROM shards GROUP BY value HAVING count(id) <> 2");

		assertTrue(resultSet.next());
		assertEquals(4, resultSet.getInt(1));
		assertFalse(resultSet.next());
		
		resultSet = statement.executeQuery("SELECT count(id) FROM shards GROUP BY value HAVING count(id) = 4");

		assertTrue(resultSet.next());
		assertEquals(4, resultSet.getInt(1));
		assertFalse(resultSet.next());
	}
	
	@Test
	public void like() throws SQLException, ClassNotFoundException {
		prepareDatabase();
	    Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("SELECT count(id) FROM shards GROUP BY value HAVING value like '?e%'");

		assertTrue(resultSet.next());
		assertEquals(4, resultSet.getInt(1));
		assertFalse(resultSet.next());			
	}
	
	@Test
	public void havingIn() throws SQLException, ClassNotFoundException {
	    prepareDatabase();
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("SELECT count(id) FROM shards GROUP BY id HAVING id IN (2,3,4,5)");

		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		assertTrue(resultSet.next());
		assertTrue(resultSet.next());
		assertTrue(resultSet.next());
		assertFalse(resultSet.next());
		
		resultSet = statement.executeQuery("SELECT count(id) FROM shards GROUP BY id HAVING id NOT IN (2,3,4,5)");

		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		assertTrue(resultSet.next());
		assertFalse(resultSet.next());
	}
	
	@Test
	public void jdbcParameterExecuteQuery() throws SQLException, ClassNotFoundException {
	    prepareDatabase();
		
		PreparedStatement prepareStatement = connection.prepareStatement("SELECT count(id) FROM shards WHERE id = ?");
		prepareStatement.setLong(1, 1l);
		ResultSet resultSet = prepareStatement.executeQuery();
		
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
	}
	
	@Test
	public void jdbcParameterExecuteUpdate() throws ClassNotFoundException, SQLException {
	    prepareDatabase();
        
        PreparedStatement prepareStatement = connection.prepareStatement("UPDATE shards SET value = ? WHERE id = ?");
        prepareStatement.setString(1, "changed");
        prepareStatement.setLong(2, 3l);
        
        int count = prepareStatement.executeUpdate();
        
        assertEquals(1, count);

        prepareStatement = connection.prepareStatement("SELECT * FROM shards WHERE value = ?");
        prepareStatement.setString(1, "changed");

        ResultSet resultSet = prepareStatement.executeQuery();
        
        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(1));
        assertEquals("changed", resultSet.getString(2));
        assertFalse(resultSet.next());
	}
	
    @Test
    public void jdbcParameterExecute() throws SQLException, ClassNotFoundException {
        prepareDatabase();
        
        PreparedStatement prepareStatement = connection.prepareStatement("SELECT count(id) FROM shards WHERE id = ?");
        prepareStatement.setLong(1, 1l);
        boolean resultSetReturned = prepareStatement.execute();
        assertTrue(resultSetReturned);
        ResultSet resultSet = prepareStatement.getResultSet();
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
        assertFalse(resultSet.next());
        assertFalse(prepareStatement.getMoreResults());
    }
    
    @Test
    public void jdbcParameterExecuteWithArithmeticOperation() throws SQLException, ClassNotFoundException {
        prepareDatabase();
        
        PreparedStatement prepareStatement = connection.prepareStatement("SELECT id FROM shards WHERE id = ? + 5");
        prepareStatement.setInt(1, 1);
        boolean resultSetReturned = prepareStatement.execute();
        assertTrue(resultSetReturned);
        ResultSet resultSet = prepareStatement.getResultSet();
        assertTrue(resultSet.next());
        assertEquals(6, resultSet.getInt(1));
        assertFalse(resultSet.next());
        assertFalse(prepareStatement.getMoreResults());
    }
    
    @Test
    public void preparedStatementMetaData() throws SQLException {
        PreparedStatement prepareStatement = connection.prepareStatement("SELECT id FROM shards WHERE id = ?");
        prepareStatement.setInt(1, 1);
        ResultSetMetaData metaData = prepareStatement.getMetaData();
        assertEquals("id", metaData.getColumnName(1));
        assertEquals(Types.BIGINT, metaData.getColumnType(1));
    }
    
    @Test
    public void preparedStatementParameterMetaData() throws SQLException {
        PreparedStatement prepareStatement = connection.prepareStatement("SELECT id FROM shards WHERE id = ?");
        prepareStatement.setInt(1, 1);
        ParameterMetaData metaData = prepareStatement.getParameterMetaData();
        assertEquals(Types.INTEGER, metaData.getParameterType(1));
    }
    
    @Test
    public void maxRows() throws SQLException {
    	prepareDatabase();
    	Statement statement = connection.createStatement();
    	statement.setMaxRows(3);
		ResultSet resultSet = statement.executeQuery("SELECT * FROM shards ORDER BY id DESC");
		
		assertTrue(resultSet.next());
		assertEquals(6, resultSet.getInt(1));
		
		assertTrue(resultSet.next());
		assertEquals(5, resultSet.getInt(1));
		
		assertTrue(resultSet.next());
		assertEquals(4, resultSet.getInt(1));
		
		assertFalse(resultSet.next());
    }
    
    @Test
    public void maxRowsWithSqlLimit() throws SQLException {
    	prepareDatabase();
    	Statement statement = connection.createStatement();
    	statement.setMaxRows(3);
		ResultSet resultSet = statement.executeQuery("SELECT * FROM shards ORDER BY id DESC LIMIT 1");
		
		assertTrue(resultSet.next());
		assertEquals(6, resultSet.getInt(1));
		
		assertFalse(resultSet.next());
    }
    
    @Test
    public void maxRowsWithSqlLimitAndOffset() throws SQLException {
    	prepareDatabase();
    	Statement statement = connection.createStatement();
    	statement.setMaxRows(3);
		ResultSet resultSet = statement.executeQuery("SELECT * FROM shards ORDER BY id DESC LIMIT 3 OFFSET 2");
		
		assertTrue(resultSet.next());
		assertEquals(4, resultSet.getInt(1));
		
		assertTrue(resultSet.next());
		assertEquals(3, resultSet.getInt(1));
		
		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getInt(1));
		
		assertFalse(resultSet.next());
    }
    
    @Test
    public void maxRowsWithSqlLimitAll() throws SQLException {
    	prepareDatabase();
    	Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("SELECT * FROM shards ORDER BY id DESC LIMIT ALL OFFSET 2");
		
		assertTrue(resultSet.next());
		assertEquals(4, resultSet.getInt(1));
		
		assertTrue(resultSet.next());
		assertEquals(3, resultSet.getInt(1));
		
		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getInt(1));
		
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		
		assertFalse(resultSet.next());
    }
    
    @Test
    public void batch() throws SQLException {
        prepareDatabase();
        Statement statement = connection.createStatement();
        
        statement.addBatch("UPDATE shards SET value='changed' WHERE value = 'test'");
        statement.addBatch("UPDATE shards SET id = 10 WHERE id = 6");
        int[] result = statement.executeBatch();
        
        assertNotNull(result);
        assertTrue(Arrays.equals(result, new int[] {4, 1}));
    }
    
    @Test
    public void preparedStatementBatch() throws SQLException {
        prepareDatabase();
        PreparedStatement statement = connection.prepareStatement("UPDATE shards SET value=? WHERE value = ?");
        statement.setString(1, "changed");
        statement.setString(2, "test");
        statement.addBatch();
        statement.setString(1, "test");
        statement.setString(2, "changed");
        statement.addBatch();
        
        int[] result = statement.executeBatch();
        
        assertNotNull(result);
        assertTrue(Arrays.equals(result, new int[] {4, 4}));
    }
    
    @Test
    public void statementWarning() throws SQLException {
    	Statement statement = connection.createStatement();
    	statement.executeUpdate("DELETE FROM shards");
    	statement.execute("INSERT INTO shards VALUES(30, 'test')");
    	SQLWarning warnings = statement.getWarnings();
    	assertNotNull(warnings);
    	assertNull(warnings.getNextWarning());
    }

    @Test(expected=SQLException.class)
    public void statementError() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("DUMMY");
    }
    
    @Test(expected = SQLException.class)
    public void notExistingConfiguration() throws SQLException {
        String url = "jdbc:shards:dummy.yml";
        DriverManager.getConnection(url);
    }

}
