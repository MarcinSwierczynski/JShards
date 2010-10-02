package shards;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import utils.SimpleResultSet;

public class SimpleResultSetTest {
	@Test
	public void sort() throws SQLException {
		SimpleResultSet resultSet = new SimpleResultSet();
		resultSet.addColumn("id", 0, 0, 0);
		resultSet.addColumn("city", 0, 0, 0);
		resultSet.addColumn("description", 0, 0, 0);
		resultSet.addRow(new Object[] { 1, "Warsaw", "Developer" });
		resultSet.addRow(new Object[] { 2, "Warsaw", "Tester" });
		resultSet.addRow(new Object[] { 3, "Torun", "Product manager" });
		resultSet.addRow(new Object[] { 4, "Warsaw", "Developer" });
		
		ImmutableList<OrderByColumn> columnsIndexesList = ImmutableList.of(new OrderByColumn(2, true), new OrderByColumn(3, true));
		resultSet.sort(columnsIndexesList);
		
		assertTrue(resultSet.next());
		assertEquals(3, resultSet.getInt("id"));
		assertEquals("Torun", resultSet.getString("city"));
		
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt("id"));
		assertEquals("Warsaw", resultSet.getString("city"));
		
		assertTrue(resultSet.next());
		assertEquals(4, resultSet.getInt("id"));
		assertEquals("Developer", resultSet.getString("description"));
		
		assertTrue(resultSet.next());
		assertEquals(2, resultSet.getInt("id"));
		assertEquals("Tester", resultSet.getString("description"));
		
		assertFalse(resultSet.next());
	}
	
	@Test(expected = WrongShardsQueryException.class)
	public void notComparableSort() throws SQLException {
		SimpleResultSet resultSet = new SimpleResultSet();
		resultSet.addColumn("id", 0, 0, 0);
		resultSet.addRow(new Object[] { String.class });
		resultSet.addRow(new Object[] { String.class });
		resultSet.sort(ImmutableList.of(new OrderByColumn(1, true)));
	}
}
