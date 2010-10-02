package shards;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class JSqlParserTest {
	
	@Test
	public void parseInsert() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "INSERT INTO Tabela(id,name) VALUES(5 * 3.0, 'lol')";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			public Set<String> selectShards(ParameterInfo param) {
				if (param.getColumn().equals("id")) {
					if (param.getValue().equals(15.0d)) {
						return ImmutableSet.of("shard1");
					} else {
						return ImmutableSet.of("shard2");
					}
				}
				return ImmutableSet.of("shard1", "shard2");
			}

			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(shards.size(), 1);
		assertEquals(shards.iterator().next(), "shard1");
	}

	@Test
	public void parseUpdate() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "Update Tabela SET name = 5 WHERE id = 3 AND id = 4 OR id = 3";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			public Set<String> selectShards(ParameterInfo param) {
				if (param.getColumn().equals("id")) {
					if (param.getValue().equals(3l)) {
						return ImmutableSet.of("shard1");
					} else {
						return ImmutableSet.of("shard2");
					}
				}
				return ImmutableSet.of("shard1", "shard2");
			}

			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(shards.size(), 1);
		assertEquals(shards.iterator().next(), "shard1");
	}

	@Test
	public void parseDelete() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "DELETE FROM Tabela WHERE id = 3 AND id = 4 OR id = 3";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			public Set<String> selectShards(ParameterInfo param) {
				if (param.getColumn().equals("id")) {
					if (param.getValue().equals(3l)) {
						return ImmutableSet.of("shard1");
					} else {
						return ImmutableSet.of("shard2");
					}
				}
				return ImmutableSet.of("shard1", "shard2");
			}

			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(shards.size(), 1);
		assertEquals(shards.iterator().next(), "shard1");
	}

	@Test
	public void parseSelectWithQuotes() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT * FROM Tabela WHERE \"name\"='Something'";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			public Set<String> selectShards(ParameterInfo param) {
				if (param.getColumn().equals("name")) {
					if (param.getValue().equals("Something")) {
						return ImmutableSet.of("shard1");
					} else {
						return ImmutableSet.of("shard2");
					}
				}
				return ImmutableSet.of("shard1", "shard2");
			}

			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(1, shards.size());
		assertEquals(shards.iterator().next(), "shard1");
	}

	//(0;2) - shard1, (3;+oo) - shard2
	@Test
	public void parseSelectBetween() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT * FROM Tabela WHERE id BETWEEN 1 AND 2";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			public Set<String> selectShards(ParameterInfo param) {
				if (param.getColumn().equals("id")) {
					switch (param.getOperator()) {
					case MORE_EQUAL:
						if ((Long) param.getValue() <= 2)
							return ImmutableSet.of("shard1", "shard2");
						if ((Long) param.getValue() >= 3)
							return ImmutableSet.of("shard2");
						break;
					case LESS_EQUAL:
						if ((Long) param.getValue() <= 2)
							return ImmutableSet.of("shard1");
						if ((Long) param.getValue() >= 3)
							return ImmutableSet.of("shard1", "shard2");
						break;
					default:
						break;
					}
				}
				return ImmutableSet.of("shard1", "shard2");
			}

			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(shards.size(), 1);
		assertEquals(shards.iterator().next(), "shard1");
	}

	@Test
	public void parseSelectIn() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT * FROM Tabela WHERE id IN(1,2)";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			public Set<String> selectShards(ParameterInfo param) {
				if (param.getColumn().equals("id")) {
					if (param.getValue().equals(1l)) {
						return ImmutableSet.of("shard1");
					} else {
						return ImmutableSet.of("shard2");
					}
				}
				return ImmutableSet.of("shard1", "shard2");
			}

			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(shards.size(), 2);
		assertTrue(shards.containsAll(ImmutableSet.of("shard1", "shard2")));
	}

	@Test
	public void parseSelectInverse() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT * FROM Tabela WHERE -id=1 * 2";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			public Set<String> selectShards(ParameterInfo param) {
				if (param.getValue().equals(-2l)) {
					return ImmutableSet.of("shard1");
				} else {
					return ImmutableSet.of("shard2");
				}
			}

			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(1, shards.size());
		assertTrue(shards.contains("shard1"));
	}

	@Test
	public void parseSelectIsNull() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT * FROM Tabela WHERE id IS NOT NULL";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			public Set<String> selectShards(ParameterInfo param) {
				if (param.getOperator().equals(Operator.NOT_EQUAL) && param.getValue() == null) {
					return ImmutableSet.of("shard1");
				} else {
					return ImmutableSet.of("shard2");
				}
			}

			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(1, shards.size());
		assertTrue(shards.contains("shard1"));
	}

	@Test
	public void parseSelectLike() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT * FROM Tabela WHERE value LIKE 'test%'";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			private String pattern = "test%";

			public Set<String> selectShards(ParameterInfo param) {
				if (param.getOperator().equals(Operator.LIKE)) {
					String like = (String) param.getValue();
					if (pattern.equals(like)) {
						return ImmutableSet.of("shard1");
					} else {
						return ImmutableSet.of("shard2");
					}
				}
				return allShards(param.getTable());
			}

			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(1, shards.size());
		assertTrue(shards.contains("shard1"));
	}

	@Test
	public void parseSelectUnion() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT * FROM Tabela WHERE id = 1 UNION SELECT * FROM Tabela WHERE id = 2";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			public Set<String> selectShards(ParameterInfo param) {
				if (param.getColumn().equals("id")) {
					if (param.getValue().equals(1l)) {
						return ImmutableSet.of("shard1");
					} else {
						return ImmutableSet.of("shard2");
					}
				}
				return ImmutableSet.of("shard1", "shard2", "shard3");
			}

			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2", "shard3");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(2, shards.size());
		assertTrue(shards.containsAll(ImmutableSet.of("shard1", "shard2")));
	}
	
	@Test
	public void parseSelectWithoutWhere() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT * FROM Tabela";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			
			public Set<String> selectShards(ParameterInfo param) {
				return ImmutableSet.of("shard1", "shard2", "shard3");
			}
			
			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2", "shard3");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(3, shards.size());
		assertTrue(shards.containsAll(ImmutableSet.of("shard1", "shard2", "shard3")));
	}
	
	@Test
	public void parseSelectDates() throws WrongShardsQueryException, SQLParserException {
		//TODO: Notacja {d|t|ts ''} dziala w MySQL i SQL Server, nie dziala w PostgreSQL
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT * FROM Tabela WHERE data = {ts '2009-12-06 00:00:00'}";
		Set<String> shards = parser.parse(sql, new ShardsSelectionStrategy() {
			Calendar calendar;
			{
				calendar = Calendar.getInstance();
				calendar.set(2009, 1, 1);
			}
			
			public Set<String> selectShards(ParameterInfo param) {
				if(param.getColumn().equals("data")) {
					if(param.getValue() instanceof Date) {
						Calendar calendar = Calendar.getInstance();
						calendar.setTime((Date) param.getValue());
						if(calendar.compareTo(this.calendar) > 0) {
							return ImmutableSet.of("shard1");
						} else {
							return ImmutableSet.of("shard2");
						}
					}
				}
				return allShards(param.getTable());
			}
			
			public Set<String> allShards(String tableName) {
				return ImmutableSet.of("shard1", "shard2");
			}
		}).getSelectedShards();
		assertNotNull(shards);
		assertEquals(1, shards.size());
		assertTrue(shards.containsAll(ImmutableSet.of("shard1")));
	}
	
	@Test
	public void groupBy() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT count(a) FROM Tabela GROUP BY a";
		ParseResult result = parser.parse(sql, new DummyShardsSelectionStrategy());
		List<Integer> groupByColumns = result.getGroupByColumns();
		assertNotNull(groupByColumns);
		assertEquals(1, groupByColumns.size());
		assertEquals(new Integer(2), groupByColumns.get(0));
		assertNotNull(result.getRewrittenQuery());
		assertEquals("SELECT count(a), a FROM Tabela GROUP BY a", result.getRewrittenQuery());
	}
	
	@Test
	public void having() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT count(a) FROM Tabela GROUP BY a HAVING count(a) > 10 AND a LIKE '%a%'";
		ParseResult result = parser.parse(sql, new DummyShardsSelectionStrategy());
		List<Integer> havingColumns = result.getHavingColumns();
		assertNotNull(havingColumns);
		assertEquals(2, havingColumns.size());
		assertEquals(new Integer(3), havingColumns.get(0));
		assertEquals(new Integer(4), havingColumns.get(1));
		assertNotNull(result.getRewrittenQuery());
		assertEquals("SELECT count(a), a, count(a), a FROM Tabela GROUP BY a", result.getRewrittenQuery());
	}
	
	@Test
	public void havingAvg() throws WrongShardsQueryException, SQLParserException {
		JSqlParser parser = new JSqlParser();
		String sql = "SELECT count(a) FROM Tabela GROUP BY a HAVING avg(a) > 10";
		ParseResult result = parser.parse(sql, new DummyShardsSelectionStrategy());
		List<Integer> havingColumns = result.getHavingColumns();
		assertNotNull(havingColumns);
		assertEquals(1, havingColumns.size());
		assertEquals(new Integer(3), havingColumns.get(0));
		assertNotNull(result.getRewrittenQuery());
		assertEquals("SELECT count(a), a, avg(a), sum(a), count(a) FROM Tabela GROUP BY a", result.getRewrittenQuery());
	}
	
	@Test
	public void jdbcParameter() throws WrongShardsQueryException, SQLParserException {
	    JSqlParser parser = new JSqlParser();
        String sql = "SELECT * FROM Tabela WHERE id = ?";
        PreparedStatementParametersList params = new PreparedStatementParametersList();
        params.addParameter(1, new PreparedStatementParameter("setLong", 1l, long.class));
        
        ParseResult result = parser.parse(sql, new ShardsSelectionStrategy() {
			
			public Set<String> selectShards(ParameterInfo param) {
				String column = param.getColumn();
                Object value = param.getValue();
                if(column.equals("id") && value.equals(1l)) {
					return ImmutableSet.of("shard1");
				} else {
					return ImmutableSet.of();
				}
			}
		}, params);
        
        Set<String> selectedShards = result.getSelectedShards();
        assertEquals(ImmutableSet.of("shard1"), selectedShards);
	}
	
}
