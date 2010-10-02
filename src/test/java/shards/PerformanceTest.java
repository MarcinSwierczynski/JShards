package shards;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

public class PerformanceTest {

	private static final String[] sqls = new String[] { "SELECT * FROM items WHERE category = 1", "SELECT * FROM items WHERE category = 1 OR category = 2",
	        "SELECT count(category) FROM items WHERE category BETWEEN 1 AND 3 GROUP BY category",
	        "UPDATE items SET price = 13.12 WHERE category = 1 and id = 3", "INSERT INTO items (id, name, category, price) values (default, 'test', 1, 12.11)",
	        "INSERT INTO items (id, name, category, price) values (default, 'test', 2, 14.11)" };

	@Test
	public void blankTest() {
	}

	public static void main(String[] args) throws SQLException, InterruptedException {
		// rozgrzewka
		for (int t = 0; t < 3; t++) {
			for (String sql : sqls) {
				postgresOneDB(sql);
				shardsOneDB(sql);
			}
		}
		// performanceTest();
		throughputTest();
	}

	private static void performanceTest() throws SQLException {
		System.out.println("Direct\tShards");
		// TESTY
		for (String sql : sqls) {
			System.out.println(sql);
			for (int t = 0; t < 10; t++) {
				long before = System.currentTimeMillis();
				postgresOneDB(sql);
				long postgres = System.currentTimeMillis() - before;
				System.out.print(postgres + "\t");
				before = System.currentTimeMillis();
				shardsOneDB(sql);
				long shardsOneDB = System.currentTimeMillis() - before;
				System.out.println(shardsOneDB);
			}
		}
	}

	public static void throughputTest() throws SQLException, InterruptedException {
		int count = 3;
		System.out.println("Multi: " + count + " threads for every query");
		multithreaded(count);
	}

	private static void postgresOneDB(String query) throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:postgresql://10.100.0.132:5432/perftest-single", "postgres", "postgres");
		Statement statement = conn.createStatement();
		final boolean resultSet = statement.execute(query);
		if (resultSet) {
			final ResultSet rs = statement.getResultSet();
			while (rs.next()) {
				for (int t = 1; t < rs.getMetaData().getColumnCount(); t++) {
					rs.getObject(t);
				}
			}
		}
	}

	private static void shardsOneDB(String query) throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:shards:src/test/resources/perftest_1db.yml");
		Statement statement = conn.createStatement();
		final boolean resultSet = statement.execute(query);
		if (resultSet) {
			final ResultSet rs = statement.getResultSet();
			while (rs.next()) {
				for (int t = 1; t < rs.getMetaData().getColumnCount(); t++) {
					rs.getObject(t);
				}
			}
		}
	}

	private static void multithreaded(int threadsForEverySql) throws SQLException, InterruptedException {
		List<Connection> shardsConnections = Lists.newArrayList();
		List<Connection> plainConnections = Lists.newArrayList();
		for (int t = 0; t < threadsForEverySql * sqls.length; t++) {
			shardsConnections.add(DriverManager.getConnection("jdbc:shards:src/test/resources/perftest_2db.yml"));
			plainConnections.add(DriverManager.getConnection("jdbc:postgresql://10.100.0.132:5432/perftest-single", "postgres", "postgres"));
		}

		multithreaded(threadsForEverySql, plainConnections);
		multithreaded(threadsForEverySql, shardsConnections);
	}

	private static void multithreaded(int threadsForEverySql, List<Connection> connections) throws SQLException, InterruptedException {
		long multiStart = System.currentTimeMillis();
		ThreadGroup group = new ThreadGroup("throughput");
		multithreadedTest(sqls, threadsForEverySql, group, connections);
		int count = group.activeCount() * 2;
		Thread[] threads = new Thread[count];
		group.enumerate(threads);
		for (int i = 0; i < count; i++) {
			Thread t = threads[i];
			if (t != null)
				t.join();
		}
		System.out.println(System.currentTimeMillis() - multiStart);
	}

	private static void multithreadedTest(String[] sqls, int threadsForEverySql, ThreadGroup group, List<Connection> connections) throws SQLException {
		Iterator<Connection> iterator = connections.iterator();
		for (String sql : sqls) {
			for (int t = 0; t < threadsForEverySql; t++) {
				new Thread(group, new SqlThread(sql, iterator.next())).start();
			}
		}
	}

	public static final class SqlThread implements Runnable {

		private String sql;
		private Statement statement;

		public SqlThread(String sql, Connection connection) throws SQLException {
			this.sql = sql;
			statement = connection.createStatement();
		}

		public void run() {
			try {
				final boolean resultSet = statement.execute(sql);
				if (resultSet) {
					final ResultSet rs = statement.getResultSet();
					while (rs.next()) {
						for (int t = 1; t < rs.getMetaData().getColumnCount(); t++) {
							rs.getObject(t);
						}
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

}
