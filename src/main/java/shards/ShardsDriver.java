package shards;

import static com.google.common.collect.Iterables.transform;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Assert;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class ShardsDriver implements java.sql.Driver {

	private static final String JDBC_SHARDS_PREFIX = "jdbc:shards:";
	private static Logger logger = LoggerFactory.getLogger(ShardsDriver.class);

	private ConfigurationLoader configLoader;

	static {
		try {
			java.sql.DriverManager.registerDriver(new ShardsDriver());
		} catch (SQLException e) {
			logger.error("Error when registering a driver", e);
		}
	}

	/** No arg constructor created according to JDBC-4.0-fr.spec, chapter 9.2 */
	public ShardsDriver() {
		configLoader = new ConfigurationLoader();
	}

	public boolean acceptsURL(String url) throws SQLException {
		logger.debug("acceptsURL " + url);
		return (url != null && url.startsWith(JDBC_SHARDS_PREFIX));
	}

	public Connection connect(String url, Properties info) throws SQLException {
		if (acceptsURL(url)) {
			String fileName = getFileNameFromUrl(url);
			try {
				Configuration configuration = configLoader.load(fileName);
				
				List<String> drivers = configuration.getDrivers();
				for (String classname : drivers) {
					Class<? extends Driver> driverClass = (Class<? extends Driver>) Class.forName(classname);
					Driver driver = driverClass.newInstance();
					java.sql.DriverManager.registerDriver(driver);
					logger.debug("Registering driver " + classname);
				}
				
				List<ConnectionInfo> connections = configuration.getConnections();
				
				ConnectionsHolder connectionsHolder = new ConnectionsHolder();
				for (ConnectionInfo connectionInfo : connections) {
					Properties properties = new Properties(info);
					properties.setProperty("user", connectionInfo.getUser());
					properties.setProperty("password", connectionInfo.getPassword());
					
					Connection connection = DriverManager.getConnection(connectionInfo.getUrl(), properties);
					connectionsHolder.add(connectionInfo.getName(), connection);
				}
				return new ShardsConnection(connectionsHolder, configuration);
			} catch (Exception e) {
				throw new SQLException("Cannot load configuration file", e);
			}
		} else {
			return null;
		}
	}

	private String getFileNameFromUrl(String url) {
		return url.substring(JDBC_SHARDS_PREFIX.length());
	}

	public int getMajorVersion() {
		return 1;
	}

	public int getMinorVersion() {
		return 0;
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return getUnderlyingDriver(url).getPropertyInfo(url, info);
	}

	public boolean jdbcCompliant() {
		return false;
	}

	private Driver getUnderlyingDriver(String url) throws SQLException {
		if (url.startsWith(JDBC_SHARDS_PREFIX)) {
			Iterable<String> urls = getUnderlyingUrls(url);
			String underlyingUrl = Iterables.get(urls, 0);
			Enumeration e = DriverManager.getDrivers();
			Driver d;
			while (e.hasMoreElements()) {
				d = (Driver) e.nextElement();
				if (d.acceptsURL(underlyingUrl)) {
					return d;
				}
			}
		}
		return null;
	}
	
	private Iterable<String> getUnderlyingUrls(String url) {
		String rest = url.substring(12);
		Assert.hasText(rest, "Illegal JDBC connection string. Usage: jdbc:shards:postgresql://dev:5432/shards,postgresql://dev:5432/shards2");
		String[] urls = rest.split(",");
		Iterable<String> results = transform(Lists.newArrayList(urls), new Function<String, String>() {
			public String apply(String from) {
				return "jdbc:" + from;
			}
		});
		return results;
	}

}
