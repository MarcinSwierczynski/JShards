package shards;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import utils.SimpleResultSet;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class ShardsResultSet implements ResultSet {

	private SimpleResultSet simpleResultSet = new SimpleResultSet();
	private SQLWarning allWarnings = null;

	public ShardsResultSet(List<ResultSet> selected) throws SQLException {
		prepareResultSetMetaData(selected);
		populateResultSetRows(selected);
	}

	public ShardsResultSet(ResultSetMetaData metaData) throws SQLException {
		prepareResultSetMetaData(metaData);
	}

	private void prepareResultSetMetaData(List<ResultSet> selected) throws SQLException {
		ResultSetMetaData metaData = selected.get(0).getMetaData();
		prepareResultSetMetaData(metaData);
	}

	private void prepareResultSetMetaData(ResultSetMetaData metaData) throws SQLException {
		int columnCount = metaData.getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			String columnName = metaData.getColumnName(i);
			int columnType = metaData.getColumnType(i);
			int precision = metaData.getPrecision(i);
			int scale = metaData.getScale(i);
			simpleResultSet.addColumn(columnName, columnType, precision, scale);
		}
	}

	private void populateResultSetRows(List<ResultSet> selected) throws SQLException {
		int columnCount = simpleResultSet.getColumnCount();
		for (ResultSet resultSet : selected) {
			while (resultSet.next()) {
				Object[] row = new Object[columnCount];
				for (int i = 1; i <= columnCount; i++) {
					row[i - 1] = resultSet.getObject(i);
				}
				simpleResultSet.addRow(row);
			}
			addWarningsFromResultSet(resultSet);
		}
	}

	private void addWarningsFromResultSet(ResultSet resultSet) throws SQLException {
		SQLWarning warnings = resultSet.getWarnings();
		if(allWarnings != null) {
			allWarnings.setNextWarning(warnings);
		} else {
			allWarnings = warnings;
		}
	}

	public void sort(List<OrderByColumn> columnsIndexes) {
		simpleResultSet.sort(columnsIndexes);
	}

	public boolean absolute(int row) throws SQLException {
		return simpleResultSet.absolute(row);
	}

	public void addColumn(String name, int sqlType, int precision, int scale) throws SQLException {
		simpleResultSet.addColumn(name, sqlType, precision, scale);
	}

	public void addRow(Object[] row) throws SQLException {
		simpleResultSet.addRow(row);
	}

	public void afterLast() throws SQLException {
		simpleResultSet.afterLast();
	}

	public void beforeFirst() throws SQLException {
		simpleResultSet.beforeFirst();
	}

	public void cancelRowUpdates() throws SQLException {
		simpleResultSet.cancelRowUpdates();
	}

	public void clearWarnings() throws SQLException {
		allWarnings = null;
	}

	public void close() throws SQLException {
		simpleResultSet.close();
	}

	public void deleteRow() throws SQLException {
		simpleResultSet.deleteRow();
	}

	public boolean equals(Object obj) {
		return simpleResultSet.equals(obj);
	}

	public int findColumn(String columnName) throws SQLException {
		return simpleResultSet.findColumn(columnName);
	}

	public boolean first() throws SQLException {
		return simpleResultSet.first();
	}

	public Array getArray(int i) throws SQLException {
		return simpleResultSet.getArray(i);
	}

	public Array getArray(String colName) throws SQLException {
		return simpleResultSet.getArray(colName);
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return simpleResultSet.getAsciiStream(columnIndex);
	}

	public InputStream getAsciiStream(String columnName) throws SQLException {
		return simpleResultSet.getAsciiStream(columnName);
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return simpleResultSet.getBigDecimal(columnIndex, scale);
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return simpleResultSet.getBigDecimal(columnIndex);
	}

	public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
		return simpleResultSet.getBigDecimal(columnName, scale);
	}

	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		return simpleResultSet.getBigDecimal(columnName);
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return simpleResultSet.getBinaryStream(columnIndex);
	}

	public InputStream getBinaryStream(String columnName) throws SQLException {
		return simpleResultSet.getBinaryStream(columnName);
	}

	public Blob getBlob(int i) throws SQLException {
		return simpleResultSet.getBlob(i);
	}

	public Blob getBlob(String colName) throws SQLException {
		return simpleResultSet.getBlob(colName);
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		return simpleResultSet.getBoolean(columnIndex);
	}

	public boolean getBoolean(String columnName) throws SQLException {
		return simpleResultSet.getBoolean(columnName);
	}

	public byte getByte(int columnIndex) throws SQLException {
		return simpleResultSet.getByte(columnIndex);
	}

	public byte getByte(String columnName) throws SQLException {
		return simpleResultSet.getByte(columnName);
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		return simpleResultSet.getBytes(columnIndex);
	}

	public byte[] getBytes(String columnName) throws SQLException {
		return simpleResultSet.getBytes(columnName);
	}

	public String getCatalogName(int columnIndex) throws SQLException {
		return simpleResultSet.getCatalogName(columnIndex);
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {
		return simpleResultSet.getCharacterStream(columnIndex);
	}

	public Reader getCharacterStream(String columnName) throws SQLException {
		return simpleResultSet.getCharacterStream(columnName);
	}

	public Clob getClob(int i) throws SQLException {
		return simpleResultSet.getClob(i);
	}

	public Clob getClob(String colName) throws SQLException {
		return simpleResultSet.getClob(colName);
	}

	public String getColumnClassName(int columnIndex) throws SQLException {
		return simpleResultSet.getColumnClassName(columnIndex);
	}

	public int getColumnCount() throws SQLException {
		return simpleResultSet.getColumnCount();
	}

	public int getColumnDisplaySize(int columnIndex) throws SQLException {
		return simpleResultSet.getColumnDisplaySize(columnIndex);
	}

	public String getColumnLabel(int columnIndex) throws SQLException {
		return simpleResultSet.getColumnLabel(columnIndex);
	}

	public String getColumnName(int columnIndex) throws SQLException {
		return simpleResultSet.getColumnName(columnIndex);
	}

	public int getColumnType(int columnIndex) throws SQLException {
		return simpleResultSet.getColumnType(columnIndex);
	}

	public String getColumnTypeName(int columnIndex) throws SQLException {
		return simpleResultSet.getColumnTypeName(columnIndex);
	}

	public int getConcurrency() throws SQLException {
		return simpleResultSet.getConcurrency();
	}

	public String getCursorName() throws SQLException {
		return simpleResultSet.getCursorName();
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		return simpleResultSet.getDate(columnIndex, cal);
	}

	public Date getDate(int columnIndex) throws SQLException {
		return simpleResultSet.getDate(columnIndex);
	}

	public Date getDate(String columnName, Calendar cal) throws SQLException {
		return simpleResultSet.getDate(columnName, cal);
	}

	public Date getDate(String columnName) throws SQLException {
		return simpleResultSet.getDate(columnName);
	}

	public double getDouble(int columnIndex) throws SQLException {
		return simpleResultSet.getDouble(columnIndex);
	}

	public double getDouble(String columnName) throws SQLException {
		return simpleResultSet.getDouble(columnName);
	}

	public int getFetchDirection() throws SQLException {
		return simpleResultSet.getFetchDirection();
	}

	public int getFetchSize() throws SQLException {
		return simpleResultSet.getFetchSize();
	}

	public float getFloat(int columnIndex) throws SQLException {
		return simpleResultSet.getFloat(columnIndex);
	}

	public float getFloat(String columnName) throws SQLException {
		return simpleResultSet.getFloat(columnName);
	}

	public int getHoldability() {
		return simpleResultSet.getHoldability();
	}

	public int getInt(int columnIndex) throws SQLException {
		return simpleResultSet.getInt(columnIndex);
	}

	public int getInt(String columnName) throws SQLException {
		return simpleResultSet.getInt(columnName);
	}

	public long getLong(int columnIndex) throws SQLException {
		return simpleResultSet.getLong(columnIndex);
	}

	public long getLong(String columnName) throws SQLException {
		return simpleResultSet.getLong(columnName);
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		return simpleResultSet.getMetaData();
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		return simpleResultSet.getNCharacterStream(columnIndex);
	}

	public Reader getNCharacterStream(String columnName) throws SQLException {
		return simpleResultSet.getNCharacterStream(columnName);
	}

	public NClob getNClob(int columnIndex) throws SQLException {
		return simpleResultSet.getNClob(columnIndex);
	}

	public NClob getNClob(String columnLabel) throws SQLException {
		return simpleResultSet.getNClob(columnLabel);
	}

	public String getNString(int columnIndex) throws SQLException {
		return simpleResultSet.getNString(columnIndex);
	}

	public String getNString(String columnName) throws SQLException {
		return simpleResultSet.getNString(columnName);
	}

	public Object getObject(int i, Map map) throws SQLException {
		return simpleResultSet.getObject(i, map);
	}

	public Object getObject(int columnIndex) throws SQLException {
		return simpleResultSet.getObject(columnIndex);
	}

	public Object getObject(String colName, Map map) throws SQLException {
		return simpleResultSet.getObject(colName, map);
	}

	public Object getObject(String columnName) throws SQLException {
		return simpleResultSet.getObject(columnName);
	}

	public int getPrecision(int columnIndex) throws SQLException {
		return simpleResultSet.getPrecision(columnIndex);
	}

	public Ref getRef(int i) throws SQLException {
		return simpleResultSet.getRef(i);
	}

	public Ref getRef(String colName) throws SQLException {
		return simpleResultSet.getRef(colName);
	}

	public int getRow() throws SQLException {
		return simpleResultSet.getRow();
	}

	public RowId getRowId(int columnIndex) throws SQLException {
		return simpleResultSet.getRowId(columnIndex);
	}

	public RowId getRowId(String columnLabel) throws SQLException {
		return simpleResultSet.getRowId(columnLabel);
	}

	public int getScale(int columnIndex) throws SQLException {
		return simpleResultSet.getScale(columnIndex);
	}

	public String getSchemaName(int columnIndex) throws SQLException {
		return simpleResultSet.getSchemaName(columnIndex);
	}

	public short getShort(int columnIndex) throws SQLException {
		return simpleResultSet.getShort(columnIndex);
	}

	public short getShort(String columnName) throws SQLException {
		return simpleResultSet.getShort(columnName);
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		return simpleResultSet.getSQLXML(columnIndex);
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		return simpleResultSet.getSQLXML(columnLabel);
	}

	public Statement getStatement() throws SQLException {
		return simpleResultSet.getStatement();
	}

	public String getString(int columnIndex) throws SQLException {
		return simpleResultSet.getString(columnIndex);
	}

	public String getString(String columnName) throws SQLException {
		return simpleResultSet.getString(columnName);
	}

	public String getTableName(int columnIndex) throws SQLException {
		return simpleResultSet.getTableName(columnIndex);
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return simpleResultSet.getTime(columnIndex, cal);
	}

	public Time getTime(int columnIndex) throws SQLException {
		return simpleResultSet.getTime(columnIndex);
	}

	public Time getTime(String columnName, Calendar cal) throws SQLException {
		return simpleResultSet.getTime(columnName, cal);
	}

	public Time getTime(String columnName) throws SQLException {
		return simpleResultSet.getTime(columnName);
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		return simpleResultSet.getTimestamp(columnIndex, cal);
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return simpleResultSet.getTimestamp(columnIndex);
	}

	public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
		return simpleResultSet.getTimestamp(columnName, cal);
	}

	public Timestamp getTimestamp(String columnName) throws SQLException {
		return simpleResultSet.getTimestamp(columnName);
	}

	public int getType() throws SQLException {
		return simpleResultSet.getType();
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return simpleResultSet.getUnicodeStream(columnIndex);
	}

	public InputStream getUnicodeStream(String columnName) throws SQLException {
		return simpleResultSet.getUnicodeStream(columnName);
	}

	public URL getURL(int columnIndex) throws SQLException {
		return simpleResultSet.getURL(columnIndex);
	}

	public URL getURL(String columnName) throws SQLException {
		return simpleResultSet.getURL(columnName);
	}

	public SQLWarning getWarnings() throws SQLException {
		return allWarnings;
	}

	public int hashCode() {
		return simpleResultSet.hashCode();
	}

	public void insertRow() throws SQLException {
		simpleResultSet.insertRow();
	}

	public boolean isAfterLast() throws SQLException {
		return simpleResultSet.isAfterLast();
	}

	public boolean isAutoIncrement(int columnIndex) throws SQLException {
		return simpleResultSet.isAutoIncrement(columnIndex);
	}

	public boolean isBeforeFirst() throws SQLException {
		return simpleResultSet.isBeforeFirst();
	}

	public boolean isCaseSensitive(int columnIndex) throws SQLException {
		return simpleResultSet.isCaseSensitive(columnIndex);
	}

	public boolean isClosed() throws SQLException {
		return simpleResultSet.isClosed();
	}

	public boolean isCurrency(int columnIndex) throws SQLException {
		return simpleResultSet.isCurrency(columnIndex);
	}

	public boolean isDefinitelyWritable(int columnIndex) throws SQLException {
		return simpleResultSet.isDefinitelyWritable(columnIndex);
	}

	public boolean isFirst() throws SQLException {
		return simpleResultSet.isFirst();
	}

	public boolean isLast() throws SQLException {
		return simpleResultSet.isLast();
	}

	public int isNullable(int columnIndex) throws SQLException {
		return simpleResultSet.isNullable(columnIndex);
	}

	public boolean isReadOnly(int columnIndex) throws SQLException {
		return simpleResultSet.isReadOnly(columnIndex);
	}

	public boolean isSearchable(int columnIndex) throws SQLException {
		return simpleResultSet.isSearchable(columnIndex);
	}

	public boolean isSigned(int columnIndex) throws SQLException {
		return simpleResultSet.isSigned(columnIndex);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return simpleResultSet.isWrapperFor(iface);
	}

	public boolean isWritable(int columnIndex) throws SQLException {
		return simpleResultSet.isWritable(columnIndex);
	}

	public boolean last() throws SQLException {
		return simpleResultSet.last();
	}

	public void moveToCurrentRow() throws SQLException {
		simpleResultSet.moveToCurrentRow();
	}

	public void moveToInsertRow() throws SQLException {
		simpleResultSet.moveToInsertRow();
	}

	public boolean next() throws SQLException {
		return simpleResultSet.next();
	}

	public boolean previous() throws SQLException {
		return simpleResultSet.previous();
	}

	public void refreshRow() throws SQLException {
		simpleResultSet.refreshRow();
	}

	public boolean relative(int rows) throws SQLException {
		return simpleResultSet.relative(rows);
	}

	public boolean rowDeleted() throws SQLException {
		return simpleResultSet.rowDeleted();
	}

	public boolean rowInserted() throws SQLException {
		return simpleResultSet.rowInserted();
	}

	public boolean rowUpdated() throws SQLException {
		return simpleResultSet.rowUpdated();
	}

	public void setFetchDirection(int direction) throws SQLException {
		simpleResultSet.setFetchDirection(direction);
	}

	public void setFetchSize(int rows) throws SQLException {
		simpleResultSet.setFetchSize(rows);
	}

	public String toString() {
		return simpleResultSet.toString();
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		return simpleResultSet.unwrap(iface);
	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
		simpleResultSet.updateArray(columnIndex, x);
	}

	public void updateArray(String columnName, Array x) throws SQLException {
		simpleResultSet.updateArray(columnName, x);
	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		simpleResultSet.updateAsciiStream(columnIndex, x, length);
	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		simpleResultSet.updateAsciiStream(columnIndex, x, length);
	}

	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		simpleResultSet.updateAsciiStream(columnIndex, x);
	}

	public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
		simpleResultSet.updateAsciiStream(columnName, x, length);
	}

	public void updateAsciiStream(String columnName, InputStream x, long length) throws SQLException {
		simpleResultSet.updateAsciiStream(columnName, x, length);
	}

	public void updateAsciiStream(String columnName, InputStream x) throws SQLException {
		simpleResultSet.updateAsciiStream(columnName, x);
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		simpleResultSet.updateBigDecimal(columnIndex, x);
	}

	public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
		simpleResultSet.updateBigDecimal(columnName, x);
	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		simpleResultSet.updateBinaryStream(columnIndex, x, length);
	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		simpleResultSet.updateBinaryStream(columnIndex, x, length);
	}

	public void updateBinaryStream(int columnName, InputStream x) throws SQLException {
		simpleResultSet.updateBinaryStream(columnName, x);
	}

	public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
		simpleResultSet.updateBinaryStream(columnName, x, length);
	}

	public void updateBinaryStream(String columnName, InputStream x, long length) throws SQLException {
		simpleResultSet.updateBinaryStream(columnName, x, length);
	}

	public void updateBinaryStream(String columnName, InputStream x) throws SQLException {
		simpleResultSet.updateBinaryStream(columnName, x);
	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		simpleResultSet.updateBlob(columnIndex, x);
	}

	public void updateBlob(int columnIndex, InputStream x, long length) throws SQLException {
		simpleResultSet.updateBlob(columnIndex, x, length);
	}

	public void updateBlob(int columnIndex, InputStream x) throws SQLException {
		simpleResultSet.updateBlob(columnIndex, x);
	}

	public void updateBlob(String columnName, Blob x) throws SQLException {
		simpleResultSet.updateBlob(columnName, x);
	}

	public void updateBlob(String columnName, InputStream x, long length) throws SQLException {
		simpleResultSet.updateBlob(columnName, x, length);
	}

	public void updateBlob(String columnName, InputStream x) throws SQLException {
		simpleResultSet.updateBlob(columnName, x);
	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		simpleResultSet.updateBoolean(columnIndex, x);
	}

	public void updateBoolean(String columnName, boolean x) throws SQLException {
		simpleResultSet.updateBoolean(columnName, x);
	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
		simpleResultSet.updateByte(columnIndex, x);
	}

	public void updateByte(String columnName, byte x) throws SQLException {
		simpleResultSet.updateByte(columnName, x);
	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		simpleResultSet.updateBytes(columnIndex, x);
	}

	public void updateBytes(String columnName, byte[] x) throws SQLException {
		simpleResultSet.updateBytes(columnName, x);
	}

	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		simpleResultSet.updateCharacterStream(columnIndex, x, length);
	}

	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		simpleResultSet.updateCharacterStream(columnIndex, x, length);
	}

	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		simpleResultSet.updateCharacterStream(columnIndex, x);
	}

	public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
		simpleResultSet.updateCharacterStream(columnName, reader, length);
	}

	public void updateCharacterStream(String columnName, Reader x, long length) throws SQLException {
		simpleResultSet.updateCharacterStream(columnName, x, length);
	}

	public void updateCharacterStream(String columnName, Reader x) throws SQLException {
		simpleResultSet.updateCharacterStream(columnName, x);
	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
		simpleResultSet.updateClob(columnIndex, x);
	}

	public void updateClob(int columnIndex, Reader x, long length) throws SQLException {
		simpleResultSet.updateClob(columnIndex, x, length);
	}

	public void updateClob(int columnIndex, Reader x) throws SQLException {
		simpleResultSet.updateClob(columnIndex, x);
	}

	public void updateClob(String columnName, Clob x) throws SQLException {
		simpleResultSet.updateClob(columnName, x);
	}

	public void updateClob(String columnName, Reader x, long length) throws SQLException {
		simpleResultSet.updateClob(columnName, x, length);
	}

	public void updateClob(String columnName, Reader x) throws SQLException {
		simpleResultSet.updateClob(columnName, x);
	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
		simpleResultSet.updateDate(columnIndex, x);
	}

	public void updateDate(String columnName, Date x) throws SQLException {
		simpleResultSet.updateDate(columnName, x);
	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
		simpleResultSet.updateDouble(columnIndex, x);
	}

	public void updateDouble(String columnName, double x) throws SQLException {
		simpleResultSet.updateDouble(columnName, x);
	}

	public void updateFloat(int columnIndex, float x) throws SQLException {
		simpleResultSet.updateFloat(columnIndex, x);
	}

	public void updateFloat(String columnName, float x) throws SQLException {
		simpleResultSet.updateFloat(columnName, x);
	}

	public void updateInt(int columnIndex, int x) throws SQLException {
		simpleResultSet.updateInt(columnIndex, x);
	}

	public void updateInt(String columnName, int x) throws SQLException {
		simpleResultSet.updateInt(columnName, x);
	}

	public void updateLong(int columnIndex, long x) throws SQLException {
		simpleResultSet.updateLong(columnIndex, x);
	}

	public void updateLong(String columnName, long x) throws SQLException {
		simpleResultSet.updateLong(columnName, x);
	}

	public void updateNCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		simpleResultSet.updateNCharacterStream(columnIndex, x, length);
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		simpleResultSet.updateNCharacterStream(columnIndex, x, length);
	}

	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		simpleResultSet.updateNCharacterStream(columnIndex, x);
	}

	public void updateNCharacterStream(String columnName, Reader x, int length) throws SQLException {
		simpleResultSet.updateNCharacterStream(columnName, x, length);
	}

	public void updateNCharacterStream(String columnName, Reader x, long length) throws SQLException {
		simpleResultSet.updateNCharacterStream(columnName, x, length);
	}

	public void updateNCharacterStream(String columnName, Reader x) throws SQLException {
		simpleResultSet.updateNCharacterStream(columnName, x);
	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		simpleResultSet.updateNClob(columnIndex, nClob);
	}

	public void updateNClob(int columnIndex, Reader x, long length) throws SQLException {
		simpleResultSet.updateNClob(columnIndex, x, length);
	}

	public void updateNClob(int columnIndex, Reader x) throws SQLException {
		simpleResultSet.updateNClob(columnIndex, x);
	}

	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		simpleResultSet.updateNClob(columnLabel, nClob);
	}

	public void updateNClob(String columnName, Reader x, long length) throws SQLException {
		simpleResultSet.updateNClob(columnName, x, length);
	}

	public void updateNClob(String columnName, Reader x) throws SQLException {
		simpleResultSet.updateNClob(columnName, x);
	}

	public void updateNString(int columnIndex, String nString) throws SQLException {
		simpleResultSet.updateNString(columnIndex, nString);
	}

	public void updateNString(String columnName, String nString) throws SQLException {
		simpleResultSet.updateNString(columnName, nString);
	}

	public void updateNull(int columnIndex) throws SQLException {
		simpleResultSet.updateNull(columnIndex);
	}

	public void updateNull(String columnName) throws SQLException {
		simpleResultSet.updateNull(columnName);
	}

	public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
		simpleResultSet.updateObject(columnIndex, x, scale);
	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
		simpleResultSet.updateObject(columnIndex, x);
	}

	public void updateObject(String columnName, Object x, int scale) throws SQLException {
		simpleResultSet.updateObject(columnName, x, scale);
	}

	public void updateObject(String columnName, Object x) throws SQLException {
		simpleResultSet.updateObject(columnName, x);
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
		simpleResultSet.updateRef(columnIndex, x);
	}

	public void updateRef(String columnName, Ref x) throws SQLException {
		simpleResultSet.updateRef(columnName, x);
	}

	public void updateRow() throws SQLException {
		simpleResultSet.updateRow();
	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		simpleResultSet.updateRowId(columnIndex, x);
	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		simpleResultSet.updateRowId(columnLabel, x);
	}

	public void updateShort(int columnIndex, short x) throws SQLException {
		simpleResultSet.updateShort(columnIndex, x);
	}

	public void updateShort(String columnName, short x) throws SQLException {
		simpleResultSet.updateShort(columnName, x);
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		simpleResultSet.updateSQLXML(columnIndex, xmlObject);
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		simpleResultSet.updateSQLXML(columnLabel, xmlObject);
	}

	public void updateString(int columnIndex, String x) throws SQLException {
		simpleResultSet.updateString(columnIndex, x);
	}

	public void updateString(String columnName, String x) throws SQLException {
		simpleResultSet.updateString(columnName, x);
	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
		simpleResultSet.updateTime(columnIndex, x);
	}

	public void updateTime(String columnName, Time x) throws SQLException {
		simpleResultSet.updateTime(columnName, x);
	}

	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		simpleResultSet.updateTimestamp(columnIndex, x);
	}

	public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
		simpleResultSet.updateTimestamp(columnName, x);
	}

	public boolean wasNull() throws SQLException {
		return simpleResultSet.wasNull();
	}

	public Row getCurrentRow() throws SQLException {
		Object[] rowData = new Object[getColumnCount()];
		for (int i = 0; i < getColumnCount(); i++) {
			rowData[i] = getObject(i + 1);
		}
		return new Row(rowData);
	}

	public void addRow(Row row) throws SQLException {
		simpleResultSet.addRow(row.asArray());
	}

	public void removeAddedColumns(int columnCount) {
		simpleResultSet.removeAddedColumns(columnCount);
	}

	public void limit(Long limit, long offset) {
		simpleResultSet.limit(limit, offset);
	}

}