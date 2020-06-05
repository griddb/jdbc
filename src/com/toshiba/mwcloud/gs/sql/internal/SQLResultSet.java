/*
   Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.toshiba.mwcloud.gs.sql.internal;

import java.io.ByteArrayInputStream;
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
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.serial.SerialBlob;

import com.toshiba.mwcloud.gs.sql.internal.SQLLaterFeatures.LaterResultSet;
import com.toshiba.mwcloud.gs.sql.internal.proxy.ProxyTargetInstanceFactory;

class SQLResultSet implements ResultSet, LaterResultSet {

	private static final Pattern INTEGER_PATTERN =
			Pattern.compile("^\\-?[0-9]+$");

	private final SQLStatement statement;

	private final ContainerInfo containerInfo;

	private final String[] labelList;

	private Map<String, Integer> labelIndexMap;

	private final long queryId;

	private boolean followingExists;

	private boolean followingFetched;

	private boolean statementOwned;

	
	private int fetchSize;

	private RowMapper.Cursor cursor;

	private Row currentRow;

	private Row firstRow;

	private Row lastRow;

	private Boolean wasNull;

	SQLResultSet(
			SQLStatement statement, RowMapper mapper, String[] labelList,
			long queryId, boolean followingExists, boolean followingAccepting,
			int rowCount, long varDataBaseOffset, BasicBuffer buf)
			throws SQLException {
		this.statement = statement;
		this.containerInfo = mapper.getContainerInfo();
		this.labelList = labelList;
		this.queryId = queryId;
		this.followingExists = (rowCount > 0 ? followingExists : false);
		fetchSize = statement.getFetchSize();
		cursor = mapper.createCursor(
				buf, RowMapper.MappingMode.ROWWISE_SEPARATED_V2,
				rowCount, false, null);
		cursor.setVarDataBaseOffset(varDataBaseOffset);

		if (!followingAccepting && cursor.hasNext()) {
			try {
				firstRow = mapper.createGeneralRow();
				cursor.decode(true, firstRow);
			}
			catch (GSException e) {
				throw SQLErrorUtils.error(0, null, e);
			}
		}
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw SQLErrorUtils.errorUnwrapping();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public boolean next() throws SQLException {
		try {
			if (firstRow != null) {
				currentRow = firstRow;
				firstRow = null;
				return true;
			}
			else if (!cursor.hasNext() && (!followingExists || !fetchFollowing())) {
				lastRow = currentRow;
				currentRow = null;
				return false;
			}

			cursor.decode(true, currentRow);
		}
		catch (GSException e) {
			throw SQLErrorUtils.error(0, null, e);
		}
		catch (NullPointerException e) {
			checkOpened();
			throw e;
		}

		return true;
	}

	@Override
	public void close() throws SQLException {
		final RowMapper.Cursor lastCursor = cursor;

		cursor = null;
		firstRow = null;
		currentRow = null;
		lastRow = null;
		followingExists = false;

		if (lastCursor != null) {
			cleanUnusedResources();
		}
	}

	@Override
	public boolean wasNull() throws SQLException {
		checkOpened();
		if (wasNull == null) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.INVALID_CURSOR_POSITION,
					"No column has been read yet", null);
		}

		return wasNull;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return getValue(columnIndex, String.class, null, null);
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return getValue(columnIndex, Boolean.class, false, null);
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return getValue(columnIndex, Byte.class, (byte) 0, null);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return getValue(columnIndex, Short.class, (short) 0, null);
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return getValue(columnIndex, Integer.class, (int) 0, null);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return getValue(columnIndex, Long.class, (long) 0, null);
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return getValue(columnIndex, Float.class, (float) 0, null);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return getValue(columnIndex, Double.class, (double) 0, null);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		return getValue(columnIndex, byte[].class, null, null);
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return getValue(columnIndex, Date.class, null, null);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return getValue(columnIndex, Time.class, null, null);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return getValue(columnIndex, Timestamp.class, null, null);
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return getValue(columnIndex, InputStream.class, null, null);
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return getString(columnLabelToIndex(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(columnLabelToIndex(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return getByte(columnLabelToIndex(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort(columnLabelToIndex(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return getInt(columnLabelToIndex(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return getLong(columnLabelToIndex(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(columnLabelToIndex(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(columnLabelToIndex(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		return getBigDecimal(columnLabelToIndex(columnLabel));
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return getBytes(columnLabelToIndex(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(columnLabelToIndex(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(columnLabelToIndex(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(columnLabelToIndex(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return getAsciiStream(columnLabelToIndex(columnLabel));
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return getUnicodeStream(columnLabelToIndex(columnLabel));
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return getBinaryStream(columnLabelToIndex(columnLabel));
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		checkOpened();
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		checkOpened();
	}

	@Override
	public String getCursorName() throws SQLException {
		checkOpened();
		return null;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkOpened();
		ProxyTargetInstanceFactory proxyTargetInstanceFactory = ProxyTargetInstanceFactory.getInstance();
		return proxyTargetInstanceFactory.getTargetInstance(
				new SQLResultSetMetaData(containerInfo, labelList));
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return getValue(columnIndex, Object.class, null, null);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getObject(columnLabelToIndex(columnLabel));
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		return columnLabelToIndex(columnLabel);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		checkOpened();
		return (firstRow != null);
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		checkOpened();
		return (lastRow != null);
	}

	@Override
	public boolean isFirst() throws SQLException {
		checkOpened();
		return (currentRow != null && !followingFetched &&
				cursor.getRowIndex() == 0);
	}

	@Override
	public boolean isLast() throws SQLException {
		checkOpened();
		return (currentRow != null && !fetchFollowing());
	}

	@Override
	public void beforeFirst() throws SQLException {
		checkOpened();
		throw errorCursorMove();
	}

	@Override
	public void afterLast() throws SQLException {
		checkOpened();
		throw errorCursorMove();
	}

	@Override
	public boolean first() throws SQLException {
		checkOpened();
		throw errorCursorMove();
	}

	@Override
	public boolean last() throws SQLException {
		checkOpened();
		throw errorCursorMove();
	}

	@Override
	public int getRow() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		checkOpened();
		throw errorCursorMove();
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		checkOpened();
		throw errorCursorMove();
	}

	@Override
	public boolean previous() throws SQLException {
		checkOpened();
		throw errorCursorMove();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		checkOpened();

		if (direction != ResultSet.FETCH_FORWARD) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.UNSUPPORTED_PARAMETER_VALUE,
					"Unsupported fetch direction (direction=" + direction + ")",
					null);
		}
	}

	@Override
	public int getFetchDirection() throws SQLException {
		checkOpened();
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		checkOpened();

		final int maxRows =
				(statement.isClosed() ? 0 : statement.getMaxRows());
		if (maxRows != 0 && rows > maxRows) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Fetch size exceeds maxRows (value=" + rows +
					", maxRows=" + maxRows + ")", null);
		}
		else if (rows < 0) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Negative fetch size (value=" + rows + ")", null);
		}

		fetchSize = rows;
	}

	@Override
	public int getFetchSize() throws SQLException {
		checkOpened();
		return fetchSize;
	}

	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void insertRow() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateRow() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void deleteRow() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void refreshRow() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Statement getStatement() throws SQLException {
		checkOpened();
		return (statementOwned ? null : statement);
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		return getValue(columnIndex, Blob.class, null, null);
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		return getValue(
				columnLabelToIndex(columnLabel), Blob.class, null, null);
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		return getValue(columnIndex, Date.class, null, cal);
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return getValue(
				columnLabelToIndex(columnLabel), Date.class, null, cal);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return getValue(columnIndex, Time.class, null, cal);
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return getValue(
				columnLabelToIndex(columnLabel), Time.class, null, cal);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		return getValue(columnIndex, Timestamp.class, null, cal);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		return getValue(
				columnLabelToIndex(columnLabel), Timestamp.class, null, cal);
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public int getHoldability() throws SQLException {
		return CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return cursor == null;
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	private void checkOpened() throws SQLException {
		if (cursor == null) {
			throw SQLErrorUtils.errorAlreadyClosed();
		}
	}

	private SQLException errorCursorMove() {
		return SQLErrorUtils.error(
				SQLErrorUtils.ILLEGAL_STATE,
				"Failed to move cursor because of forward only", null);
	}

	private boolean fetchFollowing() throws SQLException {
		if (followingExists && !cursor.hasNext()) {
			if (statement.isClosed()) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ALREADY_CLOSED,
						"Related statement already closed", null);
			}

			SQLResultSet rs = null;
			try {
				rs = statement.fetchFollowing(queryId);
			}
			finally {
				if (rs == null) {
					close();
				}
			}

			followingExists = rs.followingExists;
			cursor = rs.cursor;

			cleanUnusedResources();

			if (!cursor.hasNext()) {
				return false;
			}

			followingFetched = true;
			return true;
		}

		return cursor.hasNext();
	}

	private void cleanUnusedResources() throws SQLException {
		boolean succeeded = false;
		try {
			if (statementOwned && !followingExists) {
				statement.closeWithoutResultSet();
			}
			succeeded = true;
		}
		finally {
			if (!succeeded) {
				close();
			}
		}
	}

	private <T> T getValue(
			int columnIndex, Class<T> type, T defaultValue, Calendar cal)
			throws SQLException {
		try {
			final Object value;
			try {
				value = currentRow.getValue(columnIndex - 1);
			}
			catch (NullPointerException e) {
				checkOpened();
				if (currentRow == null) {
					throw SQLErrorUtils.error(
							SQLErrorUtils.INVALID_CURSOR_POSITION,
							"Invalid cursor position (" +
							"beforeFirst=" + isBeforeFirst() +
							", afterLast=" + isAfterLast() +
							", first=" + isFirst() + ")", null);
				}
				throw e;
			}

			wasNull = false;

			if (!type.isInstance(value)) {
				if (value == null) {
					wasNull = true;
					return defaultValue;
				}
				return convertValueType(type, value, cal);
			}

			@SuppressWarnings("unchecked")
			T typedValue = (T) value;
			return typedValue;
		}
		catch (GSException e) {
			final int columnCount = containerInfo.getColumnCount();

			if (columnIndex <= 0 || columnIndex > columnCount) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.COLUMN_INDEX_OUT_OF_RANGE,
						"Column index out of range (columnIndex=" +
						columnIndex + ", columnCount=" + columnCount + ")", e);
			}

			throw SQLErrorUtils.error(0, null, e);
		}
	}

	private <T> T convertValueType(
			Class<T> type, Object value, Calendar cal) throws SQLException {
		if (type.isInstance(value)) {
			return type.cast(value);
		}

		Exception cause = null;
		do {
			final Object destValue;
			if (Number.class.isAssignableFrom(type)) {
				final Number srcNumber;
				if (value instanceof Boolean) {
					if (type == Double.class || type == Float.class) {
						break;
					}
					srcNumber = ((Boolean) value) ? 1 : 0;
				}
				else if (value instanceof Number) {
					srcNumber = (Number) value;
				}
				else if (value instanceof String) {
					final String stringValue = (String) value;
					try {
						if (INTEGER_PATTERN.matcher(stringValue).matches()) {
							srcNumber = Long.parseLong(stringValue);
						}
						else {
							final String lowerValue =
									stringValue.toLowerCase(Locale.US);
							if (lowerValue.equals("infinity")) {
								srcNumber = Double.POSITIVE_INFINITY;
							}
							else if (lowerValue.equals("-infinity")) {
								srcNumber = Double.NEGATIVE_INFINITY;
							}
							else if (lowerValue.equals("nan")) {
								srcNumber = Double.NaN;
							}
							else {
								srcNumber = Double.parseDouble(stringValue);
							}
						}
					}
					catch (NumberFormatException e) {
						cause = e;
						break;
					}
				}
				else {
					break;
				}

				try {
					if (type == Long.class) {
						destValue = (long) convertNumber(
								Long.MIN_VALUE, Long.MAX_VALUE, srcNumber);
					}
					else if (type == Integer.class) {
						destValue = (int) convertNumber(
								Integer.MIN_VALUE, Integer.MAX_VALUE,
								srcNumber);
					}
					else if (type == Double.class) {
						destValue = srcNumber.doubleValue();
					}
					else if (type == Float.class) {
						destValue = (float) convertNumber(
								Float.MIN_VALUE, Float.MAX_VALUE,
								srcNumber);
					}
					else if (type == Short.class) {
						destValue = (short) convertNumber(
								Short.MIN_VALUE, Short.MAX_VALUE,
								srcNumber);
					}
					else if (type == Byte.class) {
						destValue = (byte) convertNumber(
								Byte.MIN_VALUE, Byte.MAX_VALUE,
								srcNumber);
					}
					else {
						break;
					}
				}
				catch (SQLException e) {
					cause = e;
					break;
				}
			}
			else if (type == String.class) {
				if (value instanceof Boolean) {
					destValue = value.toString();
				}
				else if (value instanceof Number) {
					destValue = value.toString();
				}
				else if (value instanceof java.util.Date) {
					destValue = TimestampUtils.format(
							(java.util.Date) value, resolveTimeZoneOffset());
				}
				else {
					final byte[] bytesValue;
					try {
						bytesValue =
								convertValueType(byte[].class, value, cal);
					}
					catch (SQLException e) {
						cause = e;
						break;
					}
					destValue = bytesToHexString(bytesValue);
				}
			}
			else if (type == Boolean.class) {
				if (value instanceof Number) {
					if (value instanceof Double || value instanceof Float) {
						break;
					}

					final long srcNumber;
					try {
						srcNumber = convertValueType(Long.class, value, cal);
					}
					catch (SQLException e) {
						cause = e;
						break;
					}

					if (srcNumber == 0) {
						destValue = false;
					}
					else if (srcNumber == 1) {
						destValue = true;
					}
					else {
						
						destValue = true;
					}
				}
				else if (value instanceof String) {
					final String stringValue;
					try {
						stringValue =
								convertValueType(String.class, value, cal);
					}
					catch (SQLException e) {
						cause = e;
						break;
					}

					if (stringValue.contains("0")) {
						destValue = false;
					}
					else if (stringValue.contains("1")) {
						destValue = true;
					}
					else {
						final String lower =
								stringValue.toLowerCase(Locale.US);
						if (lower.equals("false")) {
							destValue = false;
						}
						else if (lower.equals("true")) {
							destValue = true;
						}
						else {
							cause = SQLErrorUtils.error(
									SQLErrorUtils.VALUE_TYPE_CONVERSION_FAILED,
									"Neither '0' nor '1' contains and " +
									"neither 'false' nor 'true' matched " +
									"while converting to boolean (" +
									"stringValue=" + stringValue + ")", null);
							break;
						}
					}
				}
				else {
					break;
				}
			}
			else if (type == Date.class) {
				if (value instanceof Number) {
					final long longValue;
					try {
						longValue =
								convertValueType(Long.class, value, cal);
					}
					catch (SQLException e) {
						cause = e;
						break;
					}
					destValue = new Date(longValue);
				}
				else if (value instanceof java.util.Date) {
					destValue = new Date(((java.util.Date) value).getTime());
				}
				else {
					try {
						destValue = parseDate(
								convertValueType(String.class, value, cal),
								cal);
					}
					catch (SQLException e) {
						cause = e;
						break;
					}
				}
			}
			else if (type == Time.class) {
				try {
					destValue = new Time(convertValueType(
							Date.class, value, cal).getTime());
				}
				catch (SQLException e) {
					cause = e;
					break;
				}
			}
			else if (type == Timestamp.class) {
				try {
					destValue = new Timestamp(convertValueType(
							Date.class, value, cal).getTime());
				}
				catch (SQLException e) {
					cause = e;
					break;
				}
			}
			else if (type == Blob.class) {
				final byte[] bytesValue;
				try {
					bytesValue = convertValueType(byte[].class, value, cal);
				}
				catch (SQLException e) {
					cause = e;
					break;
				}
				destValue = new SerialBlob(bytesValue);
			}
			else if (type == byte[].class) {
				if (value instanceof String) {
					try {
						destValue = hexStringToBytes((String) value);
					}
					catch (SQLException e) {
						cause = e;
						break;
					}
				}
				else if (value instanceof Blob) {
					final Blob srcBlob = (Blob) value;
					final int len = (int) srcBlob.length();
					if (len == 0) {
						destValue = new byte[0];
					}
					else {
						destValue = srcBlob.getBytes(1, len);
					}
				}
				else {
					break;
				}
			}
			else if (type == InputStream.class) {
				final byte[] bytesValue;
				try {
					bytesValue = convertValueType(byte[].class, value, cal);
				}
				catch (SQLException e) {
					cause = e;
					break;
				}
				destValue = new ByteArrayInputStream(bytesValue);
			}
			else {
				break;
			}
			return type.cast(destValue);
		}
		while (false);

		throw SQLErrorUtils.error(
				SQLErrorUtils.VALUE_TYPE_CONVERSION_FAILED,
				"Failed to convert value type (" +
				"from=" + value.getClass().getName() +
				", to=" + type.getName() +
				(cause == null ? "" : ", reason=" + cause.getMessage()) + ")",
				cause);
	}

	private int columnLabelToIndex(String columnLabel) throws SQLException {
		if (labelIndexMap == null) {
			labelIndexMap = new HashMap<String, Integer>();
			for (int i = labelList.length; i > 0; i--) {
				final String candidate = labelList[i - 1];
				if (candidate.isEmpty()) {
					continue;
				}
				labelIndexMap.put(
						RowMapper.normalizeSymbolUnchecked(candidate), i);
			}
		}

		final String normalizedColumnLabel;
		try {
			normalizedColumnLabel =
					RowMapper.normalizeSymbolUnchecked(columnLabel);
		}
		catch (NullPointerException e) {
			throw SQLErrorUtils.checkNullParameter(columnLabel, "columnLabel", e);
		}

		final Integer index = labelIndexMap.get(normalizedColumnLabel);
		if (index == null) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Column label not found (label=\"" + columnLabel + "\")",
					null);
		}

		return index;
	}

	private static long convertNumber(
			long min, long max, Number src) throws SQLException {

		if (src instanceof Double || src instanceof Float) {
			final double doubleValue = src.doubleValue();
			final long longValue = (long) doubleValue;
			if (!(Double.isNaN(doubleValue) ||
					doubleValue < min || longValue < min ||
					doubleValue > max || longValue > max)) {
				return longValue;
			}
		}
		else {
			final long longValue = src.longValue();
			if (!(longValue < min || longValue > max)) {
				return longValue;
			}
		}

		throw SQLErrorUtils.error(
				SQLErrorUtils.VALUE_TYPE_CONVERSION_FAILED,
				"Value overflow (value=" + src + ")",
				null);
	}

	private static double convertNumber(
			double min, double max, Number src) throws SQLException {
		final double doubleValue = src.doubleValue();
		if (Double.isInfinite(doubleValue) ||
				!(doubleValue < min || doubleValue > max)) {
			return doubleValue;
		}

		throw SQLErrorUtils.error(
				SQLErrorUtils.VALUE_TYPE_CONVERSION_FAILED,
				"Value overflow (value=" + src + ")",
				null);
	}

	private Date parseDate(String stringValue, Calendar cal)
			throws SQLException {
		final TimeZone zoneInValue;
		final String nonZonedValue;
		{
			final Matcher zoneMatcher =
					PropertyUtils.getTimeZoneOffsetPattern().matcher(
							stringValue);
			if (zoneMatcher.find() &&
					zoneMatcher.end() == stringValue.length()) {
				try {
					zoneInValue = PropertyUtils.parseTimeZoneOffset(
							zoneMatcher.group(), false);
				}
				catch (GSException e) {
					throw SQLErrorUtils.error(0, null, e);
				}
				nonZonedValue = stringValue.substring(0, zoneMatcher.start());
			}
			else {
				if (cal == null) {
					zoneInValue = resolveTimeZoneOffset();
				}
				else {
					zoneInValue = cal.getTimeZone();
				}
				nonZonedValue = stringValue;
			}
		}

		for (String formatString : new String[] {
			"yyyy-MM-dd'T'HH:mm:ss.SSS",
			"yyyy-MM-dd'T'HH:mm:ss",
			"yyyy-MM-dd HH:mm:ss.SSS",
			"yyyy-MM-dd HH:mm:ss",
			"yyyy-MM-dd",
			"HH:mm:ss.SSS",
			"HH:mm:ss",
		}) {
			for (String zoneString : new String[] {
					"",
					"z",
					"Z",
					"'Z'"
				}) {
				final DateFormat format = new SimpleDateFormat(
						formatString + zoneString, Locale.ROOT);
				format.setLenient(false);

				if (cal != null) {
					format.setCalendar((Calendar) cal.clone());
				}

				final TimeZone zone;
				final String parsingValue;
				if (zoneString.isEmpty()) {
					zone = zoneInValue;
					parsingValue = nonZonedValue;
				}
				else {
					zone = PropertyUtils.createTimeZoneOffset(0);
					parsingValue = stringValue;
				}
				format.setTimeZone(zone);

				final ParsePosition pos = new ParsePosition(0);
				final java.util.Date parsed = format.parse(parsingValue, pos);
				if (parsed == null || pos.getIndex() != parsingValue.length()) {
					continue;
				}

				return new Date(parsed.getTime());
			}
		}

		throw SQLErrorUtils.error(
				SQLErrorUtils.VALUE_TYPE_CONVERSION_FAILED,
				"Failed to parse date (stringValue=" + stringValue + ")",
				null);
	}

	private static byte[] hexStringToBytes(
			CharSequence sequence) throws SQLException {
		final int strLen = sequence.length();
		if (strLen % 2 != 0) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.VALUE_TYPE_CONVERSION_FAILED,
					"Illegal length as hex string (length=" + strLen +
					", stringValue=\"" + sequence + "\")",
					null);
		}

		final byte[] bytes = new byte[strLen / 2];
		for (int i = 0; i < strLen; i += 2) {
			final String part = sequence.subSequence(i, i + 2).toString();
			try {
				bytes[i / 2] = (byte) (Integer.parseInt(part, 16) & 0xff);
			}
			catch (NumberFormatException e) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.VALUE_TYPE_CONVERSION_FAILED,
						"Illegal hex string part found (part=\"" + i +
						"\", stringValue=\"" + sequence + "\")",
						e);
			}
		}

		return bytes;
	}

	private static String bytesToHexString(byte[] bytes) {
		final StringBuilder builder = new StringBuilder(bytes.length + 2);
		final Formatter formatter = new Formatter(builder);

		for (byte b : bytes) {
			formatter.format(Locale.US, "%02x", b & 0xff);
		}

		return formatter.toString();
	}

	private TimeZone resolveTimeZoneOffset() {
		final TimeZone zone = statement.getTimeZoneOffset();
		if (zone == null) {
			return PropertyUtils.createTimeZoneOffset(0);
		}
		else {
			return zone;
		}
	}

	void setStatementOwned(boolean statementOwned) throws SQLException {
		this.statementOwned = statementOwned;
		cleanUnusedResources();
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

}
