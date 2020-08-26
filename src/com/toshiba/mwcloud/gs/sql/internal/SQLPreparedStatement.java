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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;

import com.toshiba.mwcloud.gs.sql.internal.RowMapper.MappingMode;
import com.toshiba.mwcloud.gs.sql.internal.SQLLaterFeatures.LaterPreparedStatement;
import com.toshiba.mwcloud.gs.sql.internal.proxy.ProxyTargetInstanceFactory;

class SQLPreparedStatement
extends SQLStatement implements PreparedStatement, LaterPreparedStatement {

	static boolean neverPrepare = false;

	private int parameterCount;

	private RowMapper mapper;

	private ContainerInfo containerInfo;

	private List<Row> rowList;

	private BitSet parameterAssignmentSet;

	private Row activeRow;

	private ResultSetMetaData lastMetaData;

	public SQLPreparedStatement(
			SQLConnection connection, String sql) throws SQLException {
		super(connection);

		boolean succeeded = false;
		try {
			refreshQuery(
					Collections.singletonList(sql),
					StatementOperation.EXECUTE);

			succeeded = true;
		}
		finally {
			if (!succeeded) {
				close();
			}
		}
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		execute(StatementOperation.QUERY, false);
		final ResultSet rs = getResultSet();
		lastMetaData = rs.getMetaData();
		return rs;
	}

	@Override
	public int executeUpdate() throws SQLException {
		execute(StatementOperation.UPDATE, false);
		return getLastResult().updateCount;
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		
		setValue(parameterIndex, null);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		setBlob(parameterIndex, x, length);
	}

	@Override
	public void clearParameters() throws SQLException {
		final Row newRow;
		try {
			newRow = mapper.createGeneralRow();
		}
		catch (GSException e) {
			throw SQLErrorUtils.error(0, null, e);
		}

		activeRow = null;
		rowList.clear();

		activeRow = newRow;
		rowList.add(activeRow);
		parameterAssignmentSet.clear();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public boolean execute() throws SQLException {
		execute(StatementOperation.EXECUTE, false);
		return getLastResult().tableFound;
	}

	@Override
	public void addBatch() throws SQLException {
		if (!batchOperationSupported) {
			throw SQLErrorUtils.errorNotSupported();
		}

		checkParameterAssigned();

		final Row newRow;
		try {
			newRow = mapper.createGeneralRow();
		}
		catch (GSException e) {
			throw SQLErrorUtils.error(0, null, e);
		}

		rowList.add(newRow);
		parameterAssignmentSet.clear();
		activeRow = newRow;
	}

	@Override
	public void setCharacterStream(
			int parameterIndex, Reader reader, int length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		setValue(parameterIndex, x);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkOpened();
		
		if (lastMetaData != null) {
			return lastMetaData;
		}

		ProxyTargetInstanceFactory proxyTargetInstanceFactory = ProxyTargetInstanceFactory.getInstance();
		return proxyTargetInstanceFactory.getTargetInstance(
				new SQLResultSetMetaData(mapper.getContainerInfo(), new String[parameterCount]));
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		ProxyTargetInstanceFactory proxyTargetInstanceFactory = ProxyTargetInstanceFactory.getInstance();
		return proxyTargetInstanceFactory.getTargetInstance(new SQLParameterMetaData(containerInfo));
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setNString(int parameterIndex, String value)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {

		if (inputStream == null) {
			setBlob(parameterIndex, (Blob) null);
			return;
		}

		if (length < 0) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Invalid length (" +
					"specified=" + length + ")", null);
		}

		final Blob blob = convertToBlob(inputStream, length);

		setBlob(parameterIndex, blob);
	}

	private Blob convertToBlob(InputStream inputStream, long length)
			throws SQLException {
		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final byte[] buffer = new byte[1024000];

		int count = 0;
		long remaining = length;
		if (length < 0) {
			remaining = Long.MAX_VALUE;
		}

		try {
			int readLen = buffer.length;

			if (remaining < buffer.length) {
				readLen = (int) remaining;
			}
			else {
				readLen = buffer.length;
			}
			while (remaining > 0 &&
					(count = inputStream.read(buffer, 0, readLen)) != -1) {
				remaining -= count;
				output.write(buffer, 0, count);

				if (remaining < buffer.length) {
					readLen = (int) remaining;
				}
				else {
					readLen = buffer.length;
				}
			}
		}
		catch (IOException e) {
			throw SQLErrorUtils.error(0, null, e);
		}

		if (!(length < 0) && remaining > 0) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Invalid length (" +
					"specified=" + length + ")", null);
		}

		final byte[] contents = output.toByteArray();
		final Blob blob = new SerialBlob(contents);

		return blob;
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scaleOrLength) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		setBlob(parameterIndex, x, length);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		setBlob(parameterIndex, x);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		if (inputStream == null) {
			setBlob(parameterIndex, (Blob) null);
			return;
		}

		final Blob blob = convertToBlob(inputStream, -1);
		setBlob(parameterIndex, blob);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		throw sqlAlteringError();
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		throw sqlAlteringError();
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		throw sqlAlteringError();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw sqlAlteringError();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		throw sqlAlteringError();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		throw sqlAlteringError();
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw sqlAlteringError();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes)
			throws SQLException {
		throw sqlAlteringError();
	}

	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		throw sqlAlteringError();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		throw sqlAlteringError();
	}

	@Override
	public void clearBatch() throws SQLException {
		checkOpened();
		clearResults();

		clearParameters();
	}

	protected boolean refreshQuery(
			List<String> sqlList,
			StatementOperation statementOp) throws SQLException {
		if (statementOp == null) {
			return false;
		}

		switch (statementOp) {
		case EXECUTE:
		case QUERY:
		case UPDATE:
			break;
		default:
			return false;
		}

		if (sqlList.size() != 1) {
			throw new Error();
		}
		setQueryDirect(sqlList.get(0));

		final int parameterCount;
		final BasicBuffer schemaBuffer;
		if (neverPrepare) {
			schemaBuffer = null;
			parameterCount = 0;
		}
		else {
			try {
				execute(StatementOperation.PREPARE, true);
				final Result result = getLastResult();
				schemaBuffer = result.getSchemaBuffer();
				parameterCount = result.parameterCount;
			}
			finally {
				clearResults();
			}
		}

		ContainerInfo containerInfo;
		if (schemaBuffer == null) {
			final List<ColumnInfo> columnInfoList = Collections.nCopies(
					parameterCount, new ColumnInfo(null, null));
			containerInfo =
					new ContainerInfo(null, null, columnInfoList, false);
		}
		else {
			containerInfo = null;
		}

		final RowMapper mapper;
		final List<Row> rowList;
		try {
			if (containerInfo == null) {
				mapper = RowMapper.getInstance(
						schemaBuffer, null, getRowMapperConfig());
			}
			else {
				mapper = RowMapper.getInstance(
						null, containerInfo, getRowMapperConfig());
			}
			containerInfo = mapper.getContainerInfo();
			rowList = rebuildRowList(
					mapper, this.parameterAssignmentSet, this.rowList);
		}
		catch (GSException e) {
			throw SQLErrorUtils.error(0, null, e);
		}

		final boolean inputCompleted =
				(this.activeRow == null && this.rowList != null);
		final Row activeRow = (inputCompleted ?
				null : rowList.get(rowList.size() - 1));
		final BitSet parameterAssignmentSet =
				(this.parameterAssignmentSet == null ?
						new BitSet(parameterCount) :
						this.parameterAssignmentSet);

		this.rowList = rowList;
		this.parameterCount = parameterCount;
		this.mapper = mapper;
		this.containerInfo = containerInfo;
		this.parameterAssignmentSet = parameterAssignmentSet;
		this.activeRow = activeRow;

		return true;
	}

	private static List<Row> rebuildRowList(
			RowMapper mapper, BitSet assignmentSet, List<Row> srcList)
			throws GSException {
		final int columnCount = mapper.getContainerInfo().getColumnCount();
		final List<Row> destList = new ArrayList<Row>();

		if (srcList != null) {
			for (Iterator<Row> it = srcList.iterator(); it.hasNext();) {
				final Row srcRow = it.next();
				final Row destRow = mapper.createGeneralRow();
				final boolean allAssigned = it.hasNext();

				for (int column = 0; column < columnCount; column++) {
					if (!allAssigned && !assignmentSet.get(column)) {
						continue;
					}
					destRow.setValue(column, srcRow.getValue(column));
				}
				destList.add(srcRow);
			}
		}

		if (destList.isEmpty()) {
			destList.add(mapper.createGeneralRow());
		}

		return destList;
	}

	@Override
	protected void completeInputTable() throws SQLException {
		checkParameterUnassigned();
		activeRow = null;
	}

	protected void putInputTable(BasicBuffer out) throws SQLException {
		final int baseRowCount = (rowList == null ? 0 : rowList.size());
		final boolean inputCompleted = (activeRow == null);

		final int rowCount = baseRowCount - (inputCompleted ? 1 : 0);
		final boolean found = (rowCount > 0);
		out.putBoolean(found);

		if (!found) {
			return;
		}

		if (!inputCompleted) {
			checkParameterAssigned();
		}

		try {
			final int dummySize = 0;
			{
				out.putLong((long) rowCount);

				out.putInt(dummySize);
				final int schemaPos = out.base().position();

				mapper.exportSchema(out, getRowMapperConfig());
				for (int i = 0; i < parameterCount; i++) {
					final String label = "";
					out.putString(label);
				}

				final int schemaSize = out.base().position() - schemaPos;
				out.base().position(schemaPos - Integer.SIZE / Byte.SIZE);
				out.putInt(schemaSize);
				out.base().position(schemaPos + schemaSize);
			}

			{
				out.putInt(dummySize);
				final int tablePos = out.base().position();

				final long varDataBaseOffset = 0;
				out.putLong(varDataBaseOffset);

				final RowMapper.Cursor cursor = mapper.createCursor(
						out, MappingMode.ROWWISE_SEPARATED_V2, rowCount,
						false, null);
				for (Row row : rowList) {
					mapper.encode(cursor, null, row);
				}

				final int tableSize = out.base().position() - tablePos;
				out.base().position(tablePos - Integer.SIZE / Byte.SIZE);
				out.putInt(tableSize);
				out.base().position(tablePos + tableSize);
			}
		}
		catch (GSException e) {
			throw SQLErrorUtils.error(0, null, e);
		}
	}

	@Override
	protected void clearInputTable() throws SQLException {
		if (activeRow == null && rowList != null && !rowList.isEmpty()) {
			activeRow = rowList.get(rowList.size() - 1);
			rowList.clear();
			rowList.add(activeRow);
		}
	}

	private SQLException sqlAlteringError() {
		return SQLErrorUtils.error(
				SQLErrorUtils.ILLEGAL_STATE,
				"SQL can not be altered on PreparedStatement", null);
	}

	private void setValue(
			int parameterIndex, Object value) throws SQLException {
		try {
			activeRow.setValue(parameterIndex - 1, value);
		}
		catch (GSException e) {
			if (parameterIndex <= 0 || parameterIndex > parameterCount) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_PARAMETER,
						"Parameter index out of range (" +
						"specified=" + parameterIndex +
						", parameterCount=" + parameterCount + ")", null);
			}

			throw SQLErrorUtils.error(0, null, e);
		}
		parameterAssignmentSet.set(parameterIndex - 1);
	}

	private void checkParameterAssigned() throws SQLException {
		final int unassignedPos = parameterAssignmentSet.nextClearBit(0);
		if (unassignedPos < parameterCount) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Parameter is not assigned (" +
					"parameterIndex=" + (unassignedPos + 1) +
					", parameterCount=" + parameterCount + ")", null);
		}
	}

	private void checkParameterUnassigned() throws SQLException {
		final int assignedPos = parameterAssignmentSet.nextSetBit(0);
		if (assignedPos >= 0) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Batch not added while parameter is assigned (" +
					"assignedParameterIndex=" + (assignedPos + 1) +
					", parameterCount=" + parameterCount + ")", null);
		}
	}

}
