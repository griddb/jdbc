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

import java.sql.Blob;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import com.toshiba.mwcloud.gs.sql.internal.SQLLaterFeatures.LaterResultSetMetaData;

class SQLResultSetMetaData
implements ResultSetMetaData, LaterResultSetMetaData {

	private final ContainerInfo info;

	private final String[] labelList;

	private boolean forParameter;

	SQLResultSetMetaData(ContainerInfo info, String[] labelList) {
		this.info = info;
		this.labelList = labelList;
	}

	void setForParameter(boolean forParameter) {
		this.forParameter = forParameter;
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
	public int getColumnCount() throws SQLException {
		return info.getColumnCount();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		checkColumn(column);
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		checkColumn(column);
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		checkColumn(column);
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		checkColumn(column);
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		checkColumn(column);
		if (getGSColumnType(column) == null ||
				Boolean.TRUE.equals(
						info.getColumnInfo(column - 1).getNullable())) {
			return columnNullable;
		}
		else {
			return columnNoNulls;
		}
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		checkColumn(column);
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		checkColumn(column);
		
		
		return 128 * 1024;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		checkColumn(column);
		return labelList[column - 1];
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return getColumnLabel(column);
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		checkColumn(column);
		return "";
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		checkColumn(column);
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		checkColumn(column);
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		checkColumn(column);
		return "";
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		checkColumn(column);
		return "";
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		checkColumn(column);

		final GSType gsType = getGSColumnType(column);
		if (gsType == null) {
			return Types.OTHER;
		}

		switch (gsType) {
		case BOOL:
			return Types.BIT;
		case BYTE:
			return Types.TINYINT;
		case SHORT:
			return Types.SMALLINT;
		case INTEGER:
			return Types.INTEGER;
		case LONG:
			return Types.BIGINT;
		case FLOAT:
			return Types.FLOAT;
		case DOUBLE:
			return Types.DOUBLE;
		case TIMESTAMP:
			return Types.TIMESTAMP;
		case STRING:
			return Types.VARCHAR;
		case BLOB:
			return Types.BLOB;
		}

		throw errorColumnType(gsType);
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		checkColumn(column);

		final GSType gsType = getGSColumnType(column);
		if (gsType == null) {
			return "UNKNOWN";
		}

		switch (gsType) {
		case BOOL:
			return "BOOL";
		case BYTE:
			return "BYTE";
		case SHORT:
			return "SHORT";
		case INTEGER:
			return "INTEGER";
		case LONG:
			return "LONG";
		case FLOAT:
			return "FLOAT";
		case DOUBLE:
			return "DOUBLE";
		case TIMESTAMP:
			return "TIMESTAMP";
		case STRING:
			return "STRING";
		case BLOB:
			return "BLOB";
		}

		throw errorColumnType(gsType);
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		checkColumn(column);
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		checkColumn(column);
		return true;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		checkColumn(column);
		return true;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		checkColumn(column);

		final GSType gsType = getGSColumnType(column);
		if (gsType == null) {
			return Object.class.getName();
		}

		switch (gsType) {
		case BOOL:
			return Boolean.class.getName();
		case BYTE:
			return Byte.class.getName();
		case SHORT:
			return Short.class.getName();
		case INTEGER:
			return Integer.class.getName();
		case LONG:
			return Long.class.getName();
		case FLOAT:
			return Float.class.getName();
		case DOUBLE:
			return Double.class.getName();
		case TIMESTAMP:
			return java.util.Date.class.getName();
		case STRING:
			return String.class.getName();
		case BLOB:
			return Blob.class.getName();
		}

		throw errorColumnType(gsType);
	}

	private void checkColumn(int column) throws SQLException {
		final int columnCount = info.getColumnCount();
		if (column <= 0 || column > columnCount) {
			if (forParameter) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_PARAMETER,
						"Parameter index out of range (" +
						"specified=" + column +
						", parameterCount=" + columnCount + ")", null);
			}
			else {
				throw SQLErrorUtils.error(
						SQLErrorUtils.COLUMN_INDEX_OUT_OF_RANGE,
						"Column index out of range (columnIndex=" + column +
						", columnCount=" + columnCount + ")", null);
			}
		}
	}

	private GSType getGSColumnType(int column) throws SQLException {
		return info.getColumnInfo(column - 1).getType();
	}

	private static SQLException errorColumnType(
			GSType type) throws SQLException {
		return SQLErrorUtils.error(SQLErrorUtils.MESSAGE_CORRUPTED,
				"Internal data error by unrecognized type (type=" +
						type + ")", null);
	}

}
