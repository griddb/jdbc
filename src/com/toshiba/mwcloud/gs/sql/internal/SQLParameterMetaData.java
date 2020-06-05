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

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import com.toshiba.mwcloud.gs.sql.internal.SQLLaterFeatures.LaterParameterMetaData;

class SQLParameterMetaData
implements ParameterMetaData, LaterParameterMetaData {

	private final SQLResultSetMetaData base;

	SQLParameterMetaData(ContainerInfo info) {
		base = new SQLResultSetMetaData(info, null);
		base.setForParameter(true);
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
	public int getParameterCount() throws SQLException {
		return base.getColumnCount();
	}

	@Override
	public int isNullable(int param) throws SQLException {
		return base.isNullable(param);
	}

	@Override
	public boolean isSigned(int param) throws SQLException {
		return base.isSigned(param);
	}

	@Override
	public int getPrecision(int param) throws SQLException {
		return base.getPrecision(param);
	}

	@Override
	public int getScale(int param) throws SQLException {
		return base.getScale(param);
	}

	@Override
	public int getParameterType(int param) throws SQLException {
		return base.getColumnType(param);
	}

	@Override
	public String getParameterTypeName(int param) throws SQLException {
		return base.getColumnTypeName(param);
	}

	@Override
	public String getParameterClassName(int param) throws SQLException {
		return base.getColumnClassName(param);
	}

	@Override
	public int getParameterMode(int param) throws SQLException {
		return parameterModeIn;
	}
}
