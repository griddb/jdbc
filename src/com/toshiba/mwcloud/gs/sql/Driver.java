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
package com.toshiba.mwcloud.gs.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import com.toshiba.mwcloud.gs.sql.internal.SQLDriver;

public class Driver implements java.sql.Driver, LaterDriver {

	private final SQLDriver impl;

	static {
		try {
			DriverManager.registerDriver(new Driver());
		}
		catch (SQLException e) {
			throw new Error(e);
		}
	}

	{
		try {
			impl = new SQLDriver(Key.KEY);
		}
		catch (SQLException e) {
			throw new Error(e);
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		return impl.connect(url, info);
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return impl.acceptsURL(url);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {
		return impl.getPropertyInfo(url, info);
	}

	@Override
	public int getMajorVersion() {
		return impl.getMajorVersion();
	}

	@Override
	public int getMinorVersion() {
		return impl.getMinorVersion();
	}

	@Override
	public boolean jdbcCompliant() {
		return impl.jdbcCompliant();
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return impl.getParentLogger();
	}

	public static final class Key {

		private static final Key KEY = new Key();

		private Key() {
		}

		public static void validate(Key key) throws SQLException {
			if (key != KEY) {
				throw new SQLException("Unexpected access for this driver");
			}
		}

	}

}

interface LaterDriver {

	public Logger getParentLogger() throws SQLFeatureNotSupportedException;

}
