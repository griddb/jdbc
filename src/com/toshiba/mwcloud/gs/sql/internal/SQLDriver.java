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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Logger;

import com.toshiba.mwcloud.gs.sql.Driver.Key;
import com.toshiba.mwcloud.gs.sql.internal.SQLLaterFeatures.LaterDriver;
import com.toshiba.mwcloud.gs.sql.internal.proxy.ProxyTargetInstanceFactory;

public class SQLDriver implements java.sql.Driver, LaterDriver {

	private static final String DRIVER_SCHEME = "jdbc";
	private static final String GS_SCHEME = "gs";

	public SQLDriver(Key key) throws SQLException {
		Key.validate(key);
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}

		ProxyTargetInstanceFactory proxyTargetInstanceFactory = ProxyTargetInstanceFactory.getInstance();
		return proxyTargetInstanceFactory.getTargetInstance(new SQLConnection(url, info));
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if (url == null) {
			return false;
		}

		final String jdbcScheme;
		final String gsScheme;
		try {
			final URI driverURI = SQLConnection.toDriverURI(url);

			jdbcScheme = driverURI.getScheme();
			gsScheme = SQLConnection.toSpecificURI(driverURI).getScheme();
		}
		catch (URISyntaxException e) {
			return false;
		}

		return (jdbcScheme != null && gsScheme != null &&
				jdbcScheme.toLowerCase(Locale.US).equals(DRIVER_SCHEME) &&
				gsScheme.toLowerCase(Locale.US).equals(GS_SCHEME));
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {
		
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion() {
		return SQLDatabaseMetaData.DriverInfo.getInstance().driverMajorVersion;
	}

	@Override
	public int getMinorVersion() {
		return SQLDatabaseMetaData.DriverInfo.getInstance().driverMinorVersion;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

}
