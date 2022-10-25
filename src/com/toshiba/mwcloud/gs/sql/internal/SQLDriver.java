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

import java.io.IOException;
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
import com.toshiba.mwcloud.gs.sql.internal.common.DriverProvider;
import com.toshiba.mwcloud.gs.sql.internal.common.DriverProvider.ExceptionFactory;
import com.toshiba.mwcloud.gs.sql.internal.common.DriverProvider.ExtensibleDriver;
import com.toshiba.mwcloud.gs.sql.internal.common.DriverProvider.TransportProvider;
import com.toshiba.mwcloud.gs.sql.internal.proxy.ProxyTargetInstanceFactory;

public class SQLDriver implements ExtensibleDriver {

	private static final String DRIVER_SCHEME = "jdbc";
	private static final String GS_SCHEME = "gs";

	private final SQLConnection.Options options =
			new SQLConnection.Options(null);

	private Throwable initalError;

	private SQLDriver(Key key) throws SQLException {
		Key.validate(key, true);
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}

		if (initalError != null) {
			throw SQLErrorUtils.error(0, null, initalError);
		}

		final SQLConnection connection =
				new SQLConnection(url, info, getOptions());
	
		final ProxyTargetInstanceFactory factory =
				ProxyTargetInstanceFactory.getInstance();
		return factory.getTargetInstance(connection);
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

	@Override
	public void setTransportProvider(TransportProvider provider) {
		synchronized (options) {
			options.setTransportProvider(provider);
		}
	}

	@Override
	public ExceptionFactory getExceptionFactory() {
		return new ExceptionFactoryImpl(0);
	}

	@Override
	public void setInitialError(Throwable initalError) {
		if (this.initalError == null) {
			this.initalError = initalError;
		}
	}

	private SQLConnection.Options getOptions() {
		synchronized (options) {
			return new SQLConnection.Options(options);
		}
	}

	public static class SQLDriverProvider extends DriverProvider {

		@Override
		public ExtensibleDriver newDriver(DriverOptions options)
				throws SQLException {
			return new SQLDriver(options.getKey());
		}

	}

	private static class ExceptionFactoryImpl implements ExceptionFactory {

		private final int errorCode;

		private ExceptionFactoryImpl(int errorCode) {
			this.errorCode = errorCode;
		}

		@Override
		public IOException create(String message, Throwable cause) {
			return new GSException(errorCode, message, cause);
		}

		@Override
		public ExceptionFactory asIllegalPropertyEntry() {
			return new ExceptionFactoryImpl(
					GSErrorCode.ILLEGAL_PROPERTY_ENTRY);
		}

	}

}
