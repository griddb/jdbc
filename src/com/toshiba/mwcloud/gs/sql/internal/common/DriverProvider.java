/*
   Copyright (c) 2020 TOSHIBA Digital Solutions Corporation

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
package com.toshiba.mwcloud.gs.sql.internal.common;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.logging.Logger;

import javax.net.ssl.SSLSocketFactory;

import com.toshiba.mwcloud.gs.sql.Driver;

public abstract class DriverProvider {

	public abstract ExtensibleDriver newDriver(DriverOptions options)
			throws SQLException;

	public interface ExtensibleDriver extends java.sql.Driver, LaterDriver {

		public void setTransportProvider(TransportProvider provider);

		public ExceptionFactory getExceptionFactory();

		public void setInitialError(Throwable initalError);

	}

	public static abstract class ChainDriver implements ExtensibleDriver {

		private final ExtensibleDriver base;

		protected ChainDriver(ExtensibleDriver base) {
			this.base = base;
		}

		@Override
		public Connection connect(String url, Properties info)
				throws SQLException {
			return base.connect(url, info);
		}

		@Override
		public boolean acceptsURL(String url) throws SQLException {
			return base.acceptsURL(url);
		}

		@Override
		public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
				throws SQLException {
			return base.getPropertyInfo(url, info);
		}

		@Override
		public int getMajorVersion() {
			return base.getMajorVersion();
		}

		@Override
		public int getMinorVersion() {
			return base.getMinorVersion();
		}

		@Override
		public boolean jdbcCompliant() {
			return base.jdbcCompliant();
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return base.getParentLogger();
		}

		@Override
		public void setTransportProvider(TransportProvider provider) {
			base.setTransportProvider(provider);
		}

		@Override
		public ExceptionFactory getExceptionFactory() {
			return base.getExceptionFactory();
		}

		@Override
		public void setInitialError(Throwable initalError) {
			base.setInitialError(initalError);
		}

	}

	private interface LaterDriver {

		public Logger getParentLogger() throws SQLFeatureNotSupportedException;

	}

	public interface DriverOptions {

		public Driver.Key getKey();

		public ExtensibleDriver getChainDriver();

	}

	public interface ExceptionFactory {

		public IOException create(String message, Throwable cause);

		public ExceptionFactory asIllegalPropertyEntry();

	}

	public interface TransportProvider {

		public void filterProperties(
				Properties src, Properties transProps) throws IOException;

		public boolean isPlainSocketAllowed(
				Properties props) throws IOException;

		public SSLSocketFactory createSecureSocketFactory(
				Properties props) throws IOException;

	}

	public static class ProviderUtils {

		public static List<DriverProvider> load(Class<?> baseClass) {
			return load(DriverProvider.class, listClassLoaders(baseClass));
		}

		public static <P> List<P> load(
				Class<P> providerClass, List<ClassLoader> classLoaderList) {
			final Set<Class<?>> visitedClasses = new HashSet<Class<?>>();
			final List<P> providerList = new ArrayList<P>();
			for (ClassLoader cl : classLoaderList) {
				for (P provider : ServiceLoader.load(providerClass, cl)) {
					final Class<?> actualClass = provider.getClass();
					if (visitedClasses.contains(actualClass)) {
						continue;
					}
					visitedClasses.add(actualClass);
					providerList.add(provider);
				}
			}
			return providerList;
		}

		public static List<ClassLoader> listClassLoaders(Class<?> baseClass) {
			return Arrays.asList(
					baseClass.getClassLoader(),
					Thread.currentThread().getContextClassLoader(),
					ClassLoader.getSystemClassLoader());
		}

	}

}
