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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.toshiba.mwcloud.gs.sql.internal.SQLDriver;
import com.toshiba.mwcloud.gs.sql.internal.common.DriverProvider;
import com.toshiba.mwcloud.gs.sql.internal.common.DriverProvider.ExtensibleDriver;

public class Driver extends DriverProvider.ChainDriver {

	private static final ExtensibleDriver CHAIN_DRIVER;

	private static final Throwable DRIVER_ERROR;

	static {
		Throwable driverError = null;
		try {
			CHAIN_DRIVER = loadChain();
			DriverManager.registerDriver(new Driver());
		}
		catch (Throwable t) {
			driverError = t;
			throw new Error(t);
		}
		finally {
			DRIVER_ERROR = driverError;
		}
	}

	public Driver() throws SQLException {
		super(getChain());
	}

	private static ExtensibleDriver getChain() throws SQLException {
		final ExtensibleDriver chain = CHAIN_DRIVER;
		final Throwable driverError = DRIVER_ERROR;
		if (chain == null || driverError != null) {
			throw new SQLException(driverError);
		}
		return chain;
	}

	private static ExtensibleDriver loadChain() throws SQLException {
		final ExtensibleDriver[] driverRef = { null };
		final DriverProvider.DriverOptions options =
				new DriverProvider.DriverOptions() {

			@Override
			public Driver.Key getKey() {
				final boolean builtin = (getChainDriver() == null);
				return (builtin ? Key.BUILTIN_KEY : Key.SUB_KEY);
			}

			@Override
			public ExtensibleDriver getChainDriver() {
				return driverRef[0];
			}

		};

		Throwable initialError = null;

		final List<DriverProvider> providerList =
				new ArrayList<DriverProvider>();
		providerList.add(new SQLDriver.SQLDriverProvider());
		do {
			final List<DriverProvider> subProviders;
			try {
				subProviders = DriverProvider.ProviderUtils.load(Driver.class);
			}
			catch (Throwable t) {
				initialError = t;
				break;
			}
			providerList.addAll(subProviders);
		}
		while (false);

		for (DriverProvider provider : providerList) {
			final ExtensibleDriver driver;
			try {
				driver = provider.newDriver(options);
				if (driver == null) {
					throw new Error();
				}
			}
			catch (Throwable t) {
				if (driverRef[0] == null) {
					throw new Error(t);
				}
				initialError = t;
				break;
			}
			driverRef[0] = driver;
		}

		if (initialError != null) {
			driverRef[0].setInitialError(initialError);
		}

		return driverRef[0];
	}

	public static final class Key {

		private static final Key BUILTIN_KEY = new Key();
		private static final Key SUB_KEY = new Key();

		private Key() {
		}

		public static void validate(Key key, boolean builtin)
				throws SQLException {
			if (key != (builtin ? BUILTIN_KEY : SUB_KEY)) {
				throw new SQLException("Unexpected access for this driver");
			}
		}

	}

}
