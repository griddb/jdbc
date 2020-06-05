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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

class SQLLaterFeatures {

	private SQLLaterFeatures() {
	}

	public interface LaterDriver {

		public Logger getParentLogger() throws SQLFeatureNotSupportedException;

	}

	public interface LaterConnection {

		void setSchema(String schema) throws SQLException;

		String getSchema() throws SQLException;

		void abort(Executor executor) throws SQLException;

		void setNetworkTimeout(Executor executor, int milliseconds)
				throws SQLException;

		int getNetworkTimeout() throws SQLException;

	}

	public interface LaterDatabaseMetadata {

		ResultSet getPseudoColumns(String catalog, String schemaPattern,
				String tableNamePattern, String columnNamePattern)
				throws SQLException;

		boolean generatedKeyAlwaysReturned() throws SQLException;

	}

	public interface LaterResultSet {
		public <T> T getObject(int columnIndex, Class<T> type)
				throws SQLException;

		public <T> T getObject(String columnLabel, Class<T> type)
				throws SQLException;
	}

	public interface LaterResultSetMetaData {

	}

	public interface LaterStatement {
		public void closeOnCompletion() throws SQLException;

		public boolean isCloseOnCompletion() throws SQLException;
	}

	public interface LaterPreparedStatement {

	}

	public interface LaterParameterMetaData {

	}

}
