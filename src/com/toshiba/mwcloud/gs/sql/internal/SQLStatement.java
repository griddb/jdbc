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

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SimpleTimeZone;

import com.toshiba.mwcloud.gs.sql.internal.NodeConnection.OptionalRequest;
import com.toshiba.mwcloud.gs.sql.internal.NodeConnection.OptionalRequestType;
import com.toshiba.mwcloud.gs.sql.internal.SQLConnection.BaseConnection;
import com.toshiba.mwcloud.gs.sql.internal.SQLConnection.Hook;
import com.toshiba.mwcloud.gs.sql.internal.SQLLaterFeatures.LaterStatement;

class SQLStatement implements Statement, LaterStatement {

	private static final long TIMEOUT_RESOLUTION_MILLIS = 1 * 1000;

	private static final RowMapper.Config DEFAULT_MAPPER_CONFIG =
			new RowMapper.Config(true, true, true, false);

	static boolean batchOperationSupported = false;

	private final SQLConnection connection;

	private boolean closed;

	private QueryReference queryRef = new QueryReference(this);

	private final List<String> queryList;

	private final List<Result> resultList;

	private int lastResultIndex;

	private ResultSet lastResultSet;

	private final BasicBuffer req;

	private BasicBuffer resp;

	private int queryTimeoutSecs;

	private int maxRows;

	private int fetchSize;

	public SQLStatement(SQLConnection connection) {
		this.connection = connection;

		final QueryPool queryPool = connection.getQueryPool();
		queryPool.add(queryRef);
		queryRef.lastBaseConnection = connection.base();
		queryRef.queryId = queryPool.pull(connection);

		queryList = new ArrayList<String>();
		resultList = new ArrayList<Result>();
		req = connection.takeReqBuffer();
		resp = connection.takeRespBuffer();
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
	public ResultSet executeQuery(String sql) throws SQLException {
		checkOpened();
		SQLErrorUtils.checkNullParameter(sql, "sql", null);

		queryList.clear();
		queryList.add(sql);
		execute(StatementOperation.QUERY, true);

		return prepareResultSet(false);
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		checkOpened();
		SQLErrorUtils.checkNullParameter(sql, "sql", null);

		queryList.clear();
		queryList.add(sql);
		execute(StatementOperation.UPDATE, true);

		return getLastResult().updateCount;
	}

	@Override
	public void close() throws SQLException {
		if (closed) {
			return;
		}

		try {
			try {
				if (lastResultSet != null) {
					lastResultSet.close();
				}
			}
			finally {
				lastResultSet = null;
				connection.getQueryPool().detach(queryRef, connection);
			}
		}
		finally {
			closed = true;
		}
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		checkOpened();
		
		return maxRows;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		checkOpened();
		if (max < 0) {
			throw SQLErrorUtils.error(SQLErrorUtils.ILLEGAL_PARAMETER,
					"Negative parameter (value=" + max + ")", null);
		}
		maxRows = max;
	}

	@Override
	public int getMaxRows() throws SQLException {
		checkOpened();
		return maxRows;
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		checkOpened();
		if (max < 0) {
			throw SQLErrorUtils.error(SQLErrorUtils.ILLEGAL_PARAMETER,
					"Negative parameter (value=" + max + ")", null);
		}

		maxRows = max;
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		checkOpened();
		
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		checkOpened();
		return queryTimeoutSecs;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		checkOpened();
		if (seconds < 0) {
			throw SQLErrorUtils.error(SQLErrorUtils.ILLEGAL_PARAMETER,
					"Negative parameter (value=" + seconds + ")", null);
		}
		queryTimeoutSecs = seconds;
	}

	@Override
	public void cancel() throws SQLException {
		checkOpened();

		try {
			connection.getHook().cancelQuery();
		}
		catch (GSException e) {
			throw SQLErrorUtils.error(0, null, e);
		}
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
	public void setCursorName(String name) throws SQLException {
		checkOpened();
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		checkOpened();
		SQLErrorUtils.checkNullParameter(sql, "sql", null);

		queryList.clear();
		queryList.add(sql);
		execute(StatementOperation.EXECUTE, true);

		return getLastResult().tableFound;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		checkOpened();
		return getResultSetDirect(false);
	}

	@Override
	public int getUpdateCount() throws SQLException {
		checkOpened();
		return (isResultsAccessible() && !getLastResult().tableFound ?
				getLastResult().updateCount : -1);
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		checkOpened();
		lastResultIndex++;
		return (isResultsAccessible() && getLastResult().tableFound);
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		if (direction != ResultSet.FETCH_FORWARD) {
			throw SQLErrorUtils.error(SQLErrorUtils.ILLEGAL_PARAMETER,
					"Unsupported fetch direction (" +
					"direction=" + direction + ")", null);
		}
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		checkOpened();
		if (maxRows != 0 && rows > maxRows) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Fetch size exceeds maxRows (value=" + rows +
					", maxRows=" + maxRows + ")", null);
		}
		else if (rows < 0) {
			throw SQLErrorUtils.error(SQLErrorUtils.ILLEGAL_PARAMETER,
					"Negative parameter (value=" + rows + ")", null);
		}

		fetchSize = rows;
	}

	@Override
	public int getFetchSize() throws SQLException {
		checkOpened();
		return fetchSize;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		checkOpened();
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public int getResultSetType() throws SQLException {
		checkOpened();
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		if (!batchOperationSupported) {
			throw SQLErrorUtils.errorNotSupported();
		}

		checkOpened();
		queryList.add(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		if (!batchOperationSupported) {
			throw SQLErrorUtils.errorNotSupported();
		}

		checkOpened();
		queryList.clear();
		clearResults();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		if (!batchOperationSupported) {
			throw SQLErrorUtils.errorNotSupported();
		}

		checkOpened();

		try {
			execute(StatementOperation.EXECUTE, true);
		}
		finally {
			queryList.clear();
		}

		final int[] batchResult = new int[resultList.size()];
		for (int i = 0; i < batchResult.length; i++) {
			final Result result = resultList.get(i);
			batchResult[i] = (result.tableFound ?
					SUCCESS_NO_INFO : result.updateCount);
		}

		return batchResult;
	}

	@Override
	public Connection getConnection() throws SQLException {
		checkOpened();
		return connection;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed || connection.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return false;
	}

	protected enum StatementOperation {
		EXECUTE,
		PREPARE,
		QUERY,
		UPDATE,
		PRAGMA,
		FETCH,
		CLOSE
	}

	enum SessionMode {
		AUTO,
		CREATE,
		GET
	}

	protected static class Result {
		BasicBuffer buf;
		long queryId;

		int updateCount;
		int parameterCount;

		boolean tableFound;
		boolean followingExists;

		int schemaSize;
		int schemaStartPos;

		int rowCount;
		int rowSetSize;
		int rowSetStartPos;

		boolean accept(BasicBuffer buf, long queryId,
				StatementOperation operation) {
			this.buf = buf;
			this.queryId = queryId;

			updateCount = buf.base().getInt();
			parameterCount = buf.base().getInt();

			tableFound = buf.getBoolean();

			if (tableFound) {
				followingExists = buf.getBoolean();
				rowCount = buf.base().getInt();

				schemaSize = buf.base().getInt();
				schemaStartPos = buf.base().position();
				buf.base().position(schemaStartPos + schemaSize);

				rowSetSize = buf.base().getInt();
				rowSetStartPos = buf.base().position();
				buf.base().position(rowSetStartPos + rowSetSize);

				switch (operation) {
				case EXECUTE:
				case FETCH:
				case PRAGMA:
				case PREPARE:
				case QUERY:
					return true;
				}
			}
			else {
				schemaSize = 0;
				schemaStartPos = 0;

				rowCount = 0;
				rowSetSize = 0;
				rowSetStartPos = 0;

				switch (operation) {
				case CLOSE:
				case EXECUTE:
				case PRAGMA:
				case PREPARE:
				case UPDATE:
					return true;
				}
			}

			return false;
		}

		BasicBuffer getSchemaBuffer() {
			if (!tableFound) {
				return null;
			}

			buf.base().limit(schemaStartPos + schemaSize);
			buf.base().position(schemaStartPos);
			return buf;
		}

		BasicBuffer getRowSetBuffer() {
			if (!tableFound) {
				return null;
			}

			buf.base().limit(rowSetStartPos + rowSetSize);
			buf.base().position(rowSetStartPos);
			return buf;
		}
	}

	protected void checkOpened() throws SQLException {
		if (isClosed()) {
			throw SQLErrorUtils.errorAlreadyClosed();
		}
	}

	boolean executePragma(String pragma) throws SQLException {
		checkOpened();

		queryList.clear();
		queryList.add(pragma);
		execute(StatementOperation.PRAGMA, true);

		return getLastResult().tableFound;
	}

	protected void execute(StatementOperation statementOp, boolean newQuery)
			throws SQLException {

		final long queryTimeoutMillis = Math.max(queryTimeoutSecs * 1000L, -1);
		long remainingTimeout = queryTimeoutMillis;
		long initialTime = -1;
		long failoverStartTime = -1;

		clearResults();

		for (int retryCount = 0;; retryCount++) {
			BaseConnection curBaseConnection = connection.base();
			final Hook hook = connection.getHook();
			try {
				if (retryCount > 0 || curBaseConnection == null) {
					if (initialTime < 0) {
						initialTime = System.currentTimeMillis();
					}
					connection.reset();
					curBaseConnection = connection.base();
				}

				if (curBaseConnection != queryRef.lastBaseConnection) {
					queryRef.queryId = 0;
					queryRef.lastBaseConnection = curBaseConnection;
				}

				final boolean prepared;
				final long oldQueryId;
				if (newQuery) {
					oldQueryId = queryRef.queryId;
					queryRef.queryId = connection.generateQueryId();
					prepared = false;
				}
				else {
					if (!refreshQuery(
							statementOp, newQuery, queryRef.queryId)) {
						final long currentTime = System.currentTimeMillis();
						final long elapsedMillis;
						if (initialTime > 0) {
							elapsedMillis = currentTime - initialTime;
						}
						else {
							elapsedMillis = 0;
						}

						final BaseConnection queryConnection =
								queryRef.lastBaseConnection;
						final GSException lastException =
								(queryConnection == null ?
										null : queryConnection.lastException);
						final int errorCode = (lastException == null ?
								SQLErrorUtils.ILLEGAL_STATE :
								lastException.getErrorCode());
						final long networkTimeout =
								connection.getNetworkTimeoutMillis();
						final String message = formatNetworkException(
								"Previous connection problem " +
								"cannot be recovered on this operation type",
								retryCount, elapsedMillis, 0,
								queryTimeoutMillis, networkTimeout,
								statementOp, lastException);
						throw SQLErrorUtils.error(
								errorCode, message, lastException);
					}

					oldQueryId = 0;
					prepared = (queryRef.queryId != 0);
				}

				if (statementOp == null) {
					break;
				}

				SQLConnection.fillRequestHead(curBaseConnection.base, req);

				req.putUUID(connection.getUUID());
				req.putByteEnum(
						prepared ? SessionMode.GET : SessionMode.CREATE);
				req.putEnum(statementOp);
				req.putLong(queryRef.queryId);
				req.putLong(oldQueryId);
				req.putBoolean(connection.isTransactionStarted());
				req.putBoolean(retryCount > 0);

				putInputTable(req);

				final OptionalRequest optionalRequest = new OptionalRequest();
				optionalRequest.put(OptionalRequestType.STATEMENT_TIMEOUT,
						PropertyUtils.timeoutPropertyToIntMillis(
								remainingTimeout));
				optionalRequest.put(OptionalRequestType.FETCH_LIMIT,
						(long) maxRows);
				optionalRequest.put(OptionalRequestType.FETCH_SIZE,
						(long) fetchSize);
				optionalRequest.put(OptionalRequestType.DB_NAME,
						connection.getDbName());
				connection.putOptionalRequest(optionalRequest);

				optionalRequest.format(req);

				if (prepared) {
					req.putInt(0);
				}
				else {
					req.putInt(queryList.size());
					for (String query : queryList) {
						req.putString(query);
					}
				}

				final int partitionId =
						Math.max(connection.getPreferablePartitionId(), 0);

				curBaseConnection.base.setMinHeartbeatTimeoutMillis(
						remainingTimeout);
				hook.startQuery(
						partitionId, connection.getUUID(), queryRef.queryId,
						remainingTimeout);

				curBaseConnection.base.executeStatementDirect(
						SQLConnection.SQL_STATEMENT_TYPE,
						partitionId, 0, req, resp, null);
			}
			catch (GSConnectionException e) {
				if (curBaseConnection != null) {
					if (failoverStartTime >= 0 &&
							curBaseConnection.lastHeartbeatCount !=
							curBaseConnection.base.
							getHeartbeatReceiveCount()) {
						failoverStartTime = -1;
					}
					curBaseConnection.lastException = e;
				}
				try {
					connection.disconnect(true);
				}
				catch (GSException e2) {
				}

				final long currentTime = System.currentTimeMillis();

				Long elapsedMillis = hook.getElapsedMillis();
				if (elapsedMillis == null) {
					if (initialTime > 0) {
						elapsedMillis = currentTime - initialTime;
					}
					else {
						elapsedMillis = connection.getNetworkTimeoutMillis();
					}
				}

				final long failureMillis;
				if (failoverStartTime >= 0) {
					failureMillis = currentTime - failoverStartTime;
				}
				else {
					failureMillis = 0;
					failoverStartTime = currentTime;
				}

				final long networkTimeout =
						connection.getNetworkTimeoutMillis();

				if (queryTimeoutMillis > 0 && elapsedMillis > queryTimeoutMillis ||
						networkTimeout > 0 && failureMillis > networkTimeout) {
					throw SQLErrorUtils.error(
							0, formatNetworkException(
									"Connection problem occurred",
									retryCount, elapsedMillis, failureMillis,
									queryTimeoutMillis, networkTimeout,
									statementOp, e), e);
				}
				else if (!refreshQuery(statementOp, newQuery, 0)) {
					throw SQLErrorUtils.error(
							0, formatNetworkException(
									"Connection problem cannot be recovered " +
									"on this operation type",
									retryCount, elapsedMillis, failureMillis,
									queryTimeoutMillis, networkTimeout,
									statementOp, e), e);
				}

				final long failoverIntervalMillis =
						connection.getFailoverIntervalMillis();

				if (queryTimeoutMillis > 0) {
					remainingTimeout = (queryTimeoutMillis -
							elapsedMillis - failoverIntervalMillis);
					remainingTimeout = Math.max(
							remainingTimeout, TIMEOUT_RESOLUTION_MILLIS);
				}

				if (retryCount > 0) {
					try {
						Thread.sleep(failoverIntervalMillis);
					}
					catch (InterruptedException e2) {
					}
				}
				continue;
			}
			catch (GSException e) {
				connection.updateTransactionStatus(
						false, connection.getAutoCommit());
				throw SQLErrorUtils.error(0, null, e);
			}
			finally {
				if (hook != null) {
					hook.endQuery();
				}
			}

			connection.updateBufferStatus(
					req.base().capacity(), resp.base().capacity());

			final boolean transactionStarted = resp.getBoolean();
			final boolean autoCommit = resp.getBoolean();
			connection.updateTransactionStatus(transactionStarted, autoCommit);

			clearResults();
			final int count = resp.base().getInt();
			boolean acceptable = true;
			for (int i = 0; i < count; i++) {
				final Result result = new Result();
				acceptable &=
						result.accept(resp, queryRef.queryId, statementOp);
				resultList.add(result);
			}

			if (resp.base().hasRemaining()) {
				final Map<String, String> env =
						SQLConnection.importRemoteEnv(resp);
				if (env != null) {
					connection.acceptRemoteEnv(env);
				}
			}

			if (!acceptable) {
				clearResults();
				connection.updateTransactionStatus(
						false, autoCommit);
				throw SQLErrorUtils.error(
						SQLErrorUtils.STATEMENT_CATEGORY_UNMATCHED,
						statementOp == StatementOperation.QUERY ?
								"Writable query specified for read only request" :
								"Read only query specified for writable request",
						null);
			}

			queryRef.lastBaseConnection = curBaseConnection;
			break;
		}
	}

	private ResultSet getResultSetDirect(
			boolean followingAccepting) throws SQLException {
		return (isResultsAccessible() && getLastResult().tableFound ?
				prepareResultSet(followingAccepting) : null);
	}

	SQLResultSet fetchFollowing(long queryId) throws SQLException {
		if (!resultList.isEmpty()) {
			resp = connection.takeRespBuffer();
		}

		if (queryId != queryRef.queryId) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ALREADY_CLOSED,
					"Unable to fetch by already closed result set", null);
		}

		final List<String> queryList = new ArrayList<String>(this.queryList);
		final List<Result> resultList = new ArrayList<Result>(this.resultList);
		final int lastResultIndex = this.lastResultIndex;
		final ResultSet lastResultSet = this.lastResultSet;

		this.resultList.clear();
		this.lastResultIndex = 0;
		this.lastResultSet = null;

		try {
			execute(StatementOperation.FETCH, false);
			final SQLResultSet rs = (SQLResultSet) getResultSetDirect(true);
			if (rs == null) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.MESSAGE_CORRUPTED,
						"Protocol error occurred on fetching results", null);
			}
			return rs;
		}
		finally {
			this.queryList.clear();
			this.queryList.addAll(queryList);
			this.resultList.clear();
			this.resultList.addAll(resultList);
			this.lastResultIndex = lastResultIndex;
			this.lastResultSet = lastResultSet;
		}
	}

	void closeQuery(long queryId) throws SQLException {
		try {
			queryList.clear();
			if (queryRef.queryId != 0) {
				execute(StatementOperation.CLOSE, false);
			}

			if (queryId != 0) {
				queryRef.queryId = queryId;
				execute(StatementOperation.CLOSE, false);
			}
		}
		finally {
			queryRef.queryId = 0;
		}
	}

	private boolean isResultsAccessible() {
		return (lastResultIndex < resultList.size());
	}

	protected Result getLastResult() throws SQLException {
		if (!isResultsAccessible()) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_STATE, "Result set not found", null);
		}

		return resultList.get(lastResultIndex);
	}

	private ResultSet prepareResultSet(
			boolean followingAccepting) throws SQLException {
		if (lastResultSet != null) {
			return lastResultSet;
		}

		final Result result = getLastResult();
		if (!result.tableFound) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_STATE, "Result set not found", null);
		}

		try {
			final RowMapper mapper = RowMapper.getInstance(
					result.getSchemaBuffer(), null, getRowMapperConfig());
			final String[] labelList =
					new String[mapper.getContainerInfo().getColumnCount()];
			for (int i = 0; i < labelList.length; i++) {
				labelList[i] = resp.getString();
			}

			final BasicBuffer rowBuf = result.getRowSetBuffer();
			long varDataBaseOffset = 0;
			varDataBaseOffset = rowBuf.base().getLong();
			lastResultSet = new SQLResultSet(
					this, mapper, labelList,
					result.queryId, result.followingExists, followingAccepting,
					result.rowCount, varDataBaseOffset, rowBuf);
		}
		catch (GSException e) {
			throw SQLErrorUtils.error(0, null, e);
		}

		return lastResultSet;
	}

	void closeWithoutResultSet() throws SQLException {
		lastResultSet = null;
		close();
	}

	protected void clearResults() throws SQLException {
		resultList.clear();
		lastResultIndex = 0;
		if (lastResultSet != null) {
			lastResultSet.close();
			lastResultSet = null;
		}
	}

	protected void putInputTable(BasicBuffer out) throws SQLException {
		out.putBoolean(false);
	}

	protected void setQueryDirect(String sql) throws SQLException {
		queryList.clear();
		queryList.add(sql);
	}

	private boolean refreshQuery(
			StatementOperation statementOp, boolean newQuery,
			long queryId) throws SQLException {
		if (!newQuery && statementOp != null && queryId == 0) {
			if (statementOp == StatementOperation.CLOSE) {
				return false;
			}
		}
		else {
			return true;
		}

		boolean succeeded = false;
		try {
			final boolean refreshed = refreshQuery(
					new ArrayList<String>(queryList), statementOp);
			succeeded = true;
			return refreshed;
		}
		finally {
			if (!succeeded) {
				try {
					closeQuery(0);
				}
				catch (Throwable t) {
				}
			}
		}
	}

	protected boolean refreshQuery(
			List<String> sqlList,
			StatementOperation statementOp) throws SQLException {
		return false;
	}

	private static String formatNetworkException(
			String message, int retryCount,
			long elapsedMillis, long failureMillis,
			long queryTimeoutMillis, long networkTimeoutMillis,
			StatementOperation statementOp, Throwable cause) {
		final StringBuilder sb = new StringBuilder();
		sb.append(message);

		sb.append(" (retryCount=");
		sb.append(retryCount);

		sb.append(", elapsedMillis=");
		sb.append(elapsedMillis);

		if (failureMillis > 0) {
			sb.append(", failureMillis=");
			sb.append(failureMillis);
		}

		sb.append(", queryTimeoutMillis=");
		sb.append(formatTimeoutMillis(queryTimeoutMillis));

		sb.append(statementOp == null ?
				", loginTimeoutMillis=" :
				", networkTimeoutMillis=");
		sb.append(formatTimeoutMillis(networkTimeoutMillis));

		sb.append(", operation=");
		if (statementOp == null) {
			sb.append("LOGIN");
		}
		else {
			sb.append(statementOp);
		}

		if (cause != null) {
			sb.append(", reason=");
			sb.append(cause.getMessage());
		}
		sb.append(")");

		return sb.toString();
	}

	private static String formatTimeoutMillis(long millis) {
		return (millis <= 0 ? "(not bounded)" : "" + millis);
	}

	static RowMapper.Config getRowMapperConfig() {
		return DEFAULT_MAPPER_CONFIG;
	}

	SimpleTimeZone getTimeZoneOffset() {
		return connection.getTimeZoneOffset();
	}

	static class QueryReference extends WeakReference<SQLStatement> {

		private BaseConnection lastBaseConnection;

		private long queryId;

		private QueryReference(SQLStatement referent) {
			super(referent);
		}

	}

	static class QueryPool {

		private final int MAX_SCAN_SIZE = 2;

		private final Set<QueryReference> activeRefSet =
				new LinkedHashSet<QueryReference>();

		private final List<Long> closeableQueryList = new ArrayList<Long>();

		void add(QueryReference ref) {
			activeRefSet.add(ref);
		}

		void detach(QueryReference ref, SQLConnection connection) {
			if (activeRefSet.remove(ref) &&
					ref.lastBaseConnection == connection.base() && ref.queryId != 0) {
				closeableQueryList.add(ref.queryId);
			}
			ref.clear();
		}

		void detachAll(SQLConnection connection) {
			for (QueryReference ref :
					new ArrayList<QueryReference>(activeRefSet)) {
				detach(ref, connection);
			}
		}

		boolean isEmpty() {
			return activeRefSet.isEmpty() && closeableQueryList.isEmpty();
		}

		long pull(SQLConnection connection) {
			if (!activeRefSet.isEmpty()) {
				for (int i = 0;;) {
					final Iterator<QueryReference> it = activeRefSet.iterator();
					final QueryReference ref = it.next();

					if (ref.get() == null) {
						detach(ref, connection);
						break;
					}
					else {
						it.remove();
						add(ref);
					}

					if (!it.hasNext() || ++i >= MAX_SCAN_SIZE) {
						break;
					}
				}
			}

			final int size = closeableQueryList.size();
			if (size == 0) {
				return 0;
			}

			final long queryId = closeableQueryList.get(size - 1);
			closeableQueryList.remove(size - 1);

			return queryId;
		}

	}

	@Override
	public void closeOnCompletion() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

}
