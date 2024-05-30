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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.BufferUnderflowException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import com.toshiba.mwcloud.gs.sql.internal.BasicBuffer.BufferUtils;
import com.toshiba.mwcloud.gs.sql.internal.LoggingUtils.BaseGridStoreLogger;
import com.toshiba.mwcloud.gs.sql.internal.PropertyUtils.WrappedProperties;
import com.toshiba.mwcloud.gs.sql.internal.Statement.GeneralStatement;

class NodeConnection implements Closeable {

	public static final int EE_MAGIC_NUMBER = 65021048;

	private static final int DEFAULT_PROTOCOL_VERSION = 15;

	private static final int STATEMENT_TYPE_NUMBER_V2_OFFSET = 100;

	private static final int SPECIAL_PARTITION_ID = 0;

	private static final int EXCEPTION_SUB_CODE_BITS = 24;

	private static final int PUBLIC_DATABASE_ID = 0;

	private static final StatementResult[] STATEMENT_RESULT_CONSTANTS =
			StatementResult.values();

	private static volatile boolean detailErrorMessageEnabled = false;

	private static volatile int protocolVersion = DEFAULT_PROTOCOL_VERSION;

	private static volatile int firstStatementTypeNumber =
			statementToNumber(Statement.CONNECT.generalize());

	private static boolean tcpNoDelayEnabled = true;

	private static final BaseGridStoreLogger HEARTBEAT_LOGGER =
			LoggingUtils.getLogger("Heartbeat");

	private static final BaseGridStoreLogger IO_LOGGER =
			LoggingUtils.getLogger("StatementIO");

	private static final Statement STATEMENT_LIST[] = Statement.values();

	private static final String ERROR_PARAM_ADDRESS = "address";

	private static final String ERROR_PARAM_PARTITION_ID = "partitionId";

	private long statementTimeoutMillis;

	private long maxHeartbeatTimeoutMillis;

	private long heartbeatTimeoutMillis;

	private Socket socket;

	private InputStream input;

	private OutputStream output;

	private Integer alternativeVersion;

	private final boolean ipv6Enabled;

	private Set<SocketType> acceptableSocketTypes;

	private Map<SocketType, SocketFactory> socketFactories;

	private AuthMode authMode = Challenge.getDefaultMode();

	private int remoteProtocolVersion;

	private FeatureVersion remoteFeatureVersion;

	private AuthType authType = AuthType.INTERNAL;

	private ConnectionRoute connectionRoute = ConnectionRoute.DEFAULT;

	private boolean responseUnacceptable;

	private long statementId = 0;

	private long heartbeatReceiveCount;

	private byte[] pendingResp;

	private Hook hook;

	public NodeConnection(
			InetSocketAddress address, Config config) throws GSException {
		try {
			socket = createSocket(
					config.socketFactories, SocketType.PLAIN, null);
			if (tcpNoDelayEnabled) {
				socket.setTcpNoDelay(true);
			}
			socket.connect(address,
					PropertyUtils.timeoutPropertyToIntMillis(Math.max(
							config.connectTimeoutMillis, 0)));
			setConfig(config);
			input = socket.getInputStream();
			output = socket.getOutputStream();
		}
		catch (IOException e) {
			throw new GSConnectionException(
					GSErrorCode.BAD_CONNECTION,
					"Failed to connect (address=" + address +
					", reason=" + e.getMessage() + ")", e);
		}

		ipv6Enabled = (address.getAddress() instanceof Inet6Address);
		acceptableSocketTypes = config.acceptableSocketTypes;
		socketFactories = config.socketFactories;
	}

	public void setHook(Hook hook) throws GSException {
		this.hook = hook;
	}

	public void setConfig(Config config) throws GSException {
		statementTimeoutMillis = (config.statementTimeoutEnabled ?
				config.statementTimeoutMillis : Long.MAX_VALUE);
		maxHeartbeatTimeoutMillis = config.heartbeatTimeoutMillis;
		heartbeatTimeoutMillis = config.heartbeatTimeoutMillis;
		try {
			socket.setSoTimeout(
					PropertyUtils.timeoutPropertyToIntMillis(Math.min(
							statementTimeoutMillis, heartbeatTimeoutMillis)));
		}
		catch (IOException e) {
			throw new GSConnectionException(
					GSErrorCode.BAD_CONNECTION,
					"Failed to change connection setting (address=" +
							getRemoteSocketAddress() +
					", reason=" + e.getMessage() + ")", e);
		}
		alternativeVersion = config.alternativeVersion;
	}

	public void setMinHeartbeatTimeoutMillis(long millis) throws GSException {
		try {
			final long correctedMillis = (millis > 0 ?
					Math.min(millis, maxHeartbeatTimeoutMillis) :
					maxHeartbeatTimeoutMillis);
			if (correctedMillis == heartbeatTimeoutMillis) {
				return;
			}

			heartbeatTimeoutMillis = correctedMillis;
			socket.setSoTimeout(
					PropertyUtils.timeoutPropertyToIntMillis(Math.min(
							statementTimeoutMillis, heartbeatTimeoutMillis)));
		}
		catch (IOException e) {
			throw new GSConnectionException(
					GSErrorCode.BAD_CONNECTION,
					"Failed to change connection setting (address=" +
					getRemoteSocketAddress() +
					", reason=" + e.getMessage() + ")", e);
		}
	}

	public static void setDetailErrorMessageEnabled(
			boolean detailErrorMessageEnabled) {
		NodeConnection.detailErrorMessageEnabled = detailErrorMessageEnabled;
	}

	public static int getProtocolVersion() {
		return protocolVersion;
	}

	public int getRemoteProtocolVersion() {
		return remoteProtocolVersion;
	}

	public static boolean isSupportedProtocolVersion(int protocolVersion) {
		switch (protocolVersion) {
		case 1:
		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
		case 8:
		case 13:
		case 14:
		case 15:
			return true;
		default:
			return false;
		}
	}

	public static void setProtocolVersion(int protocolVersion) throws GSException {
		if (!isSupportedProtocolVersion(protocolVersion)) {
			throw new GSException(
					GSErrorCode.ILLEGAL_PARAMETER,
					"Wrong protocol version (version=" + protocolVersion + ")");
		}
		NodeConnection.protocolVersion = protocolVersion;
		firstStatementTypeNumber =
				statementToNumber(Statement.CONNECT.generalize());
	}

	public SocketAddress getRemoteSocketAddress() {
		return socket.getRemoteSocketAddress();
	}

	private static int getEEHeadLength(boolean ipv6Enabled) {
		
		
		
		
		
		if (ipv6Enabled) {
			return 4 * 4 + 16;
		}
		else {
			return 4 * 5;
		}
	}

	public static int getRequestHeadLength(boolean ipv6Enabled) {
		return getRequestHeadLength(ipv6Enabled, false);
	}

	public static int getRequestHeadLength(
			boolean ipv6Enabled, boolean firstStatement) {
		
		
		
		
		final int statementIdSize =
				(isStatementIdLarge(firstStatement) ? 8 : 4);
		return getEEHeadLength(ipv6Enabled) + 4 * 2 + statementIdSize;
	}

	public static void fillRequestHead(boolean ipv6Enabled, BasicBuffer req) {
		fillRequestHead(ipv6Enabled, req, false);
	}

	public static void fillRequestHead(
			boolean ipv6Enabled, BasicBuffer req, boolean firstStatement) {
		req.clear();
		req.prepare(getRequestHeadLength(ipv6Enabled, firstStatement));

		
		req.base().putInt(EE_MAGIC_NUMBER);

		
		if (ipv6Enabled) {
			req.base().putLong(0);
			req.base().putLong(0);
		}
		else {
			req.base().putInt(0);
		}

		
		req.base().putInt(0);

		
		req.base().putInt(-1);

		
		req.base().putInt(0);

		
		req.base().putInt(0);

		
		req.base().putInt(0);

		
		putStatementId(req, 0, firstStatement);
	}

	public static boolean isStatementIdLarge(boolean firstStatement) {
		return !firstStatement && protocolVersion >= 3;
	}

	public static void putStatementId(BasicBuffer req, long statementId) {
		putStatementId(req, statementId, false);
	}

	public static void putStatementId(
			BasicBuffer req, long statementId, boolean firstStatement) {
		if (isStatementIdLarge(firstStatement)) {
			req.putLong(statementId);
		}
		else {
			req.putInt((int) statementId);
		}
	}

	public static long getStatementId(
			BasicBuffer resp, boolean firstStatement) {
		if (isStatementIdLarge(firstStatement)) {
			return resp.base().getLong();
		}
		else {
			return resp.base().getInt();
		}
	}

	public static boolean isOptionalRequestEnabled() {
		return protocolVersion >= 3;
	}

	public static boolean isClientIdOnLoginEnabled() {
		return protocolVersion >= 13;
	}

	public static boolean isDatabaseIdEnabled() {
		return protocolVersion >= 14;
	}

	public static void tryPutEmptyOptionalRequest(BasicBuffer req) {
		if (isOptionalRequestEnabled()) {
			req.putInt(0);
		}
	}

	public void executeStatement(
			Statement statement, int partitionId, long statementId,
			BasicBuffer req, BasicBuffer resp)
			throws GSException {
		executeStatement(
				statement.generalize(), partitionId, statementId, req, resp);
	}

	public void executeStatement(
			GeneralStatement statement, int partitionId, long statementId,
			BasicBuffer req, BasicBuffer resp)
			throws GSException {
		executeStatementDirect(
				statementToNumber(statement),
				partitionId, statementId, req, resp, null);
	}

	public void executeStatementDirect(
			int statementTypeNumber, int partitionId, long statementId,
			BasicBuffer req, BasicBuffer resp, Heartbeat heartbeat)
			throws GSException {
		if (partitionId < 0) {
			throw new GSException(
					GSErrorCode.INTERNAL_ERROR,
					"Internal error by illegal partition ID (" +
					"partitionId=" + partitionId +
					", address=" + getRemoteSocketAddress() + ")");
		}

		final boolean firstStatement =
				(statementTypeNumber == firstStatementTypeNumber);
		final int reqHeadLength =
				getRequestHeadLength(ipv6Enabled, firstStatement);
		final int eeHeadLength = getEEHeadLength(ipv6Enabled);
		final int reqLength = req.base().position();
		final long reqStatementId;
		if (statementId == 0) {
			while (++this.statementId == 0) {
			}
			reqStatementId = this.statementId;
		}
		else {
			reqStatementId = statementId;
		}

		if (IO_LOGGER.isDebugEnabled()) {
			final int n = statementTypeNumber - getStatementNumberOffset();
			final String statementName = (
					0 <= n && n < STATEMENT_LIST.length ?
							STATEMENT_LIST[n].toString() : "(" + n + ")");
			IO_LOGGER.debug(
					"statementIO.started",
					statementName,
					getRemoteSocketAddress(),
					partitionId,
					reqStatementId);
		}

		req.base().position(eeHeadLength - Integer.SIZE / Byte.SIZE);
		req.base().putInt(reqLength - eeHeadLength);
		req.base().putInt(statementTypeNumber);
		req.base().putInt(partitionId);
		putStatementId(req, reqStatementId, firstStatement);

		try {
			output.write(req.base().array(), 0, reqLength);
		}
		catch (IOException e) {
			throw new GSConnectionException(
					GSErrorCode.BAD_CONNECTION,
					"Failed to send message (address=" +
					getRemoteSocketAddress() +
					", reason=" + e.getMessage() + ")", e);
		}
		req.base().clear();
		req.base().position(reqHeadLength);

		if (resp == null) {
			return;
		}

		
		resp.clear();
		resp.prepare(eeHeadLength);

		resp.base().limit(eeHeadLength);
		int readLength;
		try {
			if (hook != null && heartbeat == null) {
				hook.startHeadReceiving(this, socket, reqStatementId);
			}
			try {
				readLength = readFully(resp.base().array(),
						0, eeHeadLength, resp.base().capacity());
			}
			finally {
				if (hook != null && heartbeat == null) {
					hook.endHeadReceiving(this, socket);
				}
			}
		}
		catch (GSException e) {
			if (responseUnacceptable || heartbeat != null || firstStatement) {
				throw e;
			}

			heartbeat = new Heartbeat();
			heartbeat.orgStatementTypeNumber = statementTypeNumber;
			heartbeat.orgStatementId = reqStatementId;
			heartbeat.orgStatementFound = false;

			resp = processHeartbeat(partitionId, resp, heartbeat);
			readLength = eeHeadLength;
		}

		if (resp.base().getInt() != EE_MAGIC_NUMBER) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal magic number (address=" +
					getRemoteSocketAddress() + ")");
		}
		resp.base().position(eeHeadLength - Integer.SIZE / Byte.SIZE);
		final int respBodyLength = resp.base().getInt();

		
		final int respTotalLength = eeHeadLength + respBodyLength;
		if (readLength > respTotalLength) {
			final int extraLength = readLength - respTotalLength;
			final int orgPendingLength;
			if (pendingResp == null) {
				orgPendingLength = 0;
				pendingResp = new byte[extraLength];
			}
			else {
				orgPendingLength = pendingResp.length;
				final byte[] orgPendingResp = pendingResp;
				pendingResp = new byte[orgPendingLength + extraLength];
				System.arraycopy(orgPendingResp, 0,
						pendingResp, 0, orgPendingResp.length);
			}

			System.arraycopy(
					resp.base().array(), respTotalLength, pendingResp,
					orgPendingLength, extraLength);
		}
		else if (readLength < respTotalLength) {
			final int length = respTotalLength - readLength;

			resp.base().limit(readLength);
			resp.base().position(readLength);
			resp.prepare(length);
			resp.base().position(eeHeadLength);

			readFully(resp.base().array(), readLength, length, length);
		}
		resp.base().limit(respTotalLength);

		final boolean statementIdMatched;

		final int respStatementTypeNumber = resp.base().getInt();
		if (respStatementTypeNumber != statementTypeNumber) {
			if (heartbeat == null || respStatementTypeNumber !=
					heartbeat.orgStatementTypeNumber) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal statement type (address=" +
						getRemoteSocketAddress() + ")");
			}
			heartbeat.orgStatementFound = true;

			final boolean orgFirstStatement =
					(heartbeat.orgStatementTypeNumber ==
							firstStatementTypeNumber);
			statementIdMatched =
					(getStatementId(resp, orgFirstStatement) ==
							heartbeat.orgStatementId);
		}
		else {
			statementIdMatched =
					(getStatementId(resp, firstStatement) == reqStatementId);
		}

		if (!statementIdMatched) {
			throw new GSConnectionException(
					GSErrorCode.MESSAGE_CORRUPTED,
					"Protocol error by illegal statement ID (address=" +
					getRemoteSocketAddress() + ")");
		}

		final StatementResult result =
				resp.getByteEnum(STATEMENT_RESULT_CONSTANTS);
		if (result != StatementResult.SUCCESS) {
			final GSException remoteException;
			try {
				remoteException =
						readRemoteException(result, resp, partitionId);
			}
			catch (BufferUnderflowException e) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by invalid remote error message (" +
						"result=" + result +
						", address=" + getRemoteSocketAddress() +
						", reason=" + e.getMessage() + ")", e);
			}
			remoteException.fillInStackTrace();

			if (heartbeat != null &&
					respStatementTypeNumber == firstStatementTypeNumber) {
				throw new GSConnectionException(
						GSErrorCode.BAD_CONNECTION,
						"Connection problem occurred by " +
						"invalid heartbeat response (" +
						"result=" + result +
						", address=" + getRemoteSocketAddress() +
						", reason=" + remoteException.getMessage() + ")",
						remoteException);
			}

			throw remoteException;
		}

		if (heartbeat != null && heartbeat.orgException != null) {
			throw heartbeat.orgException;
		}
	}

	private int readFully(byte[] value, int offset, int length, int maxLength)
			throws GSException {
		int pos = offset;

		if (pendingResp != null) {
			final int consumed = Math.min(pendingResp.length, maxLength);
			System.arraycopy(pendingResp, 0, value, pos, consumed);
			pos += consumed;

			if (pendingResp.length == consumed) {
				pendingResp = null;
			}
			else {
				pendingResp = Arrays.copyOfRange(
						pendingResp, consumed, pendingResp.length);
			}
		}

		final int endPos = offset + length;
		final int maxEndPos = offset + maxLength;
		try {
			for (;;) {
				final int last = input.read(value, pos, maxEndPos - pos);
				if (last < 0) {
					throw new GSConnectionException(
							GSErrorCode.BAD_CONNECTION,
							"Connection unexpectedly terminated (address=" +
							getRemoteSocketAddress() + ")");
				}

				if ((pos += last) >= endPos) {
					return pos - offset;
				}
			}
		}
		catch (GSConnectionException e) {
			throw e;
		}
		catch (IOException e) {
			final boolean timeoutOccurred =
					(e instanceof SocketTimeoutException);
			if (!timeoutOccurred || pos != offset) {
				responseUnacceptable = true;
			}

			if (timeoutOccurred) {
				throw new GSConnectionException(
						GSErrorCode.CONNECTION_TIMEOUT,
						"Connection timed out on receiving (" +
						"receivedSize=" + (pos - offset) +
						", totalSize=" + length +
						", address=" + getRemoteSocketAddress() +
						", reason=" + e.getMessage() + ")", e);
			}

			throw new GSConnectionException(
					GSErrorCode.BAD_CONNECTION,
					"Connection problem occurred on receiving (" +
					"receivedSize=" + (pos - offset) +
					", totalSize=" + length +
					", address=" + getRemoteSocketAddress() +
					", reason=" + e.getMessage() + ")", e);
		}
	}

	private BasicBuffer processHeartbeat(
			int partitionId, BasicBuffer resp, Heartbeat heartbeat)
			throws GSException {
		final BasicBuffer heartbeatBuf = createRequestBuffer();

		final int eeHeadLength = getEEHeadLength(ipv6Enabled);
		final long startTime = System.currentTimeMillis();
		final long initialMillis =
				Math.min(statementTimeoutMillis, heartbeatTimeoutMillis);
		long lastHeartbeatId;
		for (;;) {
			final long elapsedMillis =
					System.currentTimeMillis() - startTime + initialMillis;
			if (elapsedMillis >= statementTimeoutMillis) {
				throw new GSConnectionException(
					GSErrorCode.CONNECTION_TIMEOUT,
					"Connection timed out by statement timeout (" +
					"elapsedMillis=" + elapsedMillis +
					", statementTimeoutMillis=" + statementTimeoutMillis +
					", address=" + getRemoteSocketAddress() + ")");
			}

			if (hook != null) {
				hook.prepareHeartbeat(this, socket, elapsedMillis);
			}

			if (HEARTBEAT_LOGGER.isInfoEnabled()) {
				final int n = heartbeat.orgStatementTypeNumber -
						getStatementNumberOffset();
				final String statementName = (
						0 <= n && n < STATEMENT_LIST.length ?
								STATEMENT_LIST[n].toString() : "(" + n + ")");
				HEARTBEAT_LOGGER.info(
						"heartbeat.started",
						statementName,
						getRemoteSocketAddress(),
						partitionId,
						heartbeat.orgStatementId,
						elapsedMillis);
			}

			while (++this.statementId == 0) {
			}
			lastHeartbeatId = this.statementId;

			final BasicBuffer heartbeatReq = heartbeatBuf;
			putConnectRequest(heartbeatReq);

			try {
				executeStatementDirect(
						firstStatementTypeNumber, SPECIAL_PARTITION_ID,
						lastHeartbeatId, heartbeatReq, resp, heartbeat);
			}
			catch (GSStatementException e) {
				heartbeat.orgException = e;
			}

			heartbeatReceiveCount++;

			if (heartbeat.orgStatementFound) {
				resp = heartbeatBuf;
				resp.clear();
				resp.prepare(eeHeadLength);
				resp.base().limit(eeHeadLength);
				readFully(resp.base().array(), 0, eeHeadLength, eeHeadLength);

				heartbeat.orgStatementTypeNumber = firstStatementTypeNumber;
				heartbeat.orgStatementId = lastHeartbeatId;

				return resp;
			}

			resp.clear();
			resp.prepare(eeHeadLength);
			resp.base().limit(eeHeadLength);
			try {
				readFully(resp.base().array(), 0, eeHeadLength, eeHeadLength);
				return resp;
			}
			catch (GSException e) {
				if (responseUnacceptable) {
					throw new GSConnectionException(
							GSErrorCode.BAD_CONNECTION,
							"Connection problem occurred after heartbeat (" +
							"elapsedMillis=" + elapsedMillis +
							", address=" + getRemoteSocketAddress() +
							", reason=" + e.getMessage() + ")", e);
				}
				continue;
			}
		}
	}

	private GSException readRemoteException(
			StatementResult result, BasicBuffer resp, int partitionId)
			throws BufferUnderflowException {
		final int count = resp.base().getInt();

		int topCode = 0;
		String topMessage = null;

		int majorCode = 0;
		String majorMessage = null;
		final StringBuilder messageBuilder = new StringBuilder();

		for (int i = 0; i < count; i++) {
			final int code = resp.base().getInt();
			final String message = resp.getString();
			final String typeName = resp.getString();

			final String fileName = resp.getString();
			final String functionName = resp.getString();
			final int line = resp.base().getInt();

			if (i == 0) {
				topCode = code;
				topMessage = message;
			}

			boolean majorCodeUpdated = false;
			if (code != 0 && !typeName.endsWith("PlatformException")) {
				majorCodeUpdated = true;
				majorCode = code;
			}

			if (!detailErrorMessageEnabled) {
				if (!message.isEmpty()) {
					if (majorCodeUpdated) {
						majorMessage = message;
					}
					else if (majorCode == 0) {
						majorMessage = "minorCode=" + code + " : " + message;
					}
				}
				continue;
			}

			if (i > 0) {
				messageBuilder.append(" by ");
			}

			if (typeName.isEmpty()) {
				messageBuilder.append("(Unknown exception)");
			}
			else {
				messageBuilder.append(typeName);
			}

			if (!fileName.isEmpty()) {
				messageBuilder.append(" ").append(fileName);
			}

			if (!functionName.isEmpty()) {
				messageBuilder.append(" ").append(functionName);
			}

			if (line > 0) {
				messageBuilder.append(" line=").append(line);
			}

			if (code != 0) {
				messageBuilder.append(" code=").append(code);
			}

			if (!message.isEmpty()) {
				messageBuilder.append(" : ").append(message);
			}
		}

		String errorName = null;
		if (resp.base().remaining() > 0) {
			errorName = resp.getString();
			if (errorName.isEmpty()) {
				errorName = null;
			}
			else {
				majorCode = topCode;
				majorMessage = topMessage;
			}
		}

		final Map<String, String> paremeters = GSErrorCode.newParameters();
		if (resp.base().remaining() > 0) {
			final int paramCount = resp.base().getInt();
			for (int i = 0; i < paramCount; i++) {
				final String name = resp.getString();
				final String value = resp.getString();
				paremeters.put(name, value);
			}
		}

		if (!detailErrorMessageEnabled && majorMessage != null) {
			messageBuilder.append(majorMessage);
		}

		if (messageBuilder.length() > 0) {
			messageBuilder.append(" ");
		}
		messageBuilder.append("(");
		GSErrorCode.addParameter(
				messageBuilder, paremeters, ERROR_PARAM_ADDRESS,
				socketAddressToString(getRemoteSocketAddress()), true);
		GSErrorCode.addParameter(
				messageBuilder, paremeters, ERROR_PARAM_PARTITION_ID,
				Integer.toString(partitionId), false);
		messageBuilder.append(")");

		final int subCode = majorCode >>> EXCEPTION_SUB_CODE_BITS;
		switch (result) {
		case STATEMENT_ERROR:
			return new GSStatementException(
					getMainErrorCode(majorCode, GSErrorCode.BAD_STATEMENT),
					subCode, errorName, messageBuilder.toString(), paremeters,
					null);
		case DENY:
			return new GSWrongNodeException(
					getMainErrorCode(majorCode, GSErrorCode.WRONG_NODE),
					subCode, errorName, messageBuilder.toString(), paremeters,
					null);
		default:
			return new GSConnectionException(
					getMainErrorCode(majorCode, GSErrorCode.BAD_CONNECTION),
					subCode, errorName, messageBuilder.toString(), paremeters,
					null);
		}
	}

	private static int getMainErrorCode(int baseCode, int defaultCode) {
		final int code = baseCode & ((1 << EXCEPTION_SUB_CODE_BITS) - 1);
		if (code == 0) {
			return defaultCode;
		}

		return code;
	}

	private void putConnectRequest(BasicBuffer req) {
		req.base().position(getRequestHeadLength(ipv6Enabled, true));
		req.putInt(alternativeVersion == null ?
				protocolVersion : alternativeVersion);
		putControlInfo(req);
	}

	private void acceptConnectResponse(BasicBuffer resp) throws GSException {
		authMode = Challenge.getMode(resp);

		if (resp.base().remaining() > 0) {
			final int version = resp.base().getInt();
			if (version == 0 || (remoteProtocolVersion != 0 &&
					version != remoteProtocolVersion)) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by illegal remote version (version=" +
						version + ")");
			}
			remoteProtocolVersion = version;
		}

		if (resp.base().remaining() > 0) {
			authType = resp.getByteEnum(AuthType.class);
		}

		acceptControlInfo(resp);
	}

	private void putControlInfo(BasicBuffer req) {
		final int headPos = req.base().position();
		req.putInt(0);

		final int bodyPos = req.base().position();
		req.putBoolean(acceptableSocketTypes.contains(SocketType.PLAIN));
		req.putBoolean(acceptableSocketTypes.contains(SocketType.SECURE));

		final int endPos = req.base().position();
		req.base().position(headPos);
		final int bodySize = endPos - bodyPos;
		req.putInt(bodySize);
		req.base().position(endPos);
	}

	private void acceptControlInfo(BasicBuffer resp) throws GSException {
		final Set<SocketType> respTypes = EnumSet.noneOf(SocketType.class);
		do {
			if (resp.base().remaining() <= 0) {
				respTypes.add(SocketType.PLAIN);
				break;
			}

			final int bodySize = BufferUtils.getIntSize(resp.base());
			final int limit = BufferUtils.limitForward(resp.base(), bodySize);

			if (resp.getBoolean()) {
				respTypes.add(SocketType.PLAIN);
			}
			if (resp.getBoolean()) {
				respTypes.add(SocketType.SECURE);
			}

			BufferUtils.restoreLimit(resp.base(), limit);
		}
		while (false);

		final Set<SocketType> nextTypes =
				EnumSet.copyOf(acceptableSocketTypes);
		nextTypes.retainAll(respTypes);
		if (nextTypes.isEmpty()) {
			throw new GSException(
					GSErrorCode.ILLEGAL_CONFIG,
					"Requested security options not supported (" +
					"requested={plain:" + acceptableSocketTypes.contains(
							SocketType.PLAIN) +
					", secure:" + acceptableSocketTypes.contains(
							SocketType.SECURE) + "}, " +
					"supported={plain:" + respTypes.contains(SocketType.PLAIN) +
					", secure:" + respTypes.contains(SocketType.SECURE) +
					"})");
		}
		else if (!socketFactories.isEmpty()) {
			changeTransportMethod(nextTypes);
		}
	}

	private void changeTransportMethod(Set<SocketType> nextTypes)
			throws GSException {
		final Map<SocketType, SocketFactory> socketFactories =
				this.socketFactories;
		this.socketFactories = Collections.emptyMap();

		if (!nextTypes.contains(SocketType.SECURE)) {
			acceptableSocketTypes = EnumSet.of(SocketType.PLAIN);
			return;
		}
		acceptableSocketTypes = EnumSet.of(SocketType.SECURE);

		socket = createSocket(socketFactories, SocketType.SECURE, socket);
		try {
			input = socket.getInputStream();
			output = socket.getOutputStream();
		}
		catch (IOException e) {
			throw new GSConnectionException(e);
		}
	}

	public void connect(BasicBuffer req, BasicBuffer resp) throws GSException {
		req = (req == null ? createRequestBuffer() : req);
		resp = (resp == null ? createOutput() : resp);

		putConnectRequest(req);
		executeStatement(
				Statement.CONNECT.generalize(), SPECIAL_PARTITION_ID,
				0, req, resp);
		acceptConnectResponse(resp);
	}

	public void disconnect(BasicBuffer req, BasicBuffer resp) throws GSException {
		req = (req == null ? createRequestBuffer() : req);

		req.base().position(getRequestHeadLength(ipv6Enabled, false));
		tryPutEmptyOptionalRequest(req);
		executeStatement(
				Statement.DISCONNECT.generalize(), SPECIAL_PARTITION_ID,
				0, req, null);

		closeImmediately();
	}

	public void login(
			BasicBuffer req, BasicBuffer resp, LoginInfo loginInfo,
			long[] databaseId) throws GSException {
		req = (req == null ? createRequestBuffer() : req);
		resp = (resp == null ? createOutput() : resp);

		final Challenge challenge = loginInternal(req, resp, loginInfo, null);

		if (challenge != null) {
			loginInternal(req, resp, loginInfo, challenge);
		}

		final long respDatabaseId;
		if (resp.base().remaining() > 0) {
			respDatabaseId = resp.base().getLong();
		}
		else {
			if (isDatabaseIdEnabled()) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED,
						"Protocol error by lack of database ID");
			}
			respDatabaseId = PUBLIC_DATABASE_ID;
		}

		if (databaseId != null) {
			databaseId[0] = respDatabaseId;
		}

		if (resp.base().remaining() > 0) {
			remoteFeatureVersion =
					FeatureVersion.remoteValueOf(resp.base().getInt());
		}
	}

	private Challenge loginInternal(
			BasicBuffer req, BasicBuffer resp, LoginInfo loginInfo,
			Challenge lastChallenge) throws GSException {
		req.base().position(getRequestHeadLength(ipv6Enabled, false));

		final AuthType authType =
				resolveAuthType(loginInfo.authType, loginInfo.user);
		
		connectionRoute = loginInfo.connectionRoute;
		if (isOptionalRequestEnabled()) {
			final OptionalRequest request = new OptionalRequest();

			if (loginInfo.transactionTimeoutSecs >= 0) {
				request.put(OptionalRequestType.TRANSACTION_TIMEOUT,
						loginInfo.transactionTimeoutSecs);
			}

			if (loginInfo.database != null) {
				request.put(OptionalRequestType.DB_NAME, loginInfo.database);
			}
			if (loginInfo.clientId != null && isClientIdOnLoginEnabled()) {
				request.put(OptionalRequestType.CLIENT_ID, loginInfo.clientId);
			}

			if (loginInfo.applicationName != null) {
				request.put(
						OptionalRequestType.APPLICATION_NAME,
						loginInfo.applicationName);
			}
			if (loginInfo.storeMemoryAgingSwapRate >= 0) {
				request.put(
						OptionalRequestType.STORE_MEMORY_AGING_SWAP_RATE,
						loginInfo.storeMemoryAgingSwapRate);
			}
			if (loginInfo.timeZoneOffset != null) {
				request.put(
						OptionalRequestType.TIME_ZONE_OFFSET,
						(long) loginInfo.timeZoneOffset.getRawOffset());
			}
			if (authType != AuthType.INTERNAL) {
				request.put(
						OptionalRequestType.AUTHENTICATION_TYPE,
						(byte) authType.ordinal());
			}

			if (loginInfo.isPublicConnection()) {
				request.put(
						OptionalRequestType.CONNECTION_ROUTE,
						(byte) loginInfo.getConnectionRoute().ordinal());
			}
			request.put(
					OptionalRequestType.ACCEPTABLE_FEATURE_VERSION,
					FeatureVersion.latest().ordinal());

			request.format(req);
		}

		req.putString(loginInfo.user);
		req.putString(Challenge.build(
				authMode, lastChallenge, loginInfo.passwordDigest, authType));
		req.putInt(PropertyUtils.
				timeoutPropertyToIntSeconds(statementTimeoutMillis));
		req.putBoolean(loginInfo.ownerMode);

		req.putString(loginInfo.clusterName == null ? "" : loginInfo.clusterName);
		Challenge.putRequest(req, authMode, lastChallenge);

		executeStatement(
				Statement.LOGIN.generalize(), SPECIAL_PARTITION_ID,
				0, req, resp);

		final AuthMode[] resultMode = new AuthMode[] { authMode };
		final Challenge respChallenge =
				Challenge.getResponse(resp, resultMode, lastChallenge);
		authMode = resultMode[0];
		return respChallenge;
	}

	private AuthType resolveAuthType(AuthType specifiedType, String user)
			throws GSException {
		final boolean nonInternalSpecified =
				(specifiedType != null && specifiedType != AuthType.INTERNAL);
		final AuthType type = authType;

		if (type == AuthType.INTERNAL || specifiedType == AuthType.INTERNAL) {
			if (nonInternalSpecified) {
				throw new GSException(
						GSErrorCode.ILLEGAL_CONFIG,
						"Unavailable authentication type for target node (" +
						"authentication=" + specifiedType.toPropertyString() +
						", user=" + user +
						", address=" + getRemoteSocketAddress() + ")");
			}
			return AuthType.INTERNAL;
		}

		if (UserTypeUtils.checkAdmin(user)) {
			if (nonInternalSpecified) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
						"Illegal authentication type for admin user (" +
						"authentication=" + specifiedType.toPropertyString() +
						", user=" + user + ")");
			}
			return AuthType.INTERNAL;
		}

		return type;
	}

	public void reuse(
			BasicBuffer req, BasicBuffer resp, LoginInfo loginInfo,
			long[] databaseId) throws GSException {
		
		
		login(req, resp, loginInfo, databaseId);
	}

	public void logout(BasicBuffer req, BasicBuffer resp) throws GSException {
		req = (req == null ? createRequestBuffer() : req);
		resp = (resp == null ? createOutput() : resp);

		req.base().position(getRequestHeadLength(ipv6Enabled, false));
		tryPutEmptyOptionalRequest(req);
		executeStatement(
				Statement.LOGOUT.generalize(), SPECIAL_PARTITION_ID,
				0, req, resp);
	}

	public long getHeartbeatReceiveCount() {
		return heartbeatReceiveCount;
	}

	public FeatureVersion getRemoteFeatureVersion() {
		return remoteFeatureVersion;
	}

	public static String getDigest(String password) {
		try {
			final MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(password.getBytes(BasicBuffer.DEFAULT_CHARSET));

			StringBuilder builder = new StringBuilder();
			for (byte b : md.digest()) {
				builder.append(String.format("%02x", b));
			}

			return builder.toString();
		}
		catch (NoSuchAlgorithmException e) {
			throw new Error("Internal error while calculating digest", e);
		}
	}

	public static int statementToNumber(Statement statement) {
		return statementToNumber(statement.generalize());
	}

	public static int statementToNumber(GeneralStatement statement) {
		return statement.ordinal() + getStatementNumberOffset();
	}

	public static int getStatementNumberOffset() {
		if (protocolVersion < 2) {
			return 0;
		}

		return STATEMENT_TYPE_NUMBER_V2_OFFSET;
	}

	public static String socketAddressToString(
			SocketAddress socketAddress) {
		final InetSocketAddress inetSocketAddress;
		if (socketAddress instanceof InetSocketAddress) {
			inetSocketAddress = (InetSocketAddress) socketAddress;
		}
		else {
			return socketAddress.toString();
		}

		final InetAddress address = inetSocketAddress.getAddress();
		final int port = inetSocketAddress.getPort();

		final StringBuilder builder = new StringBuilder();
		if (address instanceof Inet6Address) {
			builder.append("[");
			builder.append(address.getHostAddress());
			builder.append("]");
		}
		else if (address != null) {
			builder.append(address.getHostAddress());
		}
		builder.append(":");
		builder.append(port);

		return builder.toString();
	}

	private BasicBuffer createRequestBuffer() {
		final BasicBuffer req = new BasicBuffer(64);
		fillRequestHead(ipv6Enabled, req, false);
		return req;
	}

	private BasicBuffer createOutput() {
		return new BasicBuffer(64);
	}

	public void closeImmediately() throws GSException {
		try {
			socket.close();
		}
		catch (IOException e) {
			throw new GSException(GSErrorCode.BAD_CONNECTION,
					"Connection problem occurred on closing (" +
					"reason=" + e.getMessage() + ")", e);
		}
	}

	public void close() throws GSException {
		try {
			if (!socket.isClosed()) {
				disconnect(null, null);
			}
		}
		finally {
			closeImmediately();
		}
	}

	private static Socket createSocket(
			Map<SocketType, SocketFactory> factoryMap, SocketType type,
			Socket base)
			throws GSException {
		final SocketFactory factory = factoryMap.get(type);
		if (factory == null) {
			throw new GSException(GSErrorCode.INTERNAL_ERROR, "");
		}

		try {
			if (base != null) {
				final InetSocketAddress address =
						(InetSocketAddress) base.getRemoteSocketAddress();
				return ((SSLSocketFactory) factory).createSocket(
						base, address.getHostName(), address.getPort(), true);
			}
			return factory.createSocket();
		}
		catch (IOException e) {
			throw new GSException(e);
		}
	}

	static class Heartbeat {
		int orgStatementTypeNumber;
		long orgStatementId;
		boolean orgStatementFound;
		GSStatementException orgException;
	}

	public static class ClientId {

		private final UUID uuid;

		private final long sessionId;

		public ClientId(UUID uuid, long sessionId) {
			this.uuid = uuid;
			this.sessionId = sessionId;
		}

		public static ClientId generate(long sessionId) {
			return new ClientId(UUID.randomUUID(), sessionId);
		}

		public long getSessionId() {
			return sessionId;
		}

		public UUID getUUID() {
			return uuid;
		}

	}

	public static class Config {

		private static final long CONNECT_TIMEOUT_DEFAULT = 10 * 1000;

		private static final long STATEMENT_TIMEOUT_DEFAULT = 15 * 1000;

		private static final long HEARTBEAT_TIMEOUT_DEFAULT = 10 * 1000;

		private long connectTimeoutMillis = CONNECT_TIMEOUT_DEFAULT;

		private long statementTimeoutMillis = STATEMENT_TIMEOUT_DEFAULT;

		private long heartbeatTimeoutMillis = HEARTBEAT_TIMEOUT_DEFAULT;

		private boolean statementTimeoutEnabled = false;

		private Integer alternativeVersion;

		private Set<SocketType> acceptableSocketTypes =
				Collections.emptySet();

		private Map<SocketType, SocketFactory> socketFactories =
				Collections.emptyMap();

		public void set(Config config, boolean withSocketConfig) {
			this.connectTimeoutMillis = config.connectTimeoutMillis;
			this.statementTimeoutMillis = config.statementTimeoutMillis;
			this.heartbeatTimeoutMillis = config.heartbeatTimeoutMillis;
			this.statementTimeoutEnabled = config.statementTimeoutEnabled;
			this.alternativeVersion = config.alternativeVersion;

			if (withSocketConfig) {
				this.acceptableSocketTypes = config.acceptableSocketTypes;
				this.socketFactories = config.socketFactories;
			}
		}

		public boolean set(WrappedProperties props) throws GSException {
			final long connectTimeoutMillis =
					props.getTimeoutProperty(
							"connectTimeout", this.connectTimeoutMillis, true);
			final long statementTimeoutMillis =
					props.getTimeoutProperty(
							"statementTimeout", this.statementTimeoutMillis, true);
			final long heartbeatTimeoutMillis =
					props.getTimeoutProperty(
							"heartbeatTimeout", this.heartbeatTimeoutMillis, true);
			final boolean statementTimeoutEnabled =
					props.getBooleanProperty(
							"statementTimeoutEnabled",
							this.statementTimeoutEnabled, true);
			if (connectTimeoutMillis < 0 || statementTimeoutMillis < 0) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
						"Negative timeout properties (" +
						"connectTimeoutMillis=" + connectTimeoutMillis +
						", statementTimeoutMillis=" + statementTimeoutMillis +
						", heartbeatTimeoutMillis=" + heartbeatTimeoutMillis +
						")");
			}

			if (connectTimeoutMillis != this.connectTimeoutMillis ||
					statementTimeoutMillis != this.statementTimeoutMillis ||
					heartbeatTimeoutMillis != this.heartbeatTimeoutMillis ||
					statementTimeoutEnabled != this.statementTimeoutEnabled) {
				this.connectTimeoutMillis = connectTimeoutMillis;
				this.statementTimeoutMillis = statementTimeoutMillis;
				this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
				this.statementTimeoutEnabled = statementTimeoutEnabled;
				return true;
			}

			return false;
		}

		public long getConnectTimeoutMillis() {
			return connectTimeoutMillis;
		}

		public long getStatementTimeoutMillis() {
			return statementTimeoutMillis;
		}

		public void setConnectTimeoutMillis(long connectTimeoutMillis) {
			this.connectTimeoutMillis = connectTimeoutMillis;
		}

		public void setStatementTimeoutMillis(long statementTimeoutMillis) {
			this.statementTimeoutMillis = statementTimeoutMillis;
		}

		public void setStatementTimeoutEnabled(
				boolean statementTimeoutEnabled) {
			this.statementTimeoutEnabled = statementTimeoutEnabled;
		}

		public void setHeartbeatTimeoutMillis(long heartbeatTimeoutMillis) {
			this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
		}

		public void setAlternativeVersion(Integer alternativeVersion) {
			this.alternativeVersion = alternativeVersion;
		}

		public void setSocketConfig(
				Set<SocketType> acceptableSocketTypes,
				Map<SocketType, SocketFactory> socketFactories) {
			this.acceptableSocketTypes = acceptableSocketTypes;
			this.socketFactories = socketFactories;
		}

		public Map<SocketType, SocketFactory> getSocketFactories() {
			return socketFactories;
		}

	}

	enum SocketType {
		PLAIN,
		SECURE
	}

	public static class LoginInfo {

		private static Map<String, AuthType> AUTH_TYPE_MAP = makeAuthTypeMap();

		private static Map<String, ConnectionRoute> CONNECTION_ROUTE_MAP = makeConnectionRouteMap();

		private String user;

		private PasswordDigest passwordDigest;

		private String database;

		private boolean ownerMode;

		private String clusterName;

		private int transactionTimeoutSecs;

		private ClientId clientId;

		private String applicationName;

		private double storeMemoryAgingSwapRate;

		private SimpleTimeZone timeZoneOffset;

		private AuthType authType;

		private ConnectionRoute connectionRoute;

		public LoginInfo(
				String user, String password, boolean ownerMode,
				String database, String clusterName,
				long transactionTimeoutMillis,
				String applicationName, double storeMemoryAgingSwapRate,
				SimpleTimeZone timeZoneOffset, AuthType authType, ConnectionRoute connectionRoute) {
			final ClientId clientId = null;
			set(
					user,
					Challenge.makeDigest(user, password),
					database,
					ownerMode,
					clusterName,
					PropertyUtils.timeoutPropertyToIntSeconds(
							transactionTimeoutMillis),
					clientId,
					applicationName,
					storeMemoryAgingSwapRate,
					timeZoneOffset,
					authType,
					connectionRoute);
		}

		public LoginInfo(LoginInfo loginInfo) {
			set(loginInfo);
		}

		public void set(LoginInfo loginInfo) {
			set(
					loginInfo.user,
					loginInfo.passwordDigest,
					loginInfo.database,
					loginInfo.ownerMode,
					loginInfo.clusterName,
					loginInfo.transactionTimeoutSecs,
					loginInfo.clientId,
					loginInfo.applicationName,
					loginInfo.storeMemoryAgingSwapRate,
					loginInfo.timeZoneOffset,
					loginInfo.authType,
					loginInfo.connectionRoute);
		}

		private void set(
				String user, PasswordDigest passwordDigest,
				String database, boolean ownerMode, String clusterName,
				int transactionTimeoutSecs, ClientId clientId,
				String applicationName, double storeMemoryAgingSwapRate,
				SimpleTimeZone timeZoneOffset, AuthType authType, ConnectionRoute connectionRoute) {
			this.user = user;
			this.passwordDigest = passwordDigest;
			this.database = database;
			this.ownerMode = ownerMode;
			this.clusterName = clusterName;
			this.transactionTimeoutSecs = transactionTimeoutSecs;
			this.clientId = clientId;
			this.applicationName = applicationName;
			this.storeMemoryAgingSwapRate = storeMemoryAgingSwapRate;
			this.timeZoneOffset = timeZoneOffset;
			this.authType = authType;
			this.connectionRoute = connectionRoute;
		}

		public void setUser(String user) {
			this.user = user;
		}

		public void setPassword(String password) {
			this.passwordDigest = Challenge.makeDigest(user, password);
		}

		public void setDatabase(String database) {
			this.database = database;
		}

		public void setOwnerMode(boolean ownerMode) {
			this.ownerMode = ownerMode;
		}

		public void setClientId(ClientId clientId) {
			this.clientId = clientId;
		}

		public boolean isOwnerMode() {
			return ownerMode;
		}

		public String getUser() {
			return user;
		}

		public String getClusterName() {
			return clusterName;
		}

		public String getDatabase() {
			return database;
		}

		public ClientId getClientId() {
			return clientId;
		}

		public String getApplicationName() {
			return applicationName;
		}

		public double getStoreMemoryAgingSwapRate() {
			return storeMemoryAgingSwapRate;
		}

		public SimpleTimeZone getTimeZoneOffset() {
			return timeZoneOffset;
		}

		public AuthType getAuthType() {
			return authType;
		}

		public static AuthType parseAuthType(String typeStr)
				throws GSException {
			final AuthType type = AUTH_TYPE_MAP.get(typeStr);
			if (type == null) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
						"Unknown authenthcation type (value=" +
								typeStr + ")");
			}
			return type;
		}

		private static final Map<String, AuthType> makeAuthTypeMap() {
			final Map<String, AuthType> map = new HashMap<String, AuthType>();
			for (AuthType type : AuthType.values()) {
				map.put(type.toPropertyString(), type);
			}
			return map;
		}

		public ConnectionRoute getConnectionRoute() {
			return connectionRoute;
		}
		
		public boolean isPublicConnection() {
			return (connectionRoute == ConnectionRoute.PUBLIC);
		}

		public static ConnectionRoute parseConnectionRoute(String routeStr)
				throws GSException {
			final ConnectionRoute route = CONNECTION_ROUTE_MAP.get(routeStr);
			if (route == null) {
				throw new GSException(
						GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
						"Unknown connection route (value=" +
								routeStr + ")");
			}
			return route;
		}

		private static final Map<String, ConnectionRoute> makeConnectionRouteMap() {
			final Map<String, ConnectionRoute> map = new HashMap<String, ConnectionRoute>();
			for (ConnectionRoute route : ConnectionRoute.values()) {
				if (!route.toPropertyString().isEmpty()) {
					map.put(route.toPropertyString(), route);
				}
			}
			return map;
		}
		
		public void putConnectionOption(BasicBuffer req) {
			if (connectionRoute == ConnectionRoute.PUBLIC) {
				final OptionalRequest optionalRequest = new OptionalRequest();
				optionalRequest.put(OptionalRequestType.CONNECTION_ROUTE,
						(byte) connectionRoute.ordinal());
				optionalRequest.format(req);
			}
			else {
				NodeConnection.tryPutEmptyOptionalRequest(req);
			}
		}
	}

	public enum AuthType {
		INTERNAL("INTERNAL"),
		EXTERNAL_LDAP("LDAP");

		private final String propertyString;

		private AuthType(String propertyString) {
			this.propertyString = propertyString;
		}

		public String toPropertyString() {
			return propertyString;
		}

	}

	public enum ConnectionRoute {
		DEFAULT(""),
		PUBLIC("PUBLIC");

		private final String propertyString;

		private ConnectionRoute(String propertyString) {
			this.propertyString = propertyString;
		}

		public String toPropertyString() {
			return propertyString;
		}

	}	
	
	public enum AuthMode {
		NONE,
		BASIC,
		CHALLENGE
	}

	public static class Challenge {

		private static final AuthMode DEFAULT_MODE = AuthMode.CHALLENGE;

		private static final String DEFAULT_METHOD = "POST";

		private static final String DEFAULT_REALM = "DB_Auth";

		private static final String DEFAULT_URI = "/";

		private static final String DEFAULT_QOP = "auth";

		private static final Random RANDOM = new SecureRandom();

		private static boolean challengeEnabled = false;

		private final boolean challenging;

		private final String nonce;

		private final String nc;

		private final String opaque;

		private final String baseSalt;

		private String cnonce;

		public Challenge() {
			challenging = false;
			this.nonce = null;
			this.nc = null;
			this.opaque = null;
			this.baseSalt = null;
		}

		public Challenge(
				String nonce, String nc, String opaque, String baseSalt) {
			challenging = true;
			this.nonce = nonce;
			this.nc = nc;
			this.opaque = opaque;
			this.baseSalt = baseSalt;
		}

		public static boolean isChallenging(Challenge challenge) {
			return (challenge != null && challenge.challenging);
		}

		public static PasswordDigest makeDigest(String user, String password) {
			return new PasswordDigest(
					sha256Hash(password),
					sha256Hash(user + ":" + password),
					md5Hash(user + ":" + DEFAULT_REALM + ":" + password),
					password);
		}

		public static void putRequest(
				BasicBuffer out, AuthMode mode, Challenge lastChallenge)
				throws GSException {
			if (mode == AuthMode.NONE) {
				out.putByteEnum(mode);
				return;
			}

			out.putByteEnum(mode);

			final boolean challenged = isChallenging(lastChallenge);
			out.putBoolean(challenged);

			if (challenged) {
				out.putString(lastChallenge.opaque);
				out.putString(lastChallenge.cnonce);
			}
		}

		public static Challenge getResponse(
				BasicBuffer in, AuthMode[] mode, Challenge lastChallenge)
				throws GSException {
			final AuthMode respMode = getMode(in);
			if (respMode != AuthMode.NONE && !challengeEnabled) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED, "");
			}

			if (respMode != mode[0]) {
				if (respMode == AuthMode.BASIC) {
					mode[0] = respMode;
					return new Challenge();
				}
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED, "");
			}
			else if (respMode == AuthMode.NONE) {
				return null;
			}

			final boolean respChallenging = in.getBoolean();
			final boolean challenging = isChallenging(lastChallenge);
			if (respMode != AuthMode.BASIC &&
					!(respChallenging ^ challenging)) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED, "");
			}

			if (respChallenging) {
				final String nonce = in.getString();
				final String nc = in.getString();
				final String opaque = in.getString();
				final String baseSalt = in.getString();

				return new Challenge(nonce, nc, opaque, baseSalt);
			}

			return null;
		}

		public static String build(
				AuthMode mode, Challenge challenge, PasswordDigest digest,
				AuthType type) {
			if (type == AuthType.EXTERNAL_LDAP) {
				return digest.password;
			}

			if (!isChallenging(challenge)) {
				if (mode == AuthMode.CHALLENGE) {
					return "";
				}
				else {
					return digest.basicSecret;
				}
			}

			return challenge.build(digest);
		}

		public String getOpaque() {
			return opaque;
		}

		public String getLastCNonce() {
			return cnonce;
		}

		public static String generateCNonce() {
			return randomHexString(4);
		}

		public String build(PasswordDigest digest) {
			final String cnonce = generateCNonce();
			final String result = build(digest, cnonce);

			this.cnonce = cnonce;
			return result;
		}

		public String build(PasswordDigest digest, String cnonce) {
			return "#1#" + getChallengeDigest(digest, cnonce) + "#" +
					getCryptSecret(digest);
		}

		public String getChallengeDigest(
				PasswordDigest digest, String cnonce) {
			final String ha1 = md5Hash(
					digest.challengeBase + ":" + nonce + ":" + cnonce);
			final String ha2 = md5Hash(DEFAULT_METHOD + ":" + DEFAULT_URI);

			return md5Hash(
					ha1 + ":" + nonce + ":" + nc + ":" + cnonce + ":" +
					DEFAULT_QOP + ":" + ha2);
		}

		public String getCryptSecret(PasswordDigest digest) {
			return sha256Hash(baseSalt + ":" + digest.cryptBase);
		}

		public static String sha256Hash(String value) {
			return hash(value, MessageDigestFactory.SHA256);
		}

		public static String md5Hash(String value) {
			return hash(value, MessageDigestFactory.MD5);
		}

		public static String hash(String value, MessageDigestFactory factory) {
			final MessageDigest md = factory.create();

			md.update(value.getBytes(BasicBuffer.DEFAULT_CHARSET));

			return bytesToHex(md.digest());
		}

		public static String randomHexString(int bytesSize) {
			final byte[] bytes = new byte[bytesSize];
			getRandom().nextBytes(bytes);
			return bytesToHex(bytes);
		}

		public static String bytesToHex(byte[] bytes) {
			final Formatter formatter =
					new Formatter(new StringBuilder(), Locale.US);

			for (byte b : bytes) {
				formatter.format("%02x", b);
			}

			return formatter.toString();
		}

		public static Random getRandom() {
			return RANDOM;
		}

		public static AuthMode getDefaultMode() {
			if (challengeEnabled) {
				return DEFAULT_MODE;
			}
			else {
				return AuthMode.NONE;
			}
		}

		public static AuthMode getMode(BasicBuffer in) throws GSException {
			if (in.base().remaining() > 0) {
				return in.getByteEnum(AuthMode.class);
			}
			return AuthMode.NONE;
		}

	}

	public static class UserTypeUtils {

		private static final String ADMIN_USER = "admin";
		private static final String SYSTEM_USER = "system";
		private static final String SPECIAL_USER_SYMBOL = "#";

		public static boolean checkAdmin(String userName) {
			return userName.equals(ADMIN_USER) ||
					userName.equals(SYSTEM_USER) ||
					userName.contains(SPECIAL_USER_SYMBOL);
		}
	}

	public static class PasswordDigest {

		private final String basicSecret;

		private final String cryptBase;

		private final String challengeBase;

		private transient final String password;

		public PasswordDigest(
				String basicSecret, String cryptSecret, String challengeBase,
				String password) {
			this.basicSecret = basicSecret;
			this.cryptBase = cryptSecret;
			this.challengeBase = challengeBase;
			this.password = password;
		}

		public String getBasicSecret() {
			return basicSecret;
		}

	}

	public enum FeatureVersion {
		V4_0,
		V4_1,
		V4_2,
		V4_3,
		V4_5,
		V5_3,
		V5_5,
		V5_6
		;

		public static FeatureVersion remoteValueOf(int ordinal)
				throws GSException {
			if (ordinal < 0) {
				throw new GSConnectionException(
						GSErrorCode.MESSAGE_CORRUPTED, "");
			}

			final FeatureVersion latest = latest();
			if (ordinal >= latest.ordinal()) {
				return latest();
			}
			return values()[ordinal];
		}

		public static FeatureVersion latest() {
			final FeatureVersion[] values = values();
			return values[values.length - 1];
		}

		public FeatureVersion merge(FeatureVersion another) {
			if (another != null && another.ordinal() > ordinal()) {
				return another;
			}
			return this;
		}

	}

	public static class MessageDigestFactory {

		public static final MessageDigestFactory SHA256 =
				new MessageDigestFactory("SHA-256");

		public static final MessageDigestFactory MD5 =
				new MessageDigestFactory("MD5");

		private final String algorithm;

		private final MessageDigest md;

		private final boolean cloneable;

		private MessageDigestFactory(String algorithm) {
			this.algorithm = algorithm;

			md = create(algorithm);

			boolean cloneable = false;
			try {
				cloneable = (md.clone() instanceof MessageDigest);
			}
			catch (CloneNotSupportedException e) {
			}
			this.cloneable = cloneable;
		}

		private static MessageDigest create(String algorithm) {
			try {
				return MessageDigest.getInstance(algorithm);
			}
			catch (NoSuchAlgorithmException e) {
				throw new Error("Internal error while calculating digest", e);
			}
		}

		public MessageDigest create() {
			if (cloneable) {
				try {
					return (MessageDigest) md.clone();
				}
				catch (CloneNotSupportedException e) {
				}
				catch (ClassCastException e) {
				}
			}
			return create(algorithm);
		}

	}

	public enum OptionalRequestType {

		LEGACY_VERSION_BLOCK(0, Byte.class),
		TRANSACTION_TIMEOUT(1, Integer.class),
		FOR_UPDATE(2, Boolean.class),
		CONTAINER_LOCK_REQUIRED(3, Boolean.class),
		SYSTEM_MODE(4, Boolean.class),
		DB_NAME(5, String.class),
		CONTAINER_ATTRIBUTE(6, Integer.class),
		ROW_INSERT_UPDATE(7, Integer.class),
		REQUEST_MODULE_TYPE(8, Byte.class),
		STATEMENT_TIMEOUT(10001, Integer.class),
		FETCH_LIMIT(10002, Long.class),
		FETCH_SIZE(10003, Long.class),
		CLIENT_ID(11001, ClientId.class),
		FETCH_BYTES_SIZE(11002, Integer.class),
		META_CONTAINER_ID(11003, Long.class),
		FEATURE_VERSION(11004, Integer.class),
		ACCEPTABLE_FEATURE_VERSION(11005, Integer.class),
		CONTAINER_VISIBILITY(11006, Integer.class),
		META_NAMING_TYPE(11007, Byte.class),
		QUERY_CONTAINER_KEY(11008, byte[].class),
		APPLICATION_NAME(11009, String.class),
		STORE_MEMORY_AGING_SWAP_RATE(11010, Double.class),
		TIME_ZONE_OFFSET(11011, Long.class),
		AUTHENTICATION_TYPE(11012, Byte.class),
		CONNECTION_ROUTE(11013, Byte.class);

		private final int id;

		private final Class<?> valueType;

		private OptionalRequestType(int id, Class<?> valueType) {
			this.id = id;
			this.valueType = valueType;
		}

		public int id() {
			return id;
		}

		public Class<?> valueType() {
			return valueType;
		}

	}

	private static class OptionalRequestIterator {

		private Iterator<Map.Entry<OptionalRequestType, Object>> base;

		private Iterator<Map.Entry<Integer, byte[]>> ext;

		private Integer id;

		private Class<?> valueType;

		OptionalRequestIterator(
				Map<OptionalRequestType, Object> requestMap,
				SortedMap<Integer, byte[]> extRequestMap) {
			if (!requestMap.isEmpty()) {
				base = requestMap.entrySet().iterator();
			}
			if (extRequestMap != null && !extRequestMap.isEmpty()) {
				ext = extRequestMap.entrySet().iterator();
			}
		}

		Object next() {
			Object nextValue;
			if (base != null) {
				final Map.Entry<OptionalRequestType, Object> entry =
						base.next();

				nextValue = entry.getValue();
				id = entry.getKey().id();
				valueType = entry.getKey().valueType();

				if (!base.hasNext()) {
					base = null;
				}
			}
			else if (ext != null) {
				final Map.Entry<Integer, byte[]> entry = ext.next();

				nextValue = entry.getValue();
				id = entry.getKey();
				valueType = byte[].class;

				if (!ext.hasNext()) {
					ext = null;
				}
			}
			else {
				nextValue = null;
				id = null;
				valueType = null;
			}
			return nextValue;
		}

		public int getId() {
			if (id == null) {
				throw new IllegalStateException();
			}
			return id;
		}

		public Class<?> getValueType() {
			if (valueType == null) {
				throw new IllegalStateException();
			}
			return valueType;
		}

	}

	public interface OptionalRequestSource {

		public boolean hasOptions();

		public void putOptions(OptionalRequest optionalRequest);

	}

	public static class OptionalRequest implements OptionalRequestSource {

		private static final int RANGE_SIZE = 1000;

		private static final int RANGE_START_ID = RANGE_SIZE * 11;

		private final Map<OptionalRequestType, Object> requestMap =
				new EnumMap<OptionalRequestType, Object>(
						OptionalRequestType.class);

		private SortedMap<Integer, byte[]> extRequestMap;

		@Override
		public boolean hasOptions() {
			return !requestMap.isEmpty() ||
					(extRequestMap == null || !extRequestMap.isEmpty());
		}

		@Override
		public void putOptions(OptionalRequest optionalRequest) {
			optionalRequest.requestMap.putAll(requestMap);

			if (extRequestMap != null) {
				optionalRequest.putExtAll(extRequestMap);
			}
		}

		public void clear() {
			requestMap.clear();
			extRequestMap = null;
		}

		public void putFeatureVersion(FeatureVersion version) {
			put(OptionalRequestType.LEGACY_VERSION_BLOCK, (byte) 1);
			put(OptionalRequestType.FEATURE_VERSION, version.ordinal());
		}

		public void putAcceptableFeatureVersion(FeatureVersion version) {
			put(
					OptionalRequestType.ACCEPTABLE_FEATURE_VERSION,
					version.ordinal());
		}

		public void put(OptionalRequestType type, Object value) {
			requestMap.put(type, type.valueType().cast(value));
		}

		public void putExt(int type, byte[] value) {
			if (extRequestMap == null) {
				extRequestMap = new TreeMap<Integer, byte[]>();
			}
			if (type <= RANGE_START_ID || type >= Short.MAX_VALUE) {
				throw new IllegalArgumentException();
			}
			extRequestMap.put(type, value);
		}

		public void putExtAll(SortedMap<Integer, byte[]> src) {
			if (extRequestMap == null) {
				extRequestMap = new TreeMap<Integer, byte[]>();
			}
			extRequestMap.putAll(src);
		}

		public void format(BasicBuffer req) {
			if (!hasOptions()) {
				req.putInt(0);
				return;
			}

			int lastRangeId = 0;
			int rangeHeadPos = -1;
			int rangeBodyPos = -1;

			final int headPos = req.base().position();
			req.putInt(0);

			final int bodyPos = req.base().position();
			final OptionalRequestIterator iterator =
					new OptionalRequestIterator(requestMap, extRequestMap);
			for (Object value; (value = iterator.next()) != null;) {
				final int id = iterator.getId();
				if (id >= RANGE_START_ID) {
					final int rangeId = id / RANGE_SIZE;
					if (rangeId != lastRangeId) {
						if (lastRangeId != 0) {
							putBodySize(req, rangeHeadPos, rangeBodyPos);
						}

						req.putShort((short) (rangeId * RANGE_SIZE));

						rangeHeadPos = req.base().position();
						req.putInt(0);
						rangeBodyPos = req.base().position();

						lastRangeId = rangeId;
					}
				}
				req.putShort((short) id);

				final Class<?> valueType = iterator.getValueType();
				if (valueType == Integer.class) {
					req.putInt((Integer) value);
				}
				else if (valueType == Long.class) {
					req.putLong((Long) value);
				}
				else if (valueType == Boolean.class) {
					req.putBoolean((Boolean) value);
				}
				else if (valueType == String.class) {
					req.putString((String) value);
				}
				else if (valueType == Byte.class) {
					req.put((Byte) value);
				}
				else if (valueType == Double.class) {
					req.putDouble((Double) value);
				}
				else if (valueType == ClientId.class) {
					final ClientId clientId = (ClientId) value;
					req.putLong(clientId.getSessionId());
					req.putUUID(clientId.getUUID());
				}
				else if (valueType == byte[].class) {
					final byte[] bytes = (byte[]) value;
					req.prepare(bytes.length);
					req.base().put(bytes);
				}
				else {
					throw new Error("Internal error by illegal value type");
				}
			}

			if (lastRangeId != 0) {
				putBodySize(req, rangeHeadPos, rangeBodyPos);
			}

			putBodySize(req, headPos, bodyPos);
		}

		private static void putBodySize(
				BasicBuffer req, int headPos, int bodyPos) {
			final int endPos = req.base().position();
			req.base().position(headPos);
			req.putInt(endPos - bodyPos);
			req.base().position(endPos);
		}

	}

	public interface RequestFormatter {

		void format(BasicBuffer buf) throws GSException;

	}

	public static abstract class BytesRequestFormatter
	implements RequestFormatter {

		public abstract void format(BasicBuffer buf) throws GSException;

		public byte[] format() throws GSException {
			return toBytes(this);
		}

		public static byte[] toBytes(
				RequestFormatter formatter) throws GSException {
			final BasicBuffer buf = new BasicBuffer(0);
			formatter.format(buf);
			buf.base().flip();

			final byte[] bytes = new byte[buf.base().remaining()];
			buf.base().get(bytes);
			return bytes;
		}

	}

	public abstract static class Hook {
		protected void startHeadReceiving(
				NodeConnection connection, Socket socket,
				long statementId) throws GSException {
		}
		protected void endHeadReceiving(
				NodeConnection connection, Socket socket) throws GSException {
		}
		protected void prepareHeartbeat(
				NodeConnection connection, Socket socket, long elapsedMillis)
				throws GSException {
		}
	}

}
