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
import java.io.UnsupportedEncodingException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executor;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import com.toshiba.mwcloud.gs.sql.internal.NodeConnection.LoginInfo;
import com.toshiba.mwcloud.gs.sql.internal.NodeConnection.OptionalRequest;
import com.toshiba.mwcloud.gs.sql.internal.NodeConnection.SocketType;
import com.toshiba.mwcloud.gs.sql.internal.PropertyUtils.WrappedProperties;
import com.toshiba.mwcloud.gs.sql.internal.SQLLaterFeatures.LaterConnection;
import com.toshiba.mwcloud.gs.sql.internal.SQLStatement.QueryPool;
import com.toshiba.mwcloud.gs.sql.internal.SQLStatement.StatementOperation;
import com.toshiba.mwcloud.gs.sql.internal.common.DriverProvider.TransportProvider;
import com.toshiba.mwcloud.gs.sql.internal.proxy.ProxyTargetInstanceFactory;

class SQLConnection implements Connection, LaterConnection {

	static final int SQL_STATEMENT_TYPE = 400;

	static final int CANCEL_STATEMENT_TYPE = 401;

	private static final int NOTIFICATION_STATEMENT_TYPE = 4000;

	private static final String DEFAULT_SERVICE_TYPE = "sql";
	private static final String DEFAULT_PUBLIC_SERVICE_TYPE = "sqlPublic";

	private static final int PARTITION_ADDRESS_STATEMENT_TYPE =
			NodeResolver.DEFAULT_PROTOCOL_CONFIG.getNormalStatementType(
					com.toshiba.mwcloud.gs.sql.internal.Statement.
					GET_PARTITION_ADDRESS);

	private static final NodeResolver.ProtocolConfig RESOLVER_PROTOCOL_CONFIG;

	static {
		RESOLVER_PROTOCOL_CONFIG = new NodeResolver.ProtocolConfig() {
			@Override
			public int getNotificationStatementType() {
				return NOTIFICATION_STATEMENT_TYPE;
			}

			@Override
			public int getNormalStatementType(
					com.toshiba.mwcloud.gs.sql.internal.Statement statement) {
				switch (statement) {
				case GET_PARTITION_ADDRESS:
					return PARTITION_ADDRESS_STATEMENT_TYPE;
				default:
					throw new Error();
				}
			}
		};
	}

	private static final String USER_NAME = "user";

	private static final String PASSWORD_NAME = "password";

	private static final String LOGIN_TIMEOUT_NAME = "loginTimeout";

	private static final String NETWORK_TIMEOUT_NAME =
			"internal.networkTimeout";

	private static final String HEARTBEAT_TIMEOUT_NAME =
			"internal.heartbeatTimeout";

	private static final String FAILOVER_INTERVAL_NAME =
			"internal.failoverInterval";

	private static final String APPLICATION_NAME_NAME = "applicationName";

	private static final String STORE_MEMORY_AGING_SWAP_RATE_NAME = "storeMemoryAgingSwapRate";

	private static final String TIME_ZONE_NAME = "timeZone";

	private static final String AUTHENTICATION_NAME = "authentication";

	private static final String CONNECTION_ROUTE_NAME = "connectionRoute";

	private static final String CATALOG_AND_SCHEMA_IGNORABLE_NAME =
			"catalogAndSchemaIgnorable";

	private static final long DEFAULT_LOGIN_TIMEOUT = 5 * 60 * 1000;

	private static final long DEFAULT_NETWORK_TIMEOUT = 5 * 60 * 1000;

	private static final long DEFAULT_HEARTBEAT_TIMEOUT = 1 * 60 * 1000;

	private static final long DEFAULT_FAILOVER_INTERVAL = 1 * 1000;

	private static final boolean DEFAULT_CATALOG_AND_SCHEMA_IGNORABLE = true;

	private static int sqlProtocolVersion = -9;

	private final List<InetSocketAddress> addressList;

	private final NodeConnection.Config connectionConfig;

	private final LoginInfo loginInfo;

	private final NodeResolver.ClusterInfo clusterInfo;

	private final String dbName;

	private final String userName;

	private final String url;

	private final NodeConnectionPool resolverConnections;

	private final NodeResolver nodeResolver;

	private final QueryPool queryPool = new QueryPool();

	private final Hook hook = new Hook();

	private final UUID uuid = UUID.randomUUID();

	private BaseConnection base;

	private BasicBuffer req;

	private BasicBuffer resp;

	private int lastReqSize;

	private int lastRespSize;

	private long lastQueryId;

	private int preferablePartitionId = -1;

	private long loginTimeoutMillis = DEFAULT_LOGIN_TIMEOUT;

	private long networkTimeoutMillis = DEFAULT_NETWORK_TIMEOUT;

	private long heartbeatTimeoutMillis = DEFAULT_HEARTBEAT_TIMEOUT;

	private long failoverIntervalMillis = DEFAULT_FAILOVER_INTERVAL;

	private boolean catalogAndSchemaIgnorable =
			DEFAULT_CATALOG_AND_SCHEMA_IGNORABLE;

	private boolean initialized;

	private boolean transactionStarted;

	private boolean autoCommit;

	private boolean remoteEnvLost;

	private boolean closed;

	private Map<String, String> remoteEnv;

	SQLConnection(String url, Properties info, Options options)
			throws SQLException {
		SQLErrorUtils.checkNullParameter(url, "url", null);
		SQLErrorUtils.checkNullParameter(info, "info", null);

		final URI driverURI;
		final URI gsURI;
		try {
			driverURI = toDriverURI(url);
			gsURI = toSpecificURI(driverURI);
		}
		catch (URISyntaxException e) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Illegal URI format (value=" + url +
					", reason=" + e.getMessage() + ")", e);
		}

		final Properties uriQuery = parseURIQuery(gsURI);

		final String[] userInfo = getUserInfo(gsURI, uriQuery, info);
		final String userName = userInfo[0];
		final String password = userInfo[1];

		final Properties mergedInfo = mergeClientInfo(gsURI, uriQuery, info);

		ServiceAddressResolver.Config sarConfig =
				new ServiceAddressResolver.Config();
		List<InetSocketAddress> memberList =
				new ArrayList<InetSocketAddress>();
		final InetAddress[] notificationInterfaceAddress =
				new InetAddress[1];
		addressList = getAddressList(
				gsURI, mergedInfo, sarConfig, memberList,
				notificationInterfaceAddress);
		if (!addressList.isEmpty()) {
			sarConfig = null;
			memberList = null;
		}

		connectionConfig = createConnectionConfig(mergedInfo, options);
		connectionConfig.setAlternativeVersion(sqlProtocolVersion);

		final String[] uriPath = parseURIPath(gsURI);
		final String clusterName = uriPath[0];
		final String dbName = uriPath[1];

		loginInfo = new LoginInfo(
				userName, password, true, dbName, clusterName, -1,
				resolveApplicationName(mergedInfo),
				resolveStoreMemoryAgingSwapRate(mergedInfo),
				resolveTimeZone(mergedInfo),
				resolveAuthType(mergedInfo),
				resolveConnectionRoute(mergedInfo));
		clusterInfo = new NodeResolver.ClusterInfo(loginInfo);

		this.userName = userName;
		this.dbName = dbName;
		this.url = formatURL(driverURI, gsURI, uriPath, uriQuery);

		req = new BasicBuffer(64);
		resp = new BasicBuffer(64);

		autoCommit = true;
		loginInfo.setOwnerMode(true);

		loginTimeoutMillis = DriverManager.getLoginTimeout() * 1000;
		if (loginTimeoutMillis <= 0) {
			loginTimeoutMillis = DEFAULT_LOGIN_TIMEOUT;
		}

		final Map<String, ClientInfoStatus> failedProperties =
				new HashMap<String, ClientInfoStatus>();
		setClientInfo(uriQuery, failedProperties, true);
		setClientInfo(info, failedProperties, true);

		final InetSocketAddress primaryAddress =
				(addressList.isEmpty() ? null : addressList.get(0));
		if (primaryAddress != null ||
				sarConfig != null || memberList != null) {
			resolverConnections = new NodeConnectionPool();
			resolverConnections.setMaxSize(0);

			final NodeResolver.AddressConfig addressConfig =
					new NodeResolver.AddressConfig();
			addressConfig.serviceType = DEFAULT_SERVICE_TYPE;
			if (loginInfo.isPublicConnection()) {
				addressConfig.serviceType = DEFAULT_PUBLIC_SERVICE_TYPE;
			}

			addressConfig.alwaysMaster = true;

			final boolean passive = (primaryAddress == null ? false :
					primaryAddress.getAddress().isMulticastAddress());
			try {
				nodeResolver = new NodeResolver(
						resolverConnections, passive, primaryAddress,
						connectionConfig, sarConfig, memberList,
						addressConfig, notificationInterfaceAddress[0]);
			}
			catch (GSException e) {
				throw SQLErrorUtils.error(
						0, "Failed to connect for multicast discovery (" +
						"reason=" + e.getMessage() + ")", e);
			}
			nodeResolver.setProtocolConfig(RESOLVER_PROTOCOL_CONFIG);
		}
		else {
			resolverConnections = null;
			nodeResolver = null;
		}

		final SQLStatement statement = new SQLStatement(this);
		try {
			statement.execute((StatementOperation) null, false);
		}
		finally {
			statement.close();
		}

		initialized = true;
	}

	static URI toDriverURI(String url) throws URISyntaxException {
		return new URI(url);
	}

	static URI toSpecificURI(URI driverURI) throws URISyntaxException {
		return new URI(driverURI.getRawSchemeSpecificPart());
	}

	private static List<InetSocketAddress> getAddressList(
			URI uri, Properties info,
			ServiceAddressResolver.Config sarConfig,
			List<InetSocketAddress> memberList,
			InetAddress[] notificationInterfaceAddress) throws SQLException {
		final List<InetSocketAddress> list =
				new ArrayList<InetSocketAddress>();

		if (uri.getHost() != null && uri.getPort() >= 0) {
			try {
				list.add(getSocketAddress(uri.getHost(), uri.getPort()));
			}
			catch (SQLException e) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_PARAMETER,
						"Address not resolved (" +
						"uri=" + uri +
						", reason=" + e.getMessage() + ")", e);
			}
		}
		else if (uri.getHost() == null ^ uri.getPort() < 0) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Host or port is not specified (uri=" + uri + ")", null);
		}

		final String nodeListString =
				(info == null ? null : info.getProperty("nodes"));
		if (nodeListString != null) {
			if (!list.isEmpty() &&
					list.get(0).getAddress().isMulticastAddress()) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_PARAMETER,
						"Multicast address in URI and nodesProperty can not " +
						"be specified at the same time (uri=" + uri +
						", nodesProperty=" + nodeListString + ")", null);
			}

			for (String nodeString : nodeListString.split(",")) {
				final String[] components = nodeString.split(":(?=[^:]+$)");
				if (components.length != 2) {
					throw SQLErrorUtils.error(
							SQLErrorUtils.ILLEGAL_PARAMETER,
							"Address separator not found (" +
							"target=" + nodeString +
							", nodesProperty=" + nodeListString + ")", null);
				}

				final int port;
				try {
					port = Integer.parseInt(components[1]);
				}
				catch (NumberFormatException e) {
					throw SQLErrorUtils.error(
							SQLErrorUtils.ILLEGAL_PARAMETER,
							"Illegal port format (" +
							"target=" + components[1] +
							", nodesProperty=" + nodeListString + ")", e);
				}

				final InetSocketAddress address;
				try {
					address = getSocketAddress(components[0], port);
				}
				catch (SQLException e) {
					throw SQLErrorUtils.error(
							SQLErrorUtils.ILLEGAL_PARAMETER,
							"Address not resolved (" +
							"target=" + nodeString +
							", nodesProperty=" + nodeListString +
							", reason=" + e.getMessage() + ")", e);
				}

				if (address.getAddress().isMulticastAddress()) {
					throw SQLErrorUtils.error(
							SQLErrorUtils.ILLEGAL_PARAMETER,
							"Multicast address specified for nodes (" +
							"target=" + nodeString +
							", nodesProperty=" + nodeListString + ")", null);
				}

				list.add(address);
			}
		}

		final List<String> notificationKeys = Arrays.asList(
				"notificationProvider", "notificationMember");
		final Properties notificationProps = new Properties();
		for (String name : notificationKeys) {
			if (info.containsKey(name)) {
				notificationProps.setProperty(name, info.getProperty(name));
			}
		}

		if (!list.isEmpty() && !notificationProps.isEmpty()) {
			final StringBuilder message = new StringBuilder();
			message.append("Notification property and other network address ");
			message.append("can not specified at the same time (uri=");
			message.append(uri);

			for (String key : notificationKeys) {
				if (info.containsKey(key)) {
					message.append(", ");
					message.append(key);
					message.append("=");
					message.append(info.getProperty(key));
				}
			}

			message.append(", otherAddress=");
			message.append(list);
			message.append(")");

			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER, message.toString(), null);
		}

		String key = "notificationInterfaceAddress";
		if (info.containsKey(key)) {
			notificationProps.setProperty(key, info.getProperty(key));
		}

		if (!notificationProps.isEmpty()) {
			final String destProtocolKey = "ipProtocol";
			final String srcProtocolKey = "internal." + destProtocolKey;
			if (info.containsKey(srcProtocolKey)) {
				notificationProps.setProperty(
						destProtocolKey, info.getProperty(srcProtocolKey));
			}

			final boolean[] passive = new boolean[1];
			try {
				NodeResolver.getAddressProperties(
						new WrappedProperties(notificationProps),
						passive, sarConfig, memberList, null,
						notificationInterfaceAddress);
			}
			catch (GSException e) {
				throw SQLErrorUtils.error(
						0, "Invalid notification properties (uri=" +	uri +
						"reason=" + e.getMessage() + ")", e);
			}
		}

		if (list.isEmpty() && sarConfig.getProviderURL() == null &&
				memberList.isEmpty()) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Address not specified (uri=" + uri + ")", null);
		}

		return list;
	}

	private static InetSocketAddress getSocketAddress(
			String host, int port) throws SQLException {
		final InetAddress inetAddress;
		try {
			inetAddress = InetAddress.getByName(host);
		}
		catch (UnknownHostException e) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Unknown host (host=" + host + ")", e);
		}

		try {
			return new InetSocketAddress(inetAddress, port);
		}
		catch (IllegalArgumentException e) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Illegal port number (port=" + port + ")", e);
		}
	}

	private static String[] parseURIPath(URI uri) throws SQLException {
		String clusterName = null;
		String dbName = null;
		do {
			final String rawPath = uri.getRawPath();
			if (rawPath == null || rawPath.isEmpty()) {
				break;
			}

			final String[] components = rawPath.split("/", 4);
			if (components.length < 2 || components.length > 3) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_PARAMETER,
						"Invalid URI path for cluster name and database name" +
						" (uri=" + uri + ")", null);
			}

			if (components.length >= 2) {
				clusterName = decodeURL(components[1]);
			}

			if (components.length >= 3) {
				dbName = decodeURL(components[2]);
			}
		}
		while (false);

		if (clusterName == null) {
			clusterName = "";
		}

		if (dbName == null || dbName.isEmpty()) {
			dbName = "public";
		}

		return new String[] { clusterName, dbName };
	}

	private static Properties parseURIQuery(URI uri) throws SQLException {
		final Properties uriQuery = new Properties();

		final String rawQuery = uri.getRawQuery();
		if (rawQuery == null) {
			return uriQuery;
		}

		for (String rawQueryEntry : rawQuery.split("&")) {
			final String[] components = rawQueryEntry.split("=", 2);

			final String key =
					(components.length > 0 ? decodeURL(components[0]) : null);
			final String value =
					(components.length > 1 ? decodeURL(components[1]) : "");

			if (key != null) {
				uriQuery.setProperty(key, value);
			}
		}

		return uriQuery;
	}

	private static String formatURL(
			URI driverURI, URI gsURI, String[] uriPath,
			Properties uriQuery) throws SQLException {
		try {
			final URI destSpecificUri = new URI(
					gsURI.getScheme(), gsURI.getRawAuthority(),
					formatURIPath(uriPath), formatURIQuery(uriQuery), null);
			final URI destDriverUri = new URI(
					driverURI.getScheme(), destSpecificUri.toString(), null);
			return destDriverUri.toString();
		}
		catch (URISyntaxException e) {
			throw SQLErrorUtils.error(0, null, e);
		}
	}

	private static String formatURIPath(String[] uriPath) throws SQLException {
		final StringBuilder builder = new StringBuilder();
		for (String component : uriPath) {
			builder.append("/");
			builder.append(encodeURL(component));
		}
		return builder.toString();
	}

	private static String formatURIQuery(Properties props) throws SQLException {
		final StringBuilder builder = new StringBuilder();
		for (String name : new TreeSet<String>(props.stringPropertyNames())) {
			if (name.equals(PASSWORD_NAME)) {
				
				continue;
			}
			final String value = props.getProperty(name);

			if (builder.length() > 0) {
				builder.append("&");
			}
			builder.append(encodeURL(name));
			builder.append("=");
			builder.append(encodeURL(value));
		}
		return (builder.length() > 0 ? builder.toString() : null);
	}

	private static String[] getUserInfo(
			URI uri, Properties uriQuery, Properties info)
			throws SQLException {
		String[] userInfo = { null, null };

		int specifiedCount = 0;
		boolean specifiedInURIUser = false;
		if (uri.getRawUserInfo() != null) {
			final String[] rawUserInfo = uri.getRawUserInfo().split(":", 3);
			if (rawUserInfo.length != 2) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_PARAMETER,
						"Illegal user info format on URI (uri=" + uri + ")",
						null);
			}

			final String user = decodeURL(rawUserInfo[0]);
			final String password = decodeURL(rawUserInfo[1]);

			userInfo = new String[] { user, password };
			specifiedCount++;
			specifiedInURIUser = true;
		}

		boolean specifiedInURIQuery = false;
		boolean specifiedInProperties = false;
		for (int i = 0; i < 2; i++) {
			final Properties props = (i == 0 ? uriQuery : info);
			if (props == null) {
				continue;
			}

			final String user = props.getProperty(USER_NAME);
			final String password = props.getProperty(PASSWORD_NAME);
			if (user == null ^ password == null) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_PARAMETER,
						"Only user or password is specified in " +
						(i == 0 ? "info properties" : "URI query") +
						" info properties (uri=" + uri +
						", userSpecified=" + (user != null) +
						", passwordSpecified=" + (password != null) + ")",
						null);
			}
			else if (user != null && password != null) {
				userInfo = new String[] { user, password };
				specifiedCount++;
				if (i == 0) {
					specifiedInURIQuery = true;
				}
				else {
					specifiedInProperties = true;
				}
			}
		}

		if (specifiedCount != 1) {
			if (specifiedCount >= 2) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_PARAMETER,
						"Multiple user info specified" +
						" (uri=" + uri +
						", specifiedInURIUser=" + specifiedInURIUser +
						", specifiedInURIQuery=" + specifiedInURIQuery +
						", specifiedInProperties=" + specifiedInProperties +
						")",
						null);
			}
			else {
				throw SQLErrorUtils.error(
						SQLErrorUtils.EMPTY_PARAMETER,
						"Neither user name nor password is specified (uri=" + uri + ")",
						null);
			}
		}

		return userInfo;
	}

	private static Properties mergeClientInfo(
			URI uri, Properties uriQuery, Properties info)
			throws SQLException {
		for (Object key : uriQuery.keySet()) {
			if (info.containsKey(key)) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_PARAMETER,
						"URI query and client info are conflicted (name=" +
						key + ", uri=" + uri + ")", null);
			}
		}

		final Properties mergedInfo = new Properties();
		mergedInfo.putAll(uriQuery);
		mergedInfo.putAll(info);
		return mergedInfo;
	}

	private static String resolveApplicationName(Properties props)
			throws SQLException {
		String applicationName =
				props.getProperty(APPLICATION_NAME_NAME, null);
		if (applicationName != null) {
			try {
				RowMapper.checkSymbol(applicationName, "application name");
			}
			catch (GSException e) {
				throw SQLErrorUtils.error(0, null, e);
			}
		}
		return applicationName;
	}

	private static double resolveStoreMemoryAgingSwapRate(Properties props)
			throws SQLException {
		final String name = STORE_MEMORY_AGING_SWAP_RATE_NAME;
		final String valueStr = props.getProperty(name, null);
		do {
			if (valueStr == null) {
				break;
			}

			final Double value;
			try {
				value = new WrappedProperties(props).getDoubleProperty(
						name, false);
			}
			catch (GSException e) {
				throw SQLErrorUtils.error(0, null, e);
			}

			if (value == null) {
				break;
			}

			if (!(0 <= value && value <= 1)) {
				throw SQLErrorUtils.error(
						GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
						"Property value out of range (name=" + name +
						", value=" + value + ")", null);
			}

			return value;
		}
		while (false);
		return -1;
	}

	private static SimpleTimeZone resolveTimeZone(Properties props)
			throws SQLException {
		final String zoneStr = props.getProperty(TIME_ZONE_NAME, null);
		if (zoneStr != null) {
			try {
				return PropertyUtils.parseTimeZoneOffset(zoneStr, true);
			}
			catch (GSException e) {
				throw SQLErrorUtils.error(0, null, e);
			}
		}
		return null;
	}

	private static NodeConnection.AuthType resolveAuthType(Properties props)
			throws SQLException {
		final String typeStr = props.getProperty(AUTHENTICATION_NAME, null);
		if (typeStr != null) {
			try {
				return NodeConnection.LoginInfo.parseAuthType(typeStr);
			}
			catch (GSException e) {
				throw SQLErrorUtils.error(0, null, e);
			}
		}
		return null;
	}

	private static NodeConnection.ConnectionRoute resolveConnectionRoute(Properties props)
			throws SQLException {
		final String routeStr = props.getProperty(CONNECTION_ROUTE_NAME, null);
		if (routeStr != null) {
			try {
				return NodeConnection.LoginInfo.parseConnectionRoute(routeStr);
			}
			catch (GSException e) {
				throw SQLErrorUtils.error(0, null, e);
			}
		}
		return null;
	}

	private static NodeConnection.Config createConnectionConfig(
			Properties props, Options options) throws SQLException {
		final NodeConnection.Config config = new NodeConnection.Config();
		final TransportProvider transProvider = options.getTransportProvider();
		try {
			final Properties transProps =
					resolveTransportProperties(props, transProvider);
			applySocketConfig(config, transProvider, transProps);
		}
		catch (IOException e) {
			throw SQLErrorUtils.error(0, null, e);
		}
		return config;
	}

	private static void applySocketConfig(
			NodeConnection.Config config, TransportProvider transProvider,
			Properties transProps)  throws IOException {
		final Set<SocketType> socketTypes = EnumSet.noneOf(SocketType.class);
		if (transProvider.isPlainSocketAllowed(transProps)) {
			socketTypes.add(SocketType.PLAIN);
		}

		final Map<SocketType, SocketFactory> socketFactories =
				new EnumMap<SocketType, SocketFactory>(SocketType.class);
		socketFactories.put(SocketType.PLAIN, SocketFactory.getDefault());
		{
			final SocketFactory secureFactory =
					transProvider.createSecureSocketFactory(transProps);
			if (secureFactory != null) {
				socketTypes.add(SocketType.SECURE);
				socketFactories.put(SocketType.SECURE, secureFactory);
			}
		}

		config.setSocketConfig(socketTypes, socketFactories);
	}

	private static Properties resolveTransportProperties(
			Properties props, TransportProvider transProvider)
			throws IOException {
		final Properties transProps = new Properties();
		transProvider.filterProperties(props, transProps);
		return transProps;
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
	public Statement createStatement() throws SQLException {
		checkOpened();
		ProxyTargetInstanceFactory proxyTargetInstanceFactory = ProxyTargetInstanceFactory.getInstance();
		return proxyTargetInstanceFactory.getTargetInstance(new SQLStatement(this));
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		checkOpened();
		SQLErrorUtils.checkNullParameter(sql, "sql", null);
		ProxyTargetInstanceFactory proxyTargetInstanceFactory = ProxyTargetInstanceFactory.getInstance();
		return proxyTargetInstanceFactory.getTargetInstance(new SQLPreparedStatement(this, sql));
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw SQLErrorUtils.errorNotSupported();
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		checkOpened();
		SQLErrorUtils.checkNullParameter(sql, "sql", null);
		return sql;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		checkOpened();
		
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		checkOpened();
		return autoCommit;
	}

	@Override
	public void commit() throws SQLException {
		checkOpened();
		
	}

	@Override
	public void rollback() throws SQLException {
		checkOpened();
		
	}

	@Override
	public void close() throws SQLException {
		closed = true;
		try {
			try {
				try {
					closeAllQueries();
				}
				finally {
					disconnect(false);
				}
			}
			finally {
				if (nodeResolver != null) {
					nodeResolver.close();
				}
			}
		}
		catch (GSException e) {
			throw SQLErrorUtils.error(
					0, "Failed to close related resources (reason=" +
					e.getMessage() + ")", e);
		}
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		checkOpened();
		ProxyTargetInstanceFactory proxyTargetInstanceFactory = ProxyTargetInstanceFactory.getInstance();
		return proxyTargetInstanceFactory.getTargetInstance(new SQLDatabaseMetaData(this, userName));
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		checkOpened();
		
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		checkOpened();
		return false;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		checkOpened();
		
	}

	@Override
	public String getCatalog() throws SQLException {
		checkOpened();
		return null;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		checkOpened();
		if (level != TRANSACTION_READ_COMMITTED) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.UNSUPPORTED_PARAMETER_VALUE,
					"Unsupported isolation level (value=" + level + ")", null);
		}
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		checkOpened();
		return TRANSACTION_READ_COMMITTED;
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
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		checkResultSetType(resultSetType);
		checkResultSetConcurrency(resultSetConcurrency);

		return createStatement();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		checkResultSetType(resultSetType);
		checkResultSetConcurrency(resultSetConcurrency);

		return prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		checkOpened();
		checkHoldability(holdability);
	}

	@Override
	public int getHoldability() throws SQLException {
		checkOpened();
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		checkResultSetType(resultSetType);
		checkResultSetConcurrency(resultSetConcurrency);
		checkHoldability(resultSetHoldability);

		return createStatement();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		checkResultSetType(resultSetType);
		checkResultSetConcurrency(resultSetConcurrency);
		checkHoldability(resultSetHoldability);

		return prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Clob createClob() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		if (timeout < 0) {
			throw SQLErrorUtils.error(
					SQLErrorUtils.ILLEGAL_PARAMETER,
					"Negative parameter", null);
		}
		return !closed;
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		try {
			SQLErrorUtils.checkNullParameter(name, "name", null);
		}
		catch (SQLException e) {
			throw SQLErrorUtils.errorClientInfo(0, null, null, e);
		}

		final Properties properties = new Properties();
		properties.setProperty(name, value);
		setClientInfo(properties);
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		final Map<String, ClientInfoStatus> failedProperties =
				new HashMap<String, ClientInfoStatus>();
		try {
			checkOpened();
			SQLErrorUtils.checkNullParameter(properties, "properties", null);

			setClientInfo(properties, failedProperties, false);
		}
		catch (SQLException e) {
			throw SQLErrorUtils.errorClientInfo(0, null, failedProperties, e);
		}
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return getClientInfo().getProperty(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		checkOpened();

		final Properties props = new Properties();

		props.setProperty(LOGIN_TIMEOUT_NAME,
				formatTimeoutProperty(loginTimeoutMillis));

		if (networkTimeoutMillis != DEFAULT_NETWORK_TIMEOUT) {
			props.setProperty(NETWORK_TIMEOUT_NAME,
					formatTimeoutProperty(networkTimeoutMillis));
		}

		if (heartbeatTimeoutMillis != DEFAULT_HEARTBEAT_TIMEOUT) {
			props.setProperty(HEARTBEAT_TIMEOUT_NAME,
					formatTimeoutProperty(heartbeatTimeoutMillis));
		}

		if (failoverIntervalMillis != DEFAULT_FAILOVER_INTERVAL) {
			props.setProperty(FAILOVER_INTERVAL_NAME,
					formatTimeoutProperty(failoverIntervalMillis));
		}

		if (loginInfo.getApplicationName() != null) {
			props.setProperty(
					APPLICATION_NAME_NAME, loginInfo.getApplicationName());
		}

		if (loginInfo.getStoreMemoryAgingSwapRate() >= 0) {
			props.setProperty(
					STORE_MEMORY_AGING_SWAP_RATE_NAME,
					"" + loginInfo.getStoreMemoryAgingSwapRate());
		}

		if (loginInfo.getTimeZoneOffset() != null) {
			props.setProperty(
					TIME_ZONE_NAME,
					PropertyUtils.formatTimeZoneOffset(
							loginInfo.getTimeZoneOffset().getRawOffset(),
							false));
		}

		if (loginInfo.getAuthType() != null) {
			props.setProperty(
					AUTHENTICATION_NAME,
					loginInfo.getAuthType().toPropertyString());
		}

		return props;
	}

	private void setClientInfo(
			Properties properties,
			Map<String, ClientInfoStatus> failedProperties,
			boolean unknownAllowed)
			throws SQLException {
		
		if (!unknownAllowed) {
			final Set<Object> unknownNames =
					new HashSet<Object>(properties.keySet());
			for (ClientInfoKey<?> key : ClientInfoKey.getAll()) {
				unknownNames.remove(key.getName());
			}

			for (Object nameObj : unknownNames) {
				if (nameObj instanceof String) {
					failedProperties.put(
							(String) nameObj,
							ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
				}
			}

			if (!unknownNames.isEmpty()) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_PARAMETER,
						"Unknown properties found (name(s)=" +
								unknownNames + ")", null);
			}
		}

		
		
		final List<ClientInfoKey.Entry<?>> entryList =
				new ArrayList<ClientInfoKey.Entry<?>>();
		for (ClientInfoKey<?> key : ClientInfoKey.getAll()) {
			final String strValue = properties.getProperty(key.getName());
			if (strValue != null) {
				entryList.add(
						key.parseEntry(failedProperties, this, strValue));
			}
		}

		
		for (ClientInfoKey.Entry<?> entry : entryList) {
			entry.apply(this);
		}
	}

	private static long getTimeoutProperty(
			Map<String, ClientInfoStatus> failedProperties,
			String name, String value) throws SQLException {
		try {
			return Math.max(Integer.parseInt(value) * 1000L, -1);
		}
		catch (NumberFormatException e) {
			failedProperties.put(name, ClientInfoStatus.REASON_VALUE_INVALID);
			throw SQLErrorUtils.error(SQLErrorUtils.ILLEGAL_PARAMETER,
					"Failed to parse timeout value (name=" + name +
					", value=" + value + ", reason=" + e.getMessage() + ")", e);
		}
	}

	private static boolean getBooleanProperty(
			Map<String, ClientInfoStatus> failedProperties,
			String name, String value) throws SQLException {
		if (Boolean.TRUE.toString().equals(value)) {
			return true;
		}
		else if (Boolean.FALSE.toString().equals(value)) {
			return false;
		}
		else {
			failedProperties.put(name, ClientInfoStatus.REASON_VALUE_INVALID);
			throw SQLErrorUtils.error(SQLErrorUtils.ILLEGAL_PARAMETER,
					"Failed to parse boolean value (name=" + name +
					", value=" + value + ")", null);
		}
	}

	private static String formatTimeoutProperty(long timeoutMillis) {
		final long timeoutSecs = Math.max(timeoutMillis / 1000, -1);
		return Long.toString(timeoutSecs);
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	private void checkOpened() throws SQLException {
		if (isClosed()) {
			throw SQLErrorUtils.errorAlreadyClosed();
		}
	}

	BaseConnection base() {
		return base;
	}

	Hook getHook() {
		return hook;
	}

	long getNetworkTimeoutMillis() {
		if (!initialized) {
			if (loginTimeoutMillis <= 0) {
				return DEFAULT_LOGIN_TIMEOUT;
			}
			return loginTimeoutMillis;
		}

		if (networkTimeoutMillis <= 0) {
			return DEFAULT_NETWORK_TIMEOUT;
		}
		return networkTimeoutMillis;
	}

	long getFailoverIntervalMillis() {
		if (failoverIntervalMillis <= 0) {
			return DEFAULT_FAILOVER_INTERVAL;
		}
		return failoverIntervalMillis;
	}

	boolean isCatalogAndSchemaIgnorable() {
		return catalogAndSchemaIgnorable;
	}

	static void fillRequestHead(NodeConnection base, BasicBuffer req) {
		final boolean ipv6Enabled = (((InetSocketAddress)
				base.getRemoteSocketAddress()
				).getAddress() instanceof Inet6Address);
		NodeConnection.fillRequestHead(ipv6Enabled, req);
	}

	private InetSocketAddress getNextAddress(
			NodeConnection.Config connectionConfig)
			throws SQLException, GSException {
		int partitionCount;
		if (nodeResolver == null) {
			partitionCount = addressList.size();
		}
		else {
			nodeResolver.setConnectionConfig(connectionConfig);
			nodeResolver.setNotificationReceiveTimeoutMillis(
					connectionConfig.getConnectTimeoutMillis());
			partitionCount = nodeResolver.getPartitionCount(clusterInfo);
		}

		partitionCount = Math.min(partitionCount, Integer.MAX_VALUE - 1);

		if (preferablePartitionId < 0) {
			if (partitionCount == 0) {
				throw new Error();
			}
			preferablePartitionId = (int) ((uuid.getLeastSignificantBits() &
					((long)(1L << Integer.SIZE) - 1)) % partitionCount);

		}
		preferablePartitionId = (++preferablePartitionId >= partitionCount ?
				0 : preferablePartitionId);

		final InetSocketAddress address;
		if (nodeResolver == null) {
			address = addressList.get(preferablePartitionId);
		}
		else {
			address = nodeResolver.getNodeAddress(
					clusterInfo, preferablePartitionId, false);
		}

		return address;
	}

	int getPreferablePartitionId() {
		return preferablePartitionId;
	}

	void reset() throws SQLException, GSException {
		disconnect(false);

		final NodeConnection.Config connectionConfig =
				new NodeConnection.Config();
		connectionConfig.set(this.connectionConfig, true);

		if (heartbeatTimeoutMillis > 0) {
			connectionConfig.setHeartbeatTimeoutMillis(heartbeatTimeoutMillis);
		}

		final long timeout = getNetworkTimeoutMillis();
		if (timeout < connectionConfig.getConnectTimeoutMillis()) {
			connectionConfig.setConnectTimeoutMillis(timeout);
		}

		if (timeout < connectionConfig.getStatementTimeoutMillis()) {
			connectionConfig.setStatementTimeoutMillis(timeout);
			connectionConfig.setStatementTimeoutEnabled(true);
		}

		base = new BaseConnection(new NodeConnection(
				getNextAddress(connectionConfig), connectionConfig));
		base.base.setHook(hook);

		boolean succeeded = false;
		try {
			loginInfo.setOwnerMode(autoCommit);

			fillRequestHead(base.base, req);

			base.base.connect(req, resp);
			base.base.login(req, resp, loginInfo, null);

			base.base.setConfig(this.connectionConfig);
			base.lastHeartbeatCount = base.base.getHeartbeatReceiveCount();

			succeeded = true;
		}
		finally {
			if (!succeeded) {
				disconnect(false);
			}
		}
	}

	void disconnect(boolean immediate) throws GSException {
		if (base != null) {
			try {
				if (immediate) {
					base.base.closeImmediately();
				}
				else {
					base.base.close();
				}
			}
			finally {
				try {
					if (nodeResolver != null) {
						nodeResolver.invalidateMaster(clusterInfo);
					}
				}
				finally {
					base = null;
					remoteEnvLost = true;
				}
			}
		}
	}

	boolean isTransactionStarted() {
		return transactionStarted;
	}

	void updateBufferStatus(int lastReqSize, int lastRespSize) {
		this.lastReqSize = lastReqSize;
		this.lastRespSize = lastRespSize;
	}

	void updateTransactionStatus(
			boolean transactionStarted, boolean autoCommit) {
		this.transactionStarted = transactionStarted;
		this.autoCommit = autoCommit;
	}

	BasicBuffer takeReqBuffer() {
		return new BasicBuffer(getDesiredBufferSize(lastReqSize));
	}

	BasicBuffer takeRespBuffer() {
		return new BasicBuffer(getDesiredBufferSize(lastRespSize));
	}

	private static int getDesiredBufferSize(int lastSize) {
		return Math.max(1 <<
				(Integer.highestOneBit(Math.max(lastSize, 64) - 1) + 1), 0);
	}

	QueryPool getQueryPool() {
		return queryPool;
	}

	UUID getUUID() {
		return uuid;
	}

	String getDbName() {
		return dbName;
	}

	String getURL() {
		return url;
	}

	SimpleTimeZone getTimeZoneOffset() {
		return loginInfo.getTimeZoneOffset();
	}

	long generateQueryId() {
		while (++lastQueryId == 0) {
		}
		return lastQueryId;
	}

	Map<String, String> getPendingRemoteEnv() {
		return (remoteEnvLost ? remoteEnv : null);
	}

	void acceptRemoteEnv(Map<String, String> env) throws SQLException {
		if (remoteEnv == null) {
			remoteEnv = new HashMap<String, String>();
		}

		final boolean recovered =
				remoteEnvLost && env.keySet().containsAll(remoteEnv.keySet());

		remoteEnv.putAll(env);

		if (recovered) {
			remoteEnvLost = false;
		}
	}

	void putOptionalRequest(OptionalRequest req) {
		final Map<String, String> env = getPendingRemoteEnv();
		if (env == null) {
			return;
		}

		req.putExt(ExtRequestOptionType.SQL_ENVIRONMENT.number(),
				new ExtOptionFormatter() {
					@Override
					public void format(BasicBuffer buf) {
						exportRemoteEnv(buf, env);
					}
				}.format());
	}

	static void exportRemoteEnv(BasicBuffer buf, Map<String, String> env) {
		buf.putInt(env.size());
		for (Map.Entry<String, String> entry : env.entrySet()) {
			buf.putString(entry.getKey());
			buf.putString(entry.getValue());
		}
	}

	static Map<String, String> importRemoteEnv(BasicBuffer buf) {
		final int count = buf.base().getInt();
		if (count <= 0) {
			return null;
		}

		final Map<String, String> env = new HashMap<String, String>();
		for (int i = 0; i < count; i++) {
			final String key = buf.getString();
			final String value = buf.getString();
			env.put(key, value);
		}

		return env;
	}

	private void closeAllQueries() throws SQLException {
		if (queryPool.isEmpty()) {
			return;
		}

		queryPool.detachAll(this);
		final SQLStatement statement = new SQLStatement(this);
		for (;;) {
			final long queryId = queryPool.pull(this);
			statement.closeQuery(queryId);
			if (queryId == 0) {
				break;
			}
		}
	}

	private void checkResultSetType(
			int resultSetType) throws SQLException {
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
			throw SQLErrorUtils.errorNotSupportedFeature(
					SQLErrorUtils.OPTIONAL_FEATURE_NOT_SUPPORTED,
					"Unsupported result set type (value=" +
					resultSetType + ")", null);
		}
	}

	private void checkResultSetConcurrency(
			int resultSetConcurrency) throws SQLException {
		if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw SQLErrorUtils.errorNotSupportedFeature(
					SQLErrorUtils.OPTIONAL_FEATURE_NOT_SUPPORTED,
					"Unsupported result set concurrency (value=" +
					resultSetConcurrency + ")", null);
		}
	}

	private void checkHoldability(int holdability) throws SQLException {
		if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
			throw SQLErrorUtils.errorNotSupportedFeature(
					SQLErrorUtils.OPTIONAL_FEATURE_NOT_SUPPORTED,
					"Unsupported holdability (value=" + holdability + ")",
					null);
		}
	}

	private static String decodeURL(String src) throws SQLException {
		try {
			return URLDecoder.decode(src, "UTF-8");
		}
		catch (UnsupportedEncodingException e) {
			throw new Error(e);
		}
	}

	private static String encodeURL(String src) throws SQLException {
		try {
			return URLEncoder.encode(src, "UTF-8");
		}
		catch (UnsupportedEncodingException e) {
			throw new Error(e);
		}
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public String getSchema() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	static class BaseConnection {

		final NodeConnection base;

		long lastHeartbeatCount;

		GSConnectionException lastException;

		public BaseConnection(NodeConnection base) {
			this.base = base;
		}

	}

	static class Hook extends NodeConnection.Hook {

		private NodeConnection connection;

		private Socket socket;

		private long statementId;

		private Query query;

		@Override
		protected synchronized void startHeadReceiving(
				NodeConnection connection, Socket socket,
				long statementId) throws GSException {
			this.connection = connection;
			this.socket = socket;
			this.statementId = statementId;

			final Query query = this.query;
			if (query != null && query.cancelRequested) {
				sendCancelRequest(query);
			}
		}

		@Override
		protected synchronized void endHeadReceiving(
				NodeConnection connection, Socket socket) throws GSException {
			this.connection = null;
			this.socket = null;
		}

		@Override
		protected synchronized void prepareHeartbeat(
				NodeConnection connection, Socket socket, long elapsedMillis)
				throws GSException {
			final Query query = this.query;
			if (query != null) {
				query.elapsedMillis = elapsedMillis;
			}

			if (query == null || query.cancelRequested) {
				return;
			}

			if (query.timeoutMillis <= 0 || elapsedMillis < query.timeoutMillis) {
				return;
			}
			this.connection = connection;
			this.socket = socket;
			try {
				query.cancelRequested = true;
				sendCancelRequest(query);
			}
			finally {
				this.connection = null;
				this.socket = null;
			}
		}

		void startQuery(
				int partitionId, UUID clientUUID, long queryId,
				long timeoutMillis) {
			query = new Query(partitionId, clientUUID, queryId, timeoutMillis);
		}

		void endQuery() {
			query = null;
		}

		Long getElapsedMillis() {
			return (query == null ? null : query.elapsedMillis);
		}

		synchronized void cancelQuery() throws GSException {
			final Query query = this.query;
			if (query == null || query.cancelRequested) {
				return;
			}

			query.cancelRequested = true;
			sendCancelRequest(query);
		}

		private void sendCancelRequest(Query query) throws GSException {
			if (connection == null) {
				return;
			}

			final boolean ipv6Enabled = (((InetSocketAddress)
					socket.getRemoteSocketAddress()
					).getAddress() instanceof Inet6Address);

			final BasicBuffer req = new BasicBuffer(64);
			NodeConnection.fillRequestHead(ipv6Enabled, req);

			final int reqHeadLength = req.base().position();
			final int eeHeadLength = reqHeadLength -
					Integer.SIZE / Byte.SIZE -
					Integer.SIZE / Byte.SIZE -
					Long.SIZE / Byte.SIZE;

			req.putUUID(query.clientUUID);
			req.putLong(query.queryId);

			final int reqLength = req.base().position();
			final int statementTypeNumber =
					SQLConnection.CANCEL_STATEMENT_TYPE;
			final int partitionId = query.partitionId;

			req.base().position(eeHeadLength - Integer.SIZE / Byte.SIZE);
			req.base().putInt(reqLength - eeHeadLength);
			req.base().putInt(statementTypeNumber);
			req.base().putInt(partitionId);
			NodeConnection.putStatementId(req, statementId, false);

			try {
				socket.getOutputStream().write(
						req.base().array(), 0, reqLength);
			}
			catch (IOException e) {
				throw new GSConnectionException(
						GSErrorCode.BAD_CONNECTION,
						"Failed to send message (address=" +
						connection.getRemoteSocketAddress() +
						", reason=" + e.getMessage() + ")", e);
			}
		}

		private static class Query {

			private final int partitionId;

			private final UUID clientUUID;

			private final long queryId;

			private final long timeoutMillis;

			private long elapsedMillis;

			private boolean cancelRequested;

			private Query(
					int partitionId, UUID clientUUID, long queryId,
					long timeoutMillis) {
				this.partitionId = partitionId;
				this.clientUUID = clientUUID;
				this.queryId = queryId;
				this.timeoutMillis = timeoutMillis;
			}

		}

	}

	enum ExtRequestOptionType {

		SQL_ENVIRONMENT(14001);

		private final int number;

		ExtRequestOptionType(int number) {
			this.number = number;
		}

		public int number() {
			return number;
		}

	}

	static abstract class ExtOptionFormatter {

		public abstract void format(BasicBuffer buf);

		byte[] format() {
			final BasicBuffer buf = new BasicBuffer(0);
			format(buf);
			buf.base().flip();

			final byte[] bytes = new byte[buf.base().remaining()];
			buf.base().get(bytes);
			return bytes;
		}

	}

	static class Options {

		private TransportProvider transportProvider =
				new PlainTransportProvider();

		Options(Options src) {
			if (src != null) {
				transportProvider = src.transportProvider;
			}
		}

		public TransportProvider getTransportProvider() {
			return transportProvider;
		}

		public void setTransportProvider(TransportProvider provider) {
			this.transportProvider = provider;
		}

	}

	private static class PlainTransportProvider implements TransportProvider {

		@Override
		public void filterProperties(Properties src, Properties transProps)
				throws GSException {
			for (String key : getReservedTransportPropertyKeys()) {
				if (src.containsKey(key)) {
					throw new GSException(
							GSErrorCode.ILLEGAL_PROPERTY_ENTRY,
							"Unacceptable property specified because of " +
							"lack of extra library (key=" + key + ")");
				}
			}
		}

		@Override
		public boolean isPlainSocketAllowed(Properties props)
				throws GSException {
			return true;
		}

		@Override
		public SSLSocketFactory createSecureSocketFactory(Properties props)
				throws GSException {
			return null;
		}

		public static java.util.Collection<String>
		getReservedTransportPropertyKeys() {
			return Arrays.asList("sslMode");
		}

	}

	static abstract class ClientInfoKey<T> {

		private static final List<ClientInfoKey<?>> ALL_KEYS =
				Arrays.<ClientInfoKey<?>>asList(
						ofLoginTimeout(),
						ofNetworkTimeout(),
						ofHeartbearTimeout(),
						ofFailoverInterval(),
						ofCatalogAndSchemaIgnorable());

		private final String name;

		ClientInfoKey(String name) {
			this.name = name;
		}

		static List<ClientInfoKey<?>> getAll() {
			return ALL_KEYS;
		}

		String getName() {
			return name;
		}

		Entry<T> parseEntry(
				Map<String, ClientInfoStatus> failedProperties,
				SQLConnection conn, String value) throws SQLException {
			return new Entry<T>(this, parse(failedProperties, conn, value));
		}

		abstract void apply(SQLConnection conn, T value);

		abstract T parse(
				Map<String, ClientInfoStatus> failedProperties,
				SQLConnection conn, String value) throws SQLException;

		private static MilliTimeKey ofLoginTimeout() {
			return new MilliTimeKey(LOGIN_TIMEOUT_NAME) {
				@Override
				void apply(SQLConnection conn, Long value) {
					conn.loginTimeoutMillis = value;
				}
			};
		}

		private static MilliTimeKey ofNetworkTimeout() {
			return new MilliTimeKey(NETWORK_TIMEOUT_NAME) {
				@Override
				void apply(SQLConnection conn, Long value) {
					conn.networkTimeoutMillis = value;
				}
			};
		}

		private static MilliTimeKey ofHeartbearTimeout() {
			return new MilliTimeKey(NETWORK_TIMEOUT_NAME) {
				@Override
				void apply(SQLConnection conn, Long value) {
					conn.heartbeatTimeoutMillis = value;
				}
			};
		}

		private static MilliTimeKey ofFailoverInterval() {
			return new MilliTimeKey(NETWORK_TIMEOUT_NAME) {
				@Override
				void apply(SQLConnection conn, Long value) {
					conn.failoverIntervalMillis = value;
				}
			};
		}

		private static BooleanKey ofCatalogAndSchemaIgnorable() {
			return new BooleanKey(CATALOG_AND_SCHEMA_IGNORABLE_NAME) {
				@Override
				void apply(SQLConnection conn, Boolean value) {
					conn.catalogAndSchemaIgnorable = value;
				}
			};
		}

		static class Entry<T> {
			final ClientInfoKey<T> key;
			final T value;

			public Entry(ClientInfoKey<T> key, T value) {
				this.key = key;
				this.value = value;
			}

			void apply(SQLConnection conn) {
				key.apply(conn, value);
			}

		}

		static abstract class MilliTimeKey extends ClientInfoKey<Long> {

			public MilliTimeKey(String name) {
				super(name);
			}

			@Override
			Long parse(
					Map<String, ClientInfoStatus> failedProperties,
					SQLConnection conn, String value) throws SQLException {
				return getTimeoutProperty(failedProperties, getName(), value);
			}

		}

		static abstract class BooleanKey extends ClientInfoKey<Boolean> {

			public BooleanKey(String name) {
				super(name);
			}

			@Override
			Boolean parse(
					Map<String, ClientInfoStatus> failedProperties,
					SQLConnection conn, String value) throws SQLException {
				return getBooleanProperty(failedProperties, getName(), value);
			}

		}

	}

}
