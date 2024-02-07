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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Locale;
import java.util.ResourceBundle;

import com.toshiba.mwcloud.gs.sql.internal.SQLLaterFeatures.LaterDatabaseMetadata;

class SQLDatabaseMetaData
implements DatabaseMetaData, LaterDatabaseMetadata {

	private static final int JDBC_MAJOR_VERSION = 4;

	private static final int JDBC_MINOR_VERSION = 2;

	private static boolean exactMatchParameterStrict = false;

	private static boolean patternMatchParameterStrict = false;

	private final SQLConnection connection;

	private final String userName;

	private ClusterInfo clusterInfo;

	public SQLDatabaseMetaData(SQLConnection connection, String userName) {
		this.connection = connection;
		this.userName = userName;
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
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public String getURL() throws SQLException {
		return connection.getURL();
	}

	@Override
	public String getUserName() throws SQLException {
		return userName;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return !nullsAreSortedHigh();
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return DriverInfo.getInstance().databaseProductName;
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return getDatabaseMajorVersion() + "." + getDatabaseMinorVersion();
	}

	@Override
	public String getDriverName() throws SQLException {
		return DriverInfo.getInstance().driverName;
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return getDriverMajorVersion() + "." + getDriverMinorVersion();
	}

	@Override
	public int getDriverMajorVersion() {
		return DriverInfo.getInstance().driverMajorVersion;
	}

	@Override
	public int getDriverMinorVersion() {
		return DriverInfo.getInstance().driverMinorVersion;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "\"";
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return "";
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "\\";
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return getClusterInfo().extraNameCharacters;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "schema";
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return "procedure";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return "catalog";
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 0;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return false;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_READ_COMMITTED;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		return level == Connection.TRANSACTION_READ_COMMITTED;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return true;
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException {
		checkPatternMatchParameter(
				procedureNamePattern, "procedureNamePattern", null);

		return executeQuery("select" +
				" null as PROCEDURE_CAT" +
				", null as PROCEDURE_SCHEM" +
				", null as PROCEDURE_NAME" +
				", null" +
				", null" +
				", null" +
				", null as REMARKS" +
				", null as PROCEDURE_TYPE" +
				", null as SPECIFIC_NAME" +
				" limit 0");
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern,
			String procedureNamePattern, String columnNamePattern)
			throws SQLException {
		checkPatternMatchParameter(
				procedureNamePattern, "procedureNamePattern", null);
		checkPatternMatchParameter(
				columnNamePattern, "columnNamePattern", null);

		return executeQuery("select" +
				" null as PROCEDURE_CAT" +
				", null as PROCEDURE_SCHEM" +
				", null as PROCEDURE_NAME" +
				", null as COLUMN_NAME" +
				", null as COLUMN_TYPE" +
				", null as DATA_TYPE" +
				", null as TYPE_NAME" +
				", null as PRECISION" +
				", null as LENGTH" +
				", null as SCALE" +
				", null as RADIX" +
				", null as NULLABLE" +
				", null as REMARKS" +
				", null as COLUMN_DEF" +
				", null as SQL_DATA_TYPE" +
				", null as SQL_DATETIME_SUB" +
				", null as CHAR_OCTET_LENGTH" +
				", null as ORDINAL_POSITION" +
				", null as IS_NULLABLE" +
				", null as SPECIFIC_NAME" +
				" limit 0");
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		checkPatternMatchParameter(
				tableNamePattern, "tableNamePattern", null);

		boolean matched = matchCatalogAndSchema(catalog, schemaPattern);
		if (types != null) {
			boolean typeMatched = false;
			for (final String type : types) {
				SQLErrorUtils.checkNullParameter(type, "element of types", null);
				typeMatched |= type.toUpperCase(Locale.US).equals("TABLE");
				typeMatched |= type.toUpperCase(Locale.US).equals("VIEW");
			}
			matched &= typeMatched;
		}
		final StringBuilder where = new StringBuilder();
		if (tableNamePattern != null) {
			where.append(
					" where upper(TABLE_NAME) like upper('" +
					escape(tableNamePattern) +
					"') escape '" +
					getSearchStringEscape() + "'");
		}
		if (types != null) {
			if (where.length() > 0) {
				where.append(" and ");
			}
			else {
				where.append(" where ");
			}
			where.append("upper(TABLE_TYPE) IN (");
			final int inPos = where.length();
			for (final String type : types) {
				if (inPos != where.length()) {
					where.append(", ");
				}
				where.append(" upper('").append(escape(type)).append("')");
			}
			where.append(" )");
		}
		
		String sql = "select" +
				" TABLE_CAT" +
				", TABLE_SCHEM" +
				", TABLE_NAME" +
				", TABLE_TYPE" +
				", REMARKS" +
				", TYPE_CAT" +
				", TYPE_SCHEM" +
				", TYPE_NAME" +
				", SELF_REFERENCING_COL_NAME" +
				", REF_GENERATION" +
				" from [#_driver_tables]" +
				where +
				" order by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME" +
				(matched ? "" : " limit 0");

		return executeMetaQuery(sql);
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return executeQuery("select" +
				" null as TABLE_SCHEM" +
				", null as TABLE_CATALOG" +
				" limit 0");
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		return executeQuery("select null as TABLE_CAT limit 0");
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		String sql = "select" +
				" TABLE_TYPE" +
				" from [#_driver_table_types]" +
				" order by TABLE_TYPE";

		return executeMetaQuery(sql);
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		checkPatternMatchParameter(
				tableNamePattern, "tableNamePattern", null);
		checkPatternMatchParameter(
				columnNamePattern, "columnNamePattern", null);

		String where = "";
		if (tableNamePattern != null || columnNamePattern != null) {
			where = " where";
			if (tableNamePattern != null) {
				where += " upper(TABLE_NAME) like upper('" +
						escape(tableNamePattern) +
						"') escape '" +
						getSearchStringEscape() + "'";
			}
			if (columnNamePattern != null) {
				if (tableNamePattern != null) {
					where += " and";
				}
				where += " upper(COLUMN_NAME) like upper('" +
						escape(columnNamePattern) +
						"') escape '" +
						getSearchStringEscape() + "'";
			}
		}
		
		String sql =
				"select" +
				" TABLE_CAT" +
				", TABLE_SCHEM" +
				", TABLE_NAME" +
				", COLUMN_NAME" +
				", DATA_TYPE" +
				", TYPE_NAME" +
				", COLUMN_SIZE" +
				", BUFFER_LENGTH" +
				", DECIMAL_DIGITS" +
				", NUM_PREC_RADIX" +
				", NULLABLE" +
				", REMARKS" +
				", COLUMN_DEF" +
				", SQL_DATA_TYPE" +
				", SQL_DATETIME_SUB" +
				", CHAR_OCTET_LENGTH" +
				", ORDINAL_POSITION" +
				", IS_NULLABLE" +
				", SCOPE_CATALOG" +
				", SCOPE_SCHEMA" +
				", SCOPE_TABLE" +
				", SOURCE_DATA_TYPE" +
				", IS_AUTOINCREMENT" +
				", IS_GENERATEDCOLUMN" +
				" from [#_driver_columns]" +
				where +
				" order by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";

		return executeMetaQuery(sql);
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema,
			String table, String columnNamePattern) throws SQLException {
		checkExactMatchParameter(table, "table", null);
		checkPatternMatchParameter(
				columnNamePattern, "columnNamePattern", null);

		return executeQuery("select" +
				" null as TABLE_CAT" +
				", null as TABLE_SCHEM" +
				", null as TABLE_NAME" +
				", null as COLUMN_NAME" +
				", null as GRANTOR" +
				", null as GRANTEE" +
				", null as PRIVILEGE" +
				", null as IS_GRANTABLE" +
				" limit 0");
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		checkPatternMatchParameter(
				tableNamePattern, "tableNamePattern", null);

		return executeQuery("select" +
				" null as TABLE_CAT" +
				", null as TABLE_SCHEM" +
				", null as TABLE_NAME" +
				", null as GRANTOR" +
				", null as GRANTEE" +
				", null as PRIVILEGE" +
				", null as IS_GRANTABLE" +
				" limit 0");
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) throws SQLException {
		checkExactMatchParameter(table, "table", null);

		return executeQuery("select" +
				" null as SCOPE" +
				", null as COLUMN_NAME" +
				", null as DATA_TYPE" +
				", null as TYPE_NAME" +
				", null as COLUMN_SIZE" +
				", null as BUFFER_LENGTH" +
				", null as DECIMAL_DIGITS" +
				", null as PSEUDO_COLUMN" +
				" limit 0");
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema,
			String table) throws SQLException {
		checkExactMatchParameter(table, "table", null);

		return executeQuery("select" +
				" null as SCOPE" +
				", null as COLUMN_NAME" +
				", null as DATA_TYPE" +
				", null as TYPE_NAME" +
				", null as COLUMN_SIZE" +
				", null as BUFFER_LENGTH" +
				", null as DECIMAL_DIGITS" +
				", null as PSEUDO_COLUMN" +
				" limit 0");
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		checkExactMatchParameter(table, "table", null);

		String where = "";
		if (table != null) {
			where =
					" where upper(TABLE_NAME) = upper('" + escape(table) + "')";
		}
		
		String sql =
				"select" +
				" TABLE_CAT" +
				", TABLE_SCHEM" +
				", TABLE_NAME" +
				", COLUMN_NAME" +
				", KEY_SEQ" +
				", PK_NAME" +
				" from [#_driver_primary_keys]" +
				where +
				" order by COLUMN_NAME";

		return executeMetaQuery(sql);
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		checkExactMatchParameter(table, "table", null);

		return executeQuery("select" +
				" null as PKTABLE_CAT" +
				", null as PKTABLE_SCHEM" +
				", null as PKTABLE_NAME" +
				", null as PKCOLUMN_NAME" +
				", null as FKTABLE_CAT" +
				", null as FKTABLE_SCHEM" +
				", null as FKTABLE_NAME" +
				", null as FKCOLUMN_NAME" +
				", null as KEY_SEQ" +
				", null as UPDATE_RULE" +
				", null as DELETE_RULE" +
				", null as FK_NAME" +
				", null as PK_NAME" +
				", null as DEFERRABILITY" +
				" limit 0");
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table)
			throws SQLException {
		checkExactMatchParameter(table, "table", null);

		return executeQuery("select" +
				" null as PKTABLE_CAT" +
				", null as PKTABLE_SCHEM" +
				", null as PKTABLE_NAME" +
				", null as PKCOLUMN_NAME" +
				", null as FKTABLE_CAT" +
				", null as FKTABLE_SCHEM" +
				", null as FKTABLE_NAME" +
				", null as FKCOLUMN_NAME" +
				", null as KEY_SEQ" +
				", null as UPDATE_RULE" +
				", null as DELETE_RULE" +
				", null as FK_NAME" +
				", null as PK_NAME" +
				", null as DEFERRABILITY" +
				" limit 0");
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog,
			String parentSchema, String parentTable, String foreignCatalog,
			String foreignSchema, String foreignTable) throws SQLException {
		checkExactMatchParameter(parentTable, "parentTable", null);
		checkExactMatchParameter(foreignTable, "foreignTable", null);

		return executeQuery("select" +
				" null as PKTABLE_CAT" +
				", null as PKTABLE_SCHEM" +
				", null as PKTABLE_NAME" +
				", null as PKCOLUMN_NAME" +
				", null as FKTABLE_CAT" +
				", null as FKTABLE_SCHEM" +
				", null as FKTABLE_NAME" +
				", null as FKCOLUMN_NAME" +
				", null as KEY_SEQ" +
				", null as UPDATE_RULE" +
				", null as DELETE_RULE" +
				", null as FK_NAME" +
				", null as PK_NAME" +
				", null as DEFERRABILITY" +
				" limit 0");
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		
		
		return executeMetaQuery("select" +
				" TYPE_NAME" +
				", DATA_TYPE" +
				", PRECISION" +
				", LITERAL_PREFIX" +
				", LITERAL_SUFFIX" +
				", CREATE_PARAMS" +
				", NULLABLE" +
				", CASE_SENSITIVE" +
				", SEARCHABLE" +
				", UNSIGNED_ATTRIBUTE" +
				", FIXED_PREC_SCALE" +
				", AUTO_INCREMENT" +
				", LOCAL_TYPE_NAME" +
				", MINIMUM_SCALE" +
				", MAXIMUM_SCALE" +
				", SQL_DATA_TYPE" +
				", SQL_DATETIME_SUB" +
				", NUM_PREC_RADIX" +
				" from [#_driver_type_info]" +
				" order by DATA_TYPE");

	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		checkExactMatchParameter(table, "table", null);

		if (table == null) {
			table = "";
		}
		String where = " where upper(TABLE_NAME) = upper('" + escape(table) + "')";

		if (unique) {
			where += " and NON_UNIQUE = false";
		}
		
		String sql =
				"select" +
				" TABLE_CAT" +
				", TABLE_SCHEM" +
				", TABLE_NAME" +
				", NON_UNIQUE" +
				", INDEX_QUALIFIER" +
				", INDEX_NAME" +
				", TYPE" +
				", ORDINAL_POSITION" +
				", COLUMN_NAME" +
				", ASC_OR_DESC" +
				", CARDINALITY" +
				", PAGES" +
				", FILTER_CONDITION" +
				" from [#_driver_index_info]" +
				where +
				" order by NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION";

		return executeMetaQuery(sql);
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY &&
				concurrency == ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern,
			String typeNamePattern, int[] types) throws SQLException {
		checkPatternMatchParameter(
				typeNamePattern, "typeNamePattern", null);

		return executeQuery("select" +
				" null as TYPE_CAT" +
				", null as TYPE_SCHEM" +
				", null as TYPE_NAME" +
				", null as CLASS_NAME" +
				", null as DATA_TYPE" +
				", null as REMARKS" +
				", null as BASE_TYPE" +
				" limit 0");
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern,
			String typeNamePattern) throws SQLException {
		checkPatternMatchParameter(
				typeNamePattern, "typeNamePattern", null);

		return executeQuery("select" +
				" null as TYPE_CAT" +
				", null as TYPE_SCHEM" +
				", null as TYPE_NAME" +
				", null as SUPERTYPE_CAT" +
				", null as SUPERTYPE_SCHEM" +
				", null as SUPERTYPE_NAME" +
				" limit 0");
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		checkPatternMatchParameter(
				tableNamePattern, "tableNamePattern", null);

		return executeQuery("select" +
				" null as TABLE_CAT" +
				", null as TABLE_SCHEM" +
				", null as TABLE_NAME" +
				", null as SUPERTABLE_NAME" +
				" limit 0");
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern,
			String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		checkPatternMatchParameter(
				typeNamePattern, "typeNamePattern", null);
		checkPatternMatchParameter(
				attributeNamePattern, "attributeNamePattern", null);

		return executeQuery("select" +
				" null as TYPE_CAT" +
				", null as TYPE_SCHEM" +
				", null as TYPE_NAME" +
				", null as ATTR_NAME" +
				", null as DATA_TYPE" +
				", null as ATTR_TYPE_NAME" +
				", null as ATTR_SIZE" +
				", null as DECIMAL_DIGITS" +
				", null as NUM_PREC_RADIX" +
				", null as NULLABLE" +
				", null as REMARKS" +
				", null as ATTR_DEF" +
				", null as SQL_DATA_TYPE" +
				", null as SQL_DATETIME_SUB" +
				", null as CHAR_OCTET_LENGTH" +
				", null as ORDINAL_POSITION" +
				", null as IS_NULLABLE" +
				", null as SCOPE_CATALOG" +
				", null as SCOPE_SCHEMA" +
				", null as SCOPE_TABLE" +
				", null as SOURCE_DATA_TYPE" +
				" limit 0");
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return getClusterInfo().databaseMajorVersion;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return getClusterInfo().databaseMinorVersion;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return JDBC_MAJOR_VERSION;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return JDBC_MINOR_VERSION;
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return sqlStateSQL99;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern)
			throws SQLException {
		return executeQuery("select" +
				" null as TABLE_SCHEM" +
				", null as TABLE_CATALOG" +
				" limit 0");
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		return executeQuery("select" +
				" null as NAME" +
				", null as MAX_LEN" +
				", null as DEFAULT_VALUE" +
				", null as DESCRIPTION" +
				" limit 0");
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern,
			String functionNamePattern) throws SQLException {
		checkPatternMatchParameter(
				functionNamePattern, "functionNamePattern", null);

		return executeQuery("select" +
				" null as FUNCTION_CAT" +
				", null as FUNCTION_SCHEM" +
				", null as FUNCTION_NAME" +
				", null as REMARKS" +
				", null as FUNCTION_TYPE" +
				", null as SPECIFIC_NAME" +
				" limit 0");
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern,
			String functionNamePattern, String columnNamePattern)
			throws SQLException {
		checkPatternMatchParameter(
				functionNamePattern, "functionNamePattern", null);
		checkPatternMatchParameter(
				columnNamePattern, "columnNamePattern", null);

		return executeQuery("select" +
				" null as FUNCTION_CAT" +
				", null as FUNCTION_SCHEM" +
				", null as FUNCTION_NAME" +
				", null as COLUMN_NAME" +
				", null as COLUMN_TYPE" +
				", null as DATA_TYPE" +
				", null as TYPE_NAME" +
				", null as PRECISION" +
				", null as LENGTH" +
				", null as SCALE" +
				", null as RADIX" +
				", null as NULLABLE" +
				", null as REMARKS" +
				", null as CHAR_OCTET_LENGTH" +
				", null as ORDINAL_POSITION" +
				", null as IS_NULLABLE" +
				", null as SPECIFIC_NAME" +
				" limit 0");
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		checkPatternMatchParameter(
				tableNamePattern, "tableNamePattern", null);
		checkPatternMatchParameter(
				columnNamePattern, "columnNamePattern", null);

		return executeQuery("select" +
				" null as TABLE_CAT" +
				", null as TABLE_SCHEM" +
				", null as TABLE_NAME" +
				", null as COLUMN_NAME" +
				", null as DATA_TYPE" +
				", null as COLUMN_SIZE" +
				", null as DECIMAL_DIGITS" +
				", null as NUM_PREC_RADIX" +
				", null as COLUMN_USAGE" +
				", null as REMARKS" +
				", null as CHAR_OCTET_LENGTH" +
				", null as IS_NULLABLE" +
				" limit 0");
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		throw SQLErrorUtils.errorNotSupportedFeature();
	}

	private ResultSet executeQuery(String sql) throws SQLException {
		final SQLStatement statement = new SQLStatement(connection);
		ResultSet rs = null;
		try {
			rs = statement.executeQuery(sql);
			((SQLResultSet) rs).setStatementOwned(true);

			return rs;
		}
		finally {
			if (rs == null) {
				statement.close();
			}
		}
	}

	private ResultSet executeMetaQuery(String sql) throws SQLException {
		final SQLStatement statement = new SQLStatement(connection);
		ResultSet rs = null;
		try {
			statement.executePragma(sql);

			rs = statement.getResultSet();
			if (rs == null) {
				throw SQLErrorUtils.error(
						SQLErrorUtils.ILLEGAL_STATE,
						"Protocol error occurred on querying meta data", null);
			}

			((SQLResultSet) rs).setStatementOwned(true);
			return rs;
		}
		finally {
			if (rs == null) {
				statement.close();
			}
		}
	}

	private static String escape(String value) {
		return value.replaceAll("'", "''");
	}

	private boolean matchCatalogAndSchema(
			String catalog, String schemaPattern) {
		if (connection.isCatalogAndSchemaIgnorable()) {
			return true;
		}
		return ((catalog == null || catalog.isEmpty()) &&
				(schemaPattern == null || schemaPattern.isEmpty()));
	}

	private static void checkExactMatchParameter(
			Object parameter, String name, NullPointerException cause)
			throws SQLException {
		if (exactMatchParameterStrict) {
			SQLErrorUtils.checkNullParameter(parameter, name, cause);
		}
	}

	private static void checkPatternMatchParameter(
			Object parameter, String name, NullPointerException cause)
			throws SQLException {
		if (patternMatchParameterStrict) {
			SQLErrorUtils.checkNullParameter(parameter, name, cause);
		}
	}

	private ClusterInfo getClusterInfo() throws SQLException {
		if (clusterInfo == null) {
			final ResultSet rs = executeMetaQuery(ClusterInfo.getMetaQuery());
			try {
				clusterInfo = new ClusterInfo(rs);
			}
			finally {
				rs.close();
			}
		}

		return clusterInfo;
	}

	private static class ClusterInfo {

		private static final String DATABASE_MAJOR_VERSION_COLUMN =
				"DATABASE_MAJOR_VERSION";

		private static final String DATABASE_MINOR_VERSION_COLUMN =
				"DATABASE_MINOR_VERSION";

		private static final String EXTRA_NAME_CHARACTERS_COLUMN =
				"EXTRA_NAME_CHARACTERS";

		final int databaseMajorVersion;

		final int databaseMinorVersion;

		final String extraNameCharacters;

		ClusterInfo(ResultSet rs) throws SQLException {
			rs.next();
			databaseMajorVersion = rs.getInt(DATABASE_MAJOR_VERSION_COLUMN);
			databaseMinorVersion = rs.getInt(DATABASE_MINOR_VERSION_COLUMN);
			extraNameCharacters = rs.getString(EXTRA_NAME_CHARACTERS_COLUMN);
		}

		static String getMetaQuery() {
			return
					"select" +
					" " + DATABASE_MAJOR_VERSION_COLUMN +
					", " + DATABASE_MINOR_VERSION_COLUMN +
					", " + EXTRA_NAME_CHARACTERS_COLUMN +
					" from [#_driver_cluster_info]";
		}

	}

	static class DriverInfo {

		private static final String DRIVER_MAJOR_VERSION_KEY =
				"driverMajorVersion";

		private static final String DRIVER_MINOR_VERSION_KEY =
				"driverMinorVersion";

		private static final String DRIVER_NAME_KEY =
				"driverName";

		private static final String DATABASE_PRODUCT_NAME_KEY =
				"databaseProductName";

		private static DriverInfo instance;

		final int driverMajorVersion;

		final int driverMinorVersion;

		final String driverName;

		final String databaseProductName;

		DriverInfo() {
			final ResourceBundle bundle =
					ResourceBundle.getBundle(SQLDriver.class.getName());
			driverMajorVersion = Integer.parseInt(
					bundle.getString(DRIVER_MAJOR_VERSION_KEY));
			driverMinorVersion = Integer.parseInt(
					bundle.getString(DRIVER_MINOR_VERSION_KEY));
			driverName = bundle.getString(DRIVER_NAME_KEY);
			databaseProductName = bundle.getString(DATABASE_PRODUCT_NAME_KEY);
		}

		static DriverInfo getInstance() {
			DriverInfo info = instance;
			if (info == null) {
				info = new DriverInfo();
				instance = info;
			}
			return info;
		}

	}

}
