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

import java.lang.reflect.Field;
import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;

class SQLErrorUtils {

	public static final int NOT_SUPPORTED = 147001;

	public static final int OPTIONAL_FEATURE_NOT_SUPPORTED = 147002;

	public static final int EMPTY_PARAMETER = 147003;

	public static final int ALREADY_CLOSED = 147004;

	public static final int COLUMN_INDEX_OUT_OF_RANGE = 147005;

	public static final int VALUE_TYPE_CONVERSION_FAILED = 147006;

	public static final int UNWRAPPING_NOT_SUPPORTED = 147007;

	public static final int ILLEGAL_PARAMETER = 147008;

	public static final int UNSUPPORTED_PARAMETER_VALUE = 147009;

	public static final int ILLEGAL_STATE = 147010;

	public static final int INVALID_CURSOR_POSITION = 147011;

	public static final int STATEMENT_CATEGORY_UNMATCHED = 147012;

	public static final int MESSAGE_CORRUPTED = 147013;

	private static final Map<Integer, State> STATE_MAP =
			new HashMap<Integer, State>();

	private static final Map<Integer, String> NAME_MAP = makeNameMap();

	static {
		STATE_MAP.put(NOT_SUPPORTED, State.FEATURE_NOT_SUPPORTED);
		STATE_MAP.put(OPTIONAL_FEATURE_NOT_SUPPORTED, State.FEATURE_NOT_SUPPORTED);
		STATE_MAP.put(EMPTY_PARAMETER, State.DATA_EXCEPTION);
		STATE_MAP.put(ALREADY_CLOSED, State.INVALID_SYSTEM_STATE);
		STATE_MAP.put(COLUMN_INDEX_OUT_OF_RANGE, State.DATA_EXCEPTION);
		STATE_MAP.put(VALUE_TYPE_CONVERSION_FAILED, State.DATA_EXCEPTION);
		STATE_MAP.put(UNWRAPPING_NOT_SUPPORTED, State.FEATURE_NOT_SUPPORTED);
		STATE_MAP.put(ILLEGAL_PARAMETER, State.FEATURE_NOT_SUPPORTED);
		STATE_MAP.put(UNSUPPORTED_PARAMETER_VALUE, State.FEATURE_NOT_SUPPORTED);
		STATE_MAP.put(ILLEGAL_STATE, State.INVALID_SYSTEM_STATE);
		STATE_MAP.put(INVALID_CURSOR_POSITION, State.INVALID_CURSOR_STATE);
		STATE_MAP.put(STATEMENT_CATEGORY_UNMATCHED, State.INVALID_SQL_STATEMENT_NAME);
		STATE_MAP.put(MESSAGE_CORRUPTED, State.CONNECTION_EXCEPTION);

		if (!STATE_MAP.keySet().equals(NAME_MAP.keySet())) {
			throw new Error();
		}

		STATE_MAP.put(GSErrorCode.INTERNAL_ERROR, State.SYSTEM_ERROR);
		STATE_MAP.put(GSErrorCode.EMPTY_PARAMETER, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.ILLEGAL_PARAMETER, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNSUPPORTED_OPERATION, State.FEATURE_NOT_SUPPORTED);
		STATE_MAP.put(GSErrorCode.SIZE_VALUE_OUT_OF_RANGE, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.ILLEGAL_PROPERTY_ENTRY, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.ILLEGAL_VALUE_FORMAT, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.ILLEGAL_SYMBOL_CHARACTER, State.SQL_SYNTAX_ERROR);
		STATE_MAP.put(GSErrorCode.UNKNOWN_COLUMN_NAME, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNSUPPORTED_KEY_TYPE, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNSUPPORTED_FIELD_TYPE, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNSUPPORTED_ROW_MAPPING, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNKNOWN_ELEMENT_TYPE_OPTION, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNKNOWN_GEOMETRY_OPERATOR, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNKNOWN_AGGREGATION, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNKNOWN_TIME_OPERATOR, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNKNOWN_INDEX_FLAG, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNKNOWN_FETCH_OPTION, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNKNOWN_TIME_UNIT, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.UNSUPPORTED_DEFAULT_INDEX, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.BINDING_ENTRY_NOT_FOUND, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.MULTIPLE_KEYS_FOUND, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.COLUMN_NAME_CONFLICTED, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.ILLEGAL_SCHEMA, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.KEY_NOT_FOUND, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.KEY_NOT_ACCEPTED, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.EMPTY_ROW_FIELD, State.DATA_EXCEPTION);
		STATE_MAP.put(GSErrorCode.BAD_STATEMENT, State.CONNECTION_EXCEPTION);
		STATE_MAP.put(GSErrorCode.BAD_CONNECTION, State.CONNECTION_EXCEPTION);
		STATE_MAP.put(GSErrorCode.CONNECTION_TIMEOUT, State.CONNECTION_EXCEPTION);
		STATE_MAP.put(GSErrorCode.WRONG_NODE, State.INVALID_SYSTEM_STATE);
		STATE_MAP.put(GSErrorCode.MESSAGE_CORRUPTED, State.CONNECTION_EXCEPTION);
		STATE_MAP.put(GSErrorCode.PARTITION_NOT_AVAILABLE, State.CONNECTION_EXCEPTION);
		STATE_MAP.put(GSErrorCode.ILLEGAL_PARTITION_COUNT, State.INVALID_SYSTEM_STATE);
		STATE_MAP.put(GSErrorCode.CONTAINER_NOT_OPENED, State.INVALID_SYSTEM_STATE);
		STATE_MAP.put(GSErrorCode.ILLEGAL_COMMIT_MODE, State.INVALID_TRANSACTION_STATE);
		STATE_MAP.put(GSErrorCode.TRANSACTION_CLOSED, State.INVALID_TRANSACTION_STATE);
		STATE_MAP.put(GSErrorCode.NO_SUCH_ELEMENT, State.INVALID_CURSOR_STATE);
		STATE_MAP.put(GSErrorCode.CONTAINER_CLOSED, State.INVALID_SYSTEM_STATE);
		STATE_MAP.put(GSErrorCode.NOT_LOCKED, State.INVALID_TRANSACTION_STATE);
		STATE_MAP.put(GSErrorCode.RESOURCE_CLOSED, State.INVALID_SYSTEM_STATE);
		STATE_MAP.put(GSErrorCode.ALLOCATION_FAILED, State.INSUFFICIENT_RESOURCES);
		STATE_MAP.put(GSErrorCode.RECOVERABLE_CONNECTION_PROBLEM, State.INVALID_TRANSACTION_STATE);
		STATE_MAP.put(GSErrorCode.RECOVERABLE_ROW_SET_LOST, State.INVALID_TRANSACTION_STATE);
	}

	private enum State {

		SUCCESSFUL_COMPLETION("00", 0x00),
		WARNING("01", 0x01),
		NO_DATA("02", 0x02),
		DYNAMIC_SQL_ERROR("07", 0x07),
		CONNECTION_EXCEPTION("08", 0x08),
		TRIGGERED_ACTION_EXCEPTION("09", 0x09),
		FEATURE_NOT_SUPPORTED("0A", 0x0a),
		CARDINALITY_VIOLATION("0P", 0x21),
		DATA_EXCEPTION("22", 0x22),
		INTEGRITY_CONSTRAINT_VIOLATION("23", 0x23),
		INVALID_CURSOR_STATE("24", 0x24),
		INVALID_TRANSACTION_STATE("25", 0x25),
		INVALID_SQL_STATEMENT_NAME("26", 0x26),
		INVALID_AUTHORIZATION_SPECIFICATION("28", 0x28),
		DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST("2B", 0x2b),
		INVALID_CONNECTION_NAME("2E", 0x2e),
		SQL_ROUTINE_EXCEPTION("2F", 0x2f),
		INVALID_SQL_DESCRIPTOR_NAME("33", 0x33),
		INVALID_CURSOR_NAME("34", 0x34),
		SQL_SYNTAX_ERROR("37", 0x37),
		AMBIGUOUS_CURSOR_NAME("3C", 0x3c),
		INVALID_CATALOG_NAME("3D", 0x3d),
		INVALID_SCHEMA_NAME("3F", 0x3f),
		TRANSACTION_ROLLBACK("40", 0x40),
		ACCESS_RULE_VIOLATION("42", 0x42),
		WITH_CHECK_OPTION_VIOLATION("44", 0x44),
		INVALID_SYSTEM_STATE("71", 0x71),
		INSUFFICIENT_RESOURCES("73", 0x72),
		OUT_OF_MEMORY("73", 0x73),
		IO_ERROR("74", 0x74),
		SYSTEM_ERROR("76", 0x76),
		SQL_SYSTEM_ERROR("77", 0x77),
		UNKNOWN("76", 0xff)
		;

		private static final State[] VALUE_MAP = new State[256];

		static {
			for (State value : values()) {
				VALUE_MAP[value.codeNumber] = value;
			}
		}

		private final String codeString;

		private final int codeNumber;

		State(String codeString, int codeNumber) {
			this.codeString = codeString;
			this.codeNumber = codeNumber;
		}
	}

	private static Map<Integer, String> makeNameMap() {
		final Map<Integer, String> map = new HashMap<Integer, String>();
		for (Field field : SQLErrorUtils.class.getFields()) {
			if (field.getType() != Integer.TYPE) {
				continue;
			}
			try {
				map.put(field.getInt(null), "JDBC_" + field.getName());
			}
			catch (IllegalAccessException e) {
				throw new Error(e);
			}
		}
		return map;
	}

	public static SQLFeatureNotSupportedException errorNotSupportedFeature() {
		return errorNotSupportedFeature(
				OPTIONAL_FEATURE_NOT_SUPPORTED,
				"Optional feature not supported", null);
	}

	public static SQLException errorNotSupported() {
		
		
		return errorNotSupportedFeature(
				NOT_SUPPORTED, "Currently not supported", null);
	}

	public static SQLException errorUnwrapping() {
		return error(UNWRAPPING_NOT_SUPPORTED,
				"Unwrapping interface not supported", null);
	}

	public static SQLException errorAlreadyClosed() {
		return error(ALREADY_CLOSED, "Already closed", null);
	}

	public static SQLException checkNullParameter(
			Object parameter, String name, NullPointerException cause)
			throws SQLException {
		if (parameter == null) {
			throw error(
					EMPTY_PARAMETER,
					"The parameter \"" + name + "\" must not be null" +
							(cause == null || cause.getMessage() == null ?
									"" : " (reason=" + cause.getMessage() + ")"),
					cause);
		}

		if (cause != null) {
			throw cause;
		}

		return null;
	}

	public static SQLFeatureNotSupportedException errorNotSupportedFeature(
			int errorCode, String description, Throwable cause) {
		return new SQLFeatureNotSupportedException(
				makeErrorMessage(errorCode, description, cause),
				resolveState(errorCode, cause),
				resolveErrorCode(errorCode, cause),
				cause);
	}

	public static SQLClientInfoException errorClientInfo(
			int errorCode, String description,
			Map<String, ClientInfoStatus> failedProperties,
			Throwable cause) {
		return new SQLClientInfoException(
				makeErrorMessage(errorCode, description, cause),
				resolveState(errorCode, cause),
				resolveErrorCode(errorCode, cause),
				failedProperties,
				cause);
	}

	public static SQLException error(
			int errorCode, String description, Throwable cause) {
		return new SQLException(
				makeErrorMessage(errorCode, description, cause),
				resolveState(errorCode, cause),
				resolveErrorCode(errorCode, cause),
				cause);
	}

	private static int resolveErrorCode(int errorCode, Throwable cause) {
		if (errorCode != 0) {
			return errorCode;
		}

		if (cause instanceof GSException) {
			return ((GSException) cause).getErrorCode();
		}
		else if (cause instanceof SQLException) {
			return ((SQLException) cause).getErrorCode();
		}

		return 0;
	}

	private static String resolveErrorName(int errorCode, Throwable cause) {
		if (errorCode != 0) {
			final String name = NAME_MAP.get(errorCode);
			if (name != null) {
				return name;
			}
		}

		if (cause instanceof GSException) {
			return ((GSException) cause).getErrorName();
		}

		if (cause == null) {
			return null;
		}

		return resolveErrorName(0, cause.getCause());
	}

	private static String resolveState(int errorCode, Throwable cause) {
		final int targetCode;
		if (cause instanceof GSException) {
			final int subCode = ((GSException) cause).getSubCode();
			if (0 <= subCode && subCode < State.VALUE_MAP.length) {
				final State state = State.VALUE_MAP[subCode];
				if (state != null) {
					return state.codeString;
				}
			}

			targetCode = (errorCode == 0 ?
					((GSException) cause).getErrorCode() : errorCode);
		}
		else {
			targetCode = errorCode;
		}

		final State state = STATE_MAP.get(targetCode);
		return (state == null ? null : state.codeString);
	}

	private static String makeErrorMessage(
			int errorCode, String description, Throwable cause) {
		if (errorCode == 0 && description == null) {
			if (cause == null) {
				return null;
			}
			return cause.getMessage();
		}

		final StringBuilder builder = new StringBuilder();
		final int resolvedCode = resolveErrorCode(errorCode, cause);
		final String errorName = resolveErrorName(resolvedCode, cause);

		if (errorName == null) {
			builder.append("[Code:").append(resolvedCode).append("]");
		}
		else {
			builder.append("[").append(resolvedCode).append(":");
			builder.append(errorName).append("]");
		}

		if (description != null) {
			builder.append(" ").append(description);
		}

		return builder.toString();
	}

}
