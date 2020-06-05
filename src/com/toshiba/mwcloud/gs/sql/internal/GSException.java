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
import java.util.Collections;
import java.util.Map;

/**
 * <div lang="ja">
 * GridDB機能の処理中に発生した例外状態を示します。
 * </div><div lang="en">
 * Represents the exceptions occurring during a process of a GridDB function.
 * </div>
 */
class GSException extends IOException {

	private static final long serialVersionUID = -7261622831192521426L;

	private static final Map<String, String> EMPTY_PARAMETERS =
			Collections.emptyMap();

	private final int errorCode;

	private final int subCode;

	private final String errorName;

	private final String description;

	private final Map<String, String> parameters;

	static {
		GSErrorCode.setExceptionAccessor(new GSErrorCode.ExceptionAccessor() {
			@Override
			public String getDescription(GSException e) {
				return e.description;
			}
		});
	}

	/**
	 * <div lang="ja">
	 * 詳細メッセージを持たない例外を構築します。
	 *
	 * @see Exception#Exception()
	 * </div><div lang="en">
	 * Build a non-descriptive exception.
	 *
	 * @see Exception#Exception()
	 * </div>
	 */
	public GSException() {
		this(0, 0, null, null, EMPTY_PARAMETERS, null);
	}

	/**
	 * <div lang="ja">
	 * 詳細メッセージおよび原因を指定して、例外を構築します。
	 *
	 * @param message 詳細メッセージまたは{@code null}
	 * @param cause 原因または{@code null}
	 *
	 * @see Exception#Exception(String, Throwable)
	 * </div><div lang="en">
	 * Specify the description and cause, then build an exception.
	 *
	 * @param message Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see Exception#Exception(String, Throwable)
	 * </div>
	 */
	public GSException(String message, Throwable cause) {
		this(0, 0, null, message, EMPTY_PARAMETERS, cause);
	}

	/**
	 * <div lang="ja">
	 * 詳細メッセージを指定して、例外を構築します。
	 *
	 * @param message 詳細メッセージまたは{@code null}
	 *
	 * @see Exception#Exception(String)
	 * </div><div lang="en">
	 * Specify the description, then build an exception.
	 *
	 * @param message Description or {@code null}
	 *
	 * @see Exception#Exception(String)
	 * </div>
	 */
	public GSException(String message) {
		this(0, 0, null, message, EMPTY_PARAMETERS, null);
	}

	/**
	 * <div lang="ja">
	 * 原因を指定して、例外を構築します。
	 *
	 * @param cause 原因または{@code null}
	 *
	 * @see Exception#Exception(Throwable)
	 * </div><div lang="en">
	 * Specify the error, then build an exception.
	 *
	 * @param cause Cause or {@code null}
	 *
	 * @see Exception#Exception(Throwable)
	 * </div>
	 */
	public GSException(Throwable cause) {
		this(0, 0, null, null, resolveParameters(null, cause), cause);
	}

	/**
	 * <div lang="ja">
	 * エラー番号、詳細メッセージ、および原因を指定して、例外を構築します。
	 *
	 * @param errorCode エラー番号
	 * @param description 詳細メッセージまたは{@code null}
	 * @param cause 原因または{@code null}
	 *
	 * @see Exception#Exception(String, Throwable)
	 * </div><div lang="en">
	 * Specify the error number, description and cause, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param description Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see Exception#Exception(String, Throwable)
	 * </div>
	 */
	public GSException(int errorCode, String description, Throwable cause) {
		this(errorCode, 0, null, description, EMPTY_PARAMETERS, cause);
	}

	/**
	 * <div lang="ja">
	 * エラー番号および詳細メッセージを指定して、例外を構築します。
	 *
	 * @param errorCode エラー番号
	 * @param description 詳細メッセージまたは{@code null}
	 *
	 * @see Exception#Exception(String)
	 * </div><div lang="en">
	 * Specify the error number and description, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param description Description or {@code null}
	 *
	 * @see Exception#Exception(String)
	 * </div>
	 */
	public GSException(int errorCode, String description) {
		this(errorCode, 0, null, description, EMPTY_PARAMETERS, null);
	}

	/**
	 * <div lang="ja">
	 * エラー番号および原因を指定して、例外を構築します。
	 *
	 * @param errorCode エラー番号
	 * @param cause 原因または{@code null}
	 *
	 * @see Exception#Exception(Throwable)
	 * </div><div lang="en">
	 * Specify the error number and description, then build an exception.
	 *
	 * @param errorCode Error number
	 * @param cause Cause or {@code null}
	 *
	 * @see Exception#Exception(Throwable)
	 * </div>
	 */
	public GSException(int errorCode, Throwable cause) {
		this(errorCode, 0, null, null, EMPTY_PARAMETERS, cause);
	}

	/**
	 * <div lang="ja">
	 * エラー番号、エラー名、詳細メッセージ、および原因を指定して、例外を
	 * 構築します。
	 *
	 * @param errorCode エラー番号
	 * @param errorName エラー名または{@code null}
	 * @param description 詳細メッセージまたは{@code null}
	 * @param cause 原因または{@code null}
	 *
	 * @see Exception#Exception(String, Throwable)
	 * </div><div lang="en">
	 * Specify the error number, error name, description, and cause,
	 * then build an exception.
	 *
	 * @param errorCode Error number
	 * @param errorName Error name or {@code null}
	 * @param description Description or {@code null}
	 * @param cause Cause or {@code null}
	 *
	 * @see Exception#Exception(String, Throwable)
	 * </div>
	 */
	public GSException(int errorCode,
			String errorName, String description, Throwable cause) {
		this(
				errorCode, 0, errorName, description,
				EMPTY_PARAMETERS, cause);
	}

	GSException(int errorCode, int subCode,
			String errorName, String description, Throwable cause) {
		this(
				errorCode, subCode, errorName, description,
				EMPTY_PARAMETERS, cause);
	}

	GSException(
			int errorCode, int subCode, String errorName, String description,
			Map<String, String> parameters, Throwable cause) {
		super(cause);
		this.errorCode = resolveErrorCode(errorCode, cause);
		this.subCode = resolveSubCode(subCode, cause);
		this.errorName = resolveErrorName(errorCode, errorName, cause);
		this.description = resolveDescription(description, cause);
		this.parameters = resolveParameters(parameters, null);
	}

	private static int resolveErrorCode(int errorCode, Throwable cause) {
		if (errorCode != 0) {
			return errorCode;
		}

		if (cause instanceof GSException) {
			return ((GSException) cause).getErrorCode();
		}

		return 0;
	}

	private static int resolveSubCode(int subCode, Throwable cause) {
		if (subCode != 0) {
			return subCode;
		}

		if (cause instanceof GSException) {
			return ((GSException) cause).getErrorCode();
		}

		return 0;
	}

	private static String resolveErrorName(
			int errorCode, String errorName, Throwable cause) {
		if (errorCode != 0 && errorName != null) {
			return errorName;
		}

		if (cause instanceof GSException) {
			final GSException gsCause = ((GSException) cause);
			if (errorCode == 0 || errorCode == gsCause.getErrorCode()) {
				return gsCause.errorName;
			}
		}

		return GSErrorCode.getName(errorCode);
	}

	private static String resolveDescription(
			String description, Throwable cause) {
		if (description != null) {
			return description;
		}

		if (cause instanceof GSException) {
			return ((GSException) cause).description;
		}

		if (cause != null) {
			return cause.getMessage();
		}

		return null;
	}

	private static Map<String, String> resolveParameters(
			Map<String, String> parameters, Throwable cause) {
		do {
			if (parameters != null) {
				if (parameters.isEmpty()) {
					break;
				}
				return Collections.unmodifiableMap(
						GSErrorCode.newParameters(parameters));
			}

			if (cause instanceof GSException) {
				return ((GSException) cause).parameters;
			}
		}
		while (false);

		return EMPTY_PARAMETERS;
	}

	/**
	 * <div lang="ja">
	 * エラー番号を取得します。
	 *
	 * <p>対応する番号が存在しない場合は{@code 0}を返します。</p>
	 * </div><div lang="en">
	 * Returns an error number.
	 *
	 * <p>It returns {@code 0} if no corresponding number is found.</p>
	 * </div>
	 */
	public int getErrorCode() {
		return errorCode;
	}

	public String getErrorName() {
		return errorName;
	}

	public int getSubCode() {
		return subCode;
	}

	public Map<String, String> getParameters() {
		return parameters;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getMessage() {
		if (errorCode == 0 && description == null) {
			return super.getMessage();
		}

		if (errorCode == 0) {
			return description;
		}

		final StringBuilder builder = new StringBuilder();

		if (errorName == null) {
			builder.append("[Code:").append(errorCode).append("]");
		}
		else {
			builder.append("[").append(errorCode).append(":");
			builder.append(errorName).append("]");
		}

		if (description != null) {
			builder.append(" ").append(description);
		}

		return builder.toString();
	}

}
