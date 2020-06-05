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


class LoggingUtils {

	private static final BaseGridStoreLogger EMPTY_LOGGER =
			new BaseGridStoreLogger() {
				public void debug(String format, Object... arguments) {}
				public void info(String format, Object... arguments) {}
				public void warn(String format, Object... arguments) {}
				public boolean isDebugEnabled() { return false; }
				public boolean isInfoEnabled() { return false; }
				public boolean isWarnEnabled() { return false; }
	};

	public static abstract class BaseGridStoreLogger {
		public abstract void debug(String key, Object... args);
		public abstract void info(String key, Object... args);
		public abstract void warn(String key, Object... args);
		public abstract boolean isDebugEnabled();
		public abstract boolean isInfoEnabled();
		public abstract boolean isWarnEnabled();
	}

	public static BaseGridStoreLogger getLogger(String subName) {
		return EMPTY_LOGGER;
	}

	public static String getFormatString(String key) {
		return null;
	}

}
