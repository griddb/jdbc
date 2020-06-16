/*
   Copyright (c) 2019 TOSHIBA Digital Solutions Corporation

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
package com.toshiba.mwcloud.gs.sql.internal.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogProxy implements InvocationHandler {
	private static final Logger logger = LoggerFactory.getLogger("com.toshiba.mwcloud.gs.sql.Logger");

	private static final String METHOD_NAME_TOSTRING = "toString";

	private static final String PID = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

	Object target;

	public LogProxy(Object target) {
		this.target = target;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		boolean isOutputTrace = isOutputTraceMethod(method);
		if (isOutputTrace) {
			outputStartTrace(method, args);
		}

		Object ret = null;
		try {
			ret = method.invoke(target, args);
		} catch (InvocationTargetException e) {
			logger.trace("An exception was thrown.", e);
			Throwable cause = e.getCause();
			if (cause != null) {
				throw cause;
			} else {
				throw e;
			}
		} finally {
			if (isOutputTrace) {
				outputEndTrace(method, ret);
			}
		}
		return ret;
	}

	private void outputStartTrace(Method method, Object[] args) {
		logger.trace("[Trace Start] " + "pid=" + PID + " " + getShortClassName(target) + "::" + method.getName() + "()");
		if (args != null && args.length != 0) {
			for (int i = 0; i < args.length; i++) {
				outputArg(args[i]);
			}
		}
	}

	private void outputEndTrace(Method method, Object ret) {
		Class<?> returnType = method.getReturnType();
		boolean isReturnTypeVoid = Void.TYPE.equals(returnType);

		if (!isReturnTypeVoid) {
			outputReturn(ret);
		}

		logger.trace("[Trace End]   " + "pid=" + PID + " " + getShortClassName(target) + "::" + method.getName() + "()");
	}

	private void outputArg(Object value) {
		outputTypeValue(value, "  [argument]  ");
	}

	private void outputReturn(Object value) {
		outputTypeValue(value, "  [return]    ");
	}

	private void outputTypeValue(Object value, String preMessage) {
		if (value != null) {
			logger.trace(preMessage + "pid=" + PID + " " + value.getClass().getSimpleName() + "=" + value.toString());
		} else {
			logger.trace(preMessage + "pid=" + PID + " " + "null");
		}
	}

	private boolean isOutputTraceMethod(Method method) {
		boolean ret = true;
		if (METHOD_NAME_TOSTRING.equals(method.getName())) {
			ret = false;
		}
		return ret;
	}

	private String getShortClassName(Object obj) {
		String retStr = obj.getClass().getSimpleName();
		return retStr;
	}
}
