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

public class ProxyTargetInstanceFactory {

	
	private static final String LOG_PROXY_FACTORY = "com.toshiba.mwcloud.gs.sql.internal.proxy.LogProxyFactory";
	
	private static ProxyTargetInstanceFactory proxyTargetInstanceFactory = new ProxyTargetInstanceFactory();
	
	private boolean createLogProxy = false;

	private ProxyTargetInstanceFactory() {
		try {
			
			Class.forName(LOG_PROXY_FACTORY);
			this.createLogProxy = true;
		} catch (ClassNotFoundException e) {
		}
	}

	public static ProxyTargetInstanceFactory getInstance() {
		return proxyTargetInstanceFactory;
	}

	public <T> T getTargetInstance(T instance) {
		if (this.createLogProxy) {
			
			try {
				ProxyFactory proxyFactory = (ProxyFactory) Class.forName(LOG_PROXY_FACTORY).newInstance();
				return proxyFactory.create(instance);
			} catch (Exception e) {
				throw new Error(e);
			}
		} else {
			return instance;
		}
	}

}
