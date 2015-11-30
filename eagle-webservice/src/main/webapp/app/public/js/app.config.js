/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

(function() {
	'use strict';

	app.config = {
		// ============================================================================
		// =                                   URLs                                   =
		// ============================================================================
		urls: {
			HOST: '..',

			updateEntity: 'rest/entities?serviceName=${serviceName}',
			queryEntity: 'rest/entities/rowkey?serviceName=${serviceName}&value=${encodedRowkey}',
			queryEntities: 'rest/list?query=${serviceName}[${condition}]{${values}}&pageSize=100000',
			deleteEntity: 'rest/entities/delete?serviceName=${serviceName}&byId=true',
			deleteEntities: 'rest/entities?query=${serviceName}[${condition}]{*}&pageSize=100000',

			queryGroup: 'rest/list?query=${serviceName}[${condition}]<${groupBy}>{${values}}&pageSize=100000',
			querySeries: 'rest/list?query=${serviceName}[${condition}]<${groupBy}>{${values}}&pageSize=100000&timeSeries=true&intervalmin=${intervalmin}',

			query: 'rest/',

			userProfile: 'rest/authentication',
			logout: 'logout',
		},

		// ============================================================================
		// =                               Data Sources                               =
		// ============================================================================
		dataSource: {
			uiInvisibleList: ["userProfile"],
		},
	};

	// ============================================================================
	// =                                   URLs                                   =
	// ============================================================================
	app.getURL = function(name, kvs) {
		var _host = localStorage.getItem("HOST") || app.config.urls.HOST;
		var _path = app.config.urls[name];
		if(!_path) throw "URL:'" + name + "' not exist!";
		var _url = (_host ? _host + "/" : '') + _path;
		if(kvs !== undefined) {
			_url = common.template(_url, kvs);
		}
		return _url;
	};

	app._Host = function(host) {
		if(host) {
			localStorage.setItem("HOST", host);
			return app;
		}
		return localStorage.getItem("HOST");
	};
	app._Host.clear = function() {
		localStorage.removeItem("HOST");
	};
	app._Host.sample = "http://localhost:9099/eagle-service";
})();