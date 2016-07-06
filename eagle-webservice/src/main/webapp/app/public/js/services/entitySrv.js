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

	var serviceModule = angular.module('eagle.service');

	var _host = "";
	if(localStorage) {
		_host = localStorage.getItem("host") || "";
	}

	serviceModule.service('Entity', function($http) {
		function Entity() {}

		// Dev usage. Set rest api source
		Entity._host = function (host) {
			console.warn("This function only used for development usage.");
			if(host) {
				_host = host.replace(/[\\\/]+$/, "");
				if(localStorage) {
					localStorage.setItem("host", _host);
				}
			}
			return _host;
		};

		Entity.queryMetadata = function (url) {
			var list = [];

			list._refresh = function () {
				list._done = false;
				list._promise = $http.get(_host + "/rest/metadata/" + url).then(function (res) {
					var data = res.data;
					list.splice(0);
					Array.prototype.push.apply(list, data);
					list._done = true;
				});
				return list;
			};

			return list._refresh();
		};

		Entity.deleteMetadata = function (url) {
			return {
				_promise: $http.delete(_host + "/rest/metadata/" + url).then(function (res) {
					console.log(res);
				})
			};
		};

		return Entity;
	});
}());