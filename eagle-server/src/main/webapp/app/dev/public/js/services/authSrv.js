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

	serviceModule.service('Auth', function ($http) {
		//$http.defaults.withCredentials = true;
		var Auth = {
			login: false,
			user: {},
		};

		var _host = "";
		if(localStorage) {
			_host = localStorage.getItem("host") || "";
		}

		Auth.login = function (username, password) {
			var _hash = btoa(username + ':' + password);

			Auth.sync(_hash);
		};

		Auth.sync = function (hash) {
			return $http.get(_host + "/rest/auth/principal", {
				headers: {
					'Authorization': "Basic " + hash
				}
			}).then(function (result) {
				if (result.data.success) {
					Auth.user = result.data.data;
					if (localStorage) {
						localStorage.setItem('auth', hash);
					}
				}
				return result.data.success;
			});
		};

		if (localStorage && localStorage.getItem('auth')) {
			Auth.sync(localStorage.getItem('auth'));
		}

		Object.defineProperties(Auth, {
			isAdmin: {
				get: function () {
					return (Auth.user.roles || []).indexOf('ADMINISTRATOR') !== -1;
				}
			},
		});

		return Auth;
	});
})();
