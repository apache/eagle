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
			isLogin: false,
			user: {},
		};

		var _host = "";
		if(localStorage) {
			_host = localStorage.getItem("host") || "";
		}

		Auth.login = function (username, password) {
			var _hash = btoa(username + ':' + password);

			return Auth.sync(_hash);
		};

		Auth.logout = function () {
			Auth.isLogin = false;
			Auth.user = {};
			if (localStorage) {
				localStorage.removeItem('auth');
			}
		};

		Auth.sync = function (hash) {
			return $http.get(_host + "/rest/auth/principal", {
				headers: {
					'Authorization': "Basic " + hash
				}
			}).then(function (result) {
				if (result.data.success) {
					Auth.user = result.data.data;
					Auth.isLogin = true;
					if (localStorage) {
						localStorage.setItem('auth', hash);
					}
				}
				return result.data.success;
			}, function () {
				return false;
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
			hash: {
				get: function () {
					if (localStorage && localStorage.getItem('auth')) {
						return localStorage.getItem('auth');
					}
					return null;
				}
			},
		});

		return Auth;
	});

	serviceModule.service('$authHttp', function ($http, Auth) {
		function mergeConfig(config) {
			config = config || {};
			if (Auth.hash) {
				config.headers = config.headers || {};
				config.headers.Authorization = "Basic " + Auth.hash;
			}
			return config;
		}

		var $authHttp = function (config) {
			return $http(mergeConfig(config));
		};

		$authHttp.get = function (url, config) {
			return $http.get(url, mergeConfig(config));
		};

		$authHttp.post = function (url, data, config) {
			return $http.post(url, data, mergeConfig(config));
		};

		$authHttp.delete = function (url, config) {
			return $http.delete(url, mergeConfig(config));
		};

		return $authHttp;
	});
})();
