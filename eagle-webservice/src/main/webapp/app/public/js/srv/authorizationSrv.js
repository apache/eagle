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
	serviceModule.service('Authorization', function ($http, $location, $q) {
		$http.defaults.withCredentials = true;

		var _promise;
		var _path = "";

		var content = {
			isLogin: true,	// Status mark. Work for UI status check, changed when eagle api return 403 authorization failure.
			needLogin: function () {
				_path = _path || $location.path();
				content.isLogin = false;
				$location.path("/dam/login");
			},
			login: function (username, password) {
				var _hash = btoa(username + ':' + password);
				return $http({
					url: app.getURL('userProfile'),
					method: "GET",
					headers: {
						'Authorization': "Basic " + _hash
					}
				}).then(function () {
					content.isLogin = true;
					return true;
				}, function () {
					return false;
				});
			},
			logout: function () {
				$http({
					url: app.getURL('logout'),
					method: "GET"
				});
			},
			path: function (path) {
				if (typeof path === "string") {
					_path = path;
				} else if (path === true) {
					$location.path(_path || "");
					_path = "";
				}
			}
		};

		content.userProfile = {};
		content.isRole = function (role) {
			if (!content.userProfile.roles) return null;

			return content.userProfile.roles[role] === true;
		};

		content.refresh = function () {
			_promise = $http({
				url: app.getURL('userProfile'),
				method: "GET"
			}).then(function (data) {
				content.userProfile = data.data;

				// Role
				content.userProfile.roles = {};
				$.each(content.userProfile.authorities, function (i, role) {
					content.userProfile.roles[role.authority] = true;
				});
			});
			return _promise;
		};

		content._promise = function () {
			if (!_promise) {
				content.refresh();
			}
			return _promise;
		};

		content.rolePromise = function(role, rejectPath) {
			var _deferred = $q.defer();
			var _oriPromise = content._promise();
			_oriPromise.then(function() {
				if(content.isRole(role)) {
					_deferred.resolve(content);
				} else {
					$location.path(rejectPath || "");
				}
			});

			return _deferred.promise;
		};

		return content;
	});
})();