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
	serviceModule.service('Authorization', function ($rootScope, $http, $wrapState, $q) {
		$http.defaults.withCredentials = true;

		var _promise;
		var _path = "";

		var content = {
			isLogin: true,	// Status mark. Work for UI status check, changed when eagle api return 403 authorization failure.
			needLogin: function () {
				console.log("[Authorization] Need Login!");
				if(content.isLogin) {
					_path = _path || $wrapState.path();
					content.isLogin = false;
					console.log("[Authorization] Call need login. Redirect...");
					$wrapState.go("login", 99);
				} else {
					console.log("[Authorization] Already login state...");
				}
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
					$wrapState.path(_path || "");
					_path = "";
				}
			}
		};

		content.userProfile = {};
		content.isRole = function (role) {
			if (!content.userProfile.roles) return null;

			return content.userProfile.roles[role] === true;
		};

		content.reload = function () {
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

				return content;
			}, function(data) {
				if(data.status === 403) {
					content.needLogin();
				}
			});
			return _promise;
		};

		content._promise = function () {
			if (!_promise) {
				content.reload();
			}
			return _promise;
		};

		content.rolePromise = function(role, rejectState) {
			var _deferred = $q.defer();
			var _oriPromise = content._promise();
			_oriPromise.then(function() {
				if(content.isRole(role)) {
					_deferred.resolve(content);
				} else if(content.isLogin) {
					_deferred.resolve(content);
					console.log("[Authorization] go landing...");
					$wrapState.go(rejectState || "landing");
				} else {
					_deferred.reject(content);
				}

				return content;
			});

			return _deferred.promise;
		};

		// Call web service to keep session
		setInterval(function() {
			if(!content.isLogin) return;

			$http.get(app.getURL('userProfile')).then(null, function (response) {
				if(response.status === 403) {
					console.log("[Session] Out of date...", response);
					content.needLogin();
				}
			});
		}, 1000 * 10);// 60 * 5);

		return content;
	});
})();