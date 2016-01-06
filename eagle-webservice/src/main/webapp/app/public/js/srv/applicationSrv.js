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
	serviceModule.service('Application', function($q, $location, $wrapState) {
		var Application = {};
		var _deferred;
		var _current;

		// TODO: Mock
		Application.list = [
			{
				name: "DAM",
				description: "Security check application",
				feature: {
					common: true,
					classification: true,
					userProfile: true,
					metadata: true,
					setup: true
				}
			},
			{
				name: "JPA",
				description: "JPA Test Application",
				feature: {}
			},
			{
				name: "TEST",
				description: "Test for something",
				feature: {}
			}
		];

		// TODO: Mock
		Application.featureList = [
			{name: "common", displayName: "Common", description: "Provide the Policy & Alert feature."},
			{name: "classification", displayName: "Classification", description: "Sensitivity browser of the data classification."},
			{name: "userProfile", displayName: "User Profile", description: "Machine learning of the user profile"},
			{name: "metadata", displayName: "Metadata", description: "Stream metadata viewer"},
			{name: "setup", displayName: "Setup", description: "Stream configuration"},
		];

		// Set current application
		Application.current = function(app) {
			if(arguments.length && _current !== app) {
				var _prev = _current;
				_current = app;

				if(sessionStorage && _current) {
					sessionStorage.setItem("application", _current.name);
				}

				if(_prev) {
					console.log("[Application] Switch. Redirect to landing page.");
					$wrapState.go('landing', true);
				}
			}
			return _current;
		};
		Application.find = function(appName) {
			return common.array.find(appName, Application.list, "name");
		};

		// TODO: Mock promise
		Application._promise = function() {
			if(!_deferred) {
				_deferred = $q.defer();

				console.log("[Application]", "Do ajax mock delay.");
				setTimeout(function () {
					console.log("[Application]", "Do ajax mock delay...mock list ready!");

					// Dynamic load feature js list
					var _ajaxList = $.map(Application.featureList, function (feature) {
						var _ajax_deferred = $q.defer();
						var _script = document.createElement('script');
						_script.type = 'text/javascript';
						_script.src = "public/feature/" + feature.name + "/controller.js?_=" + Math.random();
						document.head.appendChild(_script);
						_script.onload = function() {
							_ajax_deferred.resolve();
						};
						_script.onerror = function() {
							_ajax_deferred.reject();
						};
						return _ajax_deferred.promise;
					});
					$q.all(_ajaxList).then(function() {
						console.log("[Application]", "Load module...finished!");
						if(sessionStorage && Application.find(sessionStorage.getItem("application"))) {
							Application.current(Application.find(sessionStorage.getItem("application")));
						} else {
							Application.current(Application.list[0]);
						}
						_deferred.resolve(Application);
					});
				}, 1000);
			}
			return _deferred.promise;
		};

		return Application;
	});
})();