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
	var eagleApp = angular.module('eagleApp');

	serviceModule.service('Application', function($q, $location, $wrapState, Entities) {
		var Application = {};
		var _current;
		var _featureCache = {};// After loading feature will be in cache. Which will not load twice.
		var _deferred;

		Application.list = [];
		Application.list.set = {};
		Application.featureList = [];
		Application.featureList.set = {};

		// Set current application
		Application.current = function(app) {
			if(arguments.length && _current !== app) {
				var _prev = _current;
				_current = app;

				if(sessionStorage && _current) {
					sessionStorage.setItem("application", _current.tags.application);
				}

				if(_prev) {
					console.log("[Application] Switch. Redirect to landing page.");
					$wrapState.go('landing', true);
				}
			}
			return _current;
		};
		Application.find = function(appName) {
			return common.array.find(appName, Application.list, "tags.application");
		};

		Application.reload = function() {
			_deferred = $q.defer();

			if(Application.list && Application.list._promise) Application.list._promise.abort();
			if(Application.featureList && Application.featureList._promise) Application.featureList._promise.abort();

			Application.list = Entities.queryEntities("ApplicationDescService", '');
			Application.list.set = {};
			Application.featureList = Entities.queryEntities("FeatureDescService", '');
			Application.featureList.set = {};

			Application.featureList._promise.then(function() {
				var _promiseList;
				// Load feature script
				_promiseList = $.map(Application.featureList, function(feature) {
					var _ajax_deferred, _script;
					if(_featureCache[feature.tags.feature]) return;

					_featureCache[feature.tags.feature] = true;
					_ajax_deferred = $q.defer();
					_script = document.createElement('script');
					_script.type = 'text/javascript';
					_script.src = "public/feature/" + feature.tags.feature + "/controller.js?_=" + eagleApp._TRS();
					document.head.appendChild(_script);
					_script.onload = function() {
						feature._loaded = true;
						_ajax_deferred.resolve();
					};
					_script.onerror = function() {
						feature._loaded = false;
						_featureCache[feature.tags.feature] = false;
						_ajax_deferred.reject();
					};
					return _ajax_deferred.promise;
				});

				// Merge application & feature
				Application.list._promise.then(function() {
					// Fill feature set
					$.each(Application.featureList, function(i, feature) {
						Application.featureList.set[feature.tags.feature] = feature;
					});

					// Fill application set
					$.each(Application.list, function(i, application) {
						Application.list.set[application.tags.application] = application;
						application.features = application.features || [];
						var _configObj = common.parseJSON(application.config, {});
						var _appFeatureList = $.map(application.features, function(featureName) {
							var _feature = Application.featureList.set[featureName];
							if(!_feature) {
								console.warn("[Application] Feature not mapping:", application.tags.application, "-", featureName);
							} else {
								return _feature;
							}
						});

						// Find feature
						_appFeatureList.find = function(featureName) {
							return common.array.find(featureName, _appFeatureList, "tags.feature");
						};

						Object.defineProperties(application, {
							featureList: {
								get: function () {
									return _appFeatureList;
								}
							},
							// Get format group name. Will mark as 'Others' if no group defined
							groupName: {
								get: function () {
									return this.group || "Others";
								}
							},
							configObj: {
								get: function() {
									return _configObj;
								}
							},
							displayName: {
								get: function() {
									return this.alias || this.tags.application;
								}
							}
						});
					});

					// Set current application
					if(!Application.current() && sessionStorage && Application.find(sessionStorage.getItem("application"))) {
						Application.current(Application.find(sessionStorage.getItem("application")));
					}
				});

				// Process all promise
				$q.all(_promiseList.concat(Application.list._promise)).finally(function() {
					_deferred.resolve(Application);
				});
			}, function() {
				_deferred.reject(Application);
			});

			return _deferred.promise;
		};

		Application._promise = function() {
			if(!_deferred) {
				Application.reload();
			}
			return _deferred.promise;
		};

		return Application;
	});
})();