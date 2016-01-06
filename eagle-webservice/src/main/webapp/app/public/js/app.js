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

var app = {};

(function() {
	'use strict';

	/* App Module */
	var eagleApp = angular.module('eagleApp', ['ngRoute', 'ngCookies', 'ui.router', 'eagleControllers', 'featureControllers', 'damControllers', 'eagle.service']);

	// ======================================================================================
	// =                                   Feature Module                                   =
	// ======================================================================================
	var featureControllers = angular.module('featureControllers', ['ui.bootstrap', 'eagle.components']);
	featureControllers.config(function ($routeProvider, $controllerProvider) {
		var _features = {};
		var Feature = function(name) {
			this.name = name;
		};

		Feature.prototype.controller = function(name, constructor) {
			$controllerProvider.register(this.name + "_" + name, constructor);
		};

		Feature.prototype.navItem = function(path, title, icon) {
			title = title || path;
			icon = icon || "question";

			featureControllers.FeaturePageConfig.addNavItem(this.name, {
				icon: icon,
				title: title,
				url: "#/" + this.name + "/" + path
			});

		};

		featureControllers.register = function(featureName) {
			return _features[featureName] = _features[featureName] || new Feature(featureName);
		};
	});

	// ======================================================================================
	// =                                   Router config                                    =
	// ======================================================================================
	eagleApp.config(function ($stateProvider, $urlRouterProvider) {
		// Resolve
		function _resolve(config) {
			config = config || {};
			var resolve = {
				Site: function (Site) {
					return Site._promise();
				},
				Authorization: function (Authorization) {
					if(!config.roleType) {
						return Authorization._promise();
					} else {
						return Authorization.rolePromise(config.roleType);
					}
				},
				Application: function (Application) {
					return Application._promise();
				}
			};

			if(config.featureCheck) {
				resolve._navigationCheck = function($q, $wrapState, Site, Application, FeaturePageConfig) {
					var _deferred = $q.defer();

					$q.all(Site._promise(), Application._promise()).then(function() {
						var _match;
						var _site = Site.current();
						var _app = Application.current();

						FeaturePageConfig.pageList = [];

						// Check application
						if(_site && (!_app || (_app && !_site.app[_app.name]))) {
							_match = false;

							$.each(Application.list, function(i, app) {
								if(_site.app[app.name]) {
									_app = Application.current(app);
									_match = true;
									return false;
								}
							});

							if(!_match) {
								_app = null;
								Application.current(null);
							}
						}

						// Update feature navigation list
						if(_app && _app.feature) {
							$.each(Application.featureList, function (i, feature) {
								if(!_app.feature[feature.name]) return;

								FeaturePageConfig.pageList.push.apply(FeaturePageConfig.pageList, FeaturePageConfig._navItemMapping[feature.name] || []);
							});
						}

						_deferred.resolve();
					});

					return _deferred.promise;
				};
			}

			return resolve;
		}

		// Router
		$urlRouterProvider.otherwise("/landing");
		$stateProvider
			// Landing
			.state('landing', {
				url: "/landing",
				templateUrl: "partials/landing.html",
				controller: "landingCtrl",
				resolve: _resolve({featureCheck: true})
			})

			// Authorization
			.state('login', {
				url: "/login",
				templateUrl: "partials/login.html",
				controller: "authLoginCtrl",
				access: {skipCheck: true}
			})

			// Configuration
			.state('config', {
				url: "/config/site",
				templateUrl: "partials/config/site.html",
				controller: "configSiteCtrl",
				pageConfig: "ConfigPageConfig",
				resolve: _resolve({roleType: 'ROLE_ADMIN'})
			})

			// Dynamic feature page
			.state('page', {
				url: "/:feature/:page",
				templateUrl: function ($stateParams) {
					return "public/feature/" + $stateParams.feature + "/page/" + $stateParams.page;
				},
				controllerProvider: function ($stateParams) {
					return $stateParams.feature + "_" + $stateParams.page + "Ctrl";
				},
				resolve: _resolve({featureCheck: true}),
				pageConfig: "FeaturePageConfig"
			})
		;
	});

	eagleApp.filter('parseJSON', function () {
		return function (input, defaultVal) {
			return common.parseJSON(input, defaultVal);
		};
	});

	eagleApp.filter('split', function () {
		return function (input, regex) {
			return input.split(regex);
		};
	});

	eagleApp.filter('reverse', function () {
		return function (items) {
			return items.slice().reverse();
		};
	});

	// ======================================================================================
	// =                                   Main Controller                                  =
	// ======================================================================================
	eagleApp.controller('MainCtrl', function ($scope, $location, $wrapState, $http, $injector, PageConfig, Site, Authorization, Entities, nvd3, Application, FeaturePageConfig) {
		featureControllers.FeaturePageConfig = FeaturePageConfig;

		window.site = $scope.Site = $scope.site = Site;
		window.auth = $scope.Auth = $scope.auth = Authorization;
		window.entities = $scope.Entities = $scope.entities = Entities;
		window.application = $scope.Application = Application;
		window.pageConfig = $scope.PageConfig = PageConfig;
		window.nvd3 = nvd3;
		$scope.app = app;

		// Clean up
		$scope.$on('$stateChangeStart', function (event, next, nextParam, current, currentParam) {
			console.log("[Switch] current ->", current, currentParam);
			console.log("[Switch] next ->", next, nextParam);
			// Page initialization
			pageConfig.reset();

			// Dynamic navigation list
			if(next.pageConfig) {
				$scope.PageConfig.navConfig = $injector.get(next.pageConfig);
			} else {
				$scope.PageConfig.navConfig = {};
			}

			// Authorization
			// > Login check
			if (!common.getValueByPath(next, "access.skipCheck", false)) {
				if (!Authorization.isLogin) {
					console.log("[Authorization] Need access. Redirect...");
					$wrapState.go("login");
				}
			}

			// > Role control
			/*var _roles = common.getValueByPath(next, "access.roles", []);
			if (_roles.length && Authorization.userProfile.roles) {
				var _roleMatch = false;
				$.each(_roles, function (i, roleName) {
					if (Authorization.isRole(roleName)) {
						_roleMatch = true;
						return false;
					}
				});

				if (!_roleMatch) {
					$location.path("/dam");
				}
			}*/
		});

		// Get side bar navigation item class
		$scope.getNavClass = function (page) {
			var path = page.url.replace(/^#/, '');

			if ($location.path() === path) {
				pageConfig.pageTitle = pageConfig.pageTitle || page.title;
				return "active";
			} else {
				return "";
			}
		};

		// Get side bar navigation item class visible
		$scope.getNavVisible = function (page) {
			if (!page.roles) return true;

			for (var i = 0; i < page.roles.length; i += 1) {
				var roleName = page.roles[i];
				if (Authorization.isRole(roleName)) {
					return true;
				}
			}

			return false;
		};

		// Authorization
		$scope.logout = function () {
			console.log("[Authorization] Logout. Redirect...");
			Authorization.logout();
			$wrapState.go("login");
		};
	});
})();