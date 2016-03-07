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
	var eagleApp = angular.module('eagleApp', ['ngRoute', 'ngAnimate', 'ui.router', 'eagleControllers', 'featureControllers', 'eagle.service']);

	// GRUNT REPLACEMENT: eagleApp.buildTimestamp = TIMESTAMP
	eagleApp._TRS = function() {
		return eagleApp.buildTimestamp || Math.random();
	};

	// ======================================================================================
	// =                                   Feature Module                                   =
	// ======================================================================================
	var FN_ARGS = /^[^\(]*\(\s*([^\)]*)\)/m;
	var FN_ARG_SPLIT = /,/;
	var FN_ARG = /^\s*(_?)(\S+?)\1\s*$/;
	var STRIP_COMMENTS = /((\/\/.*$)|(\/\*[\s\S]*?\*\/))/mg;

	var featureControllers = angular.module('featureControllers', ['ui.bootstrap', 'eagle.components']);
	var featureControllerCustomizeHtmlTemplate = {};
	var featureControllerProvider;
	var featureProvider;

	featureControllers.config(function ($controllerProvider, $provide) {
		featureControllerProvider = $controllerProvider;
		featureProvider = $provide;
	});

	featureControllers.service("Feature", function($wrapState, PageConfig, ConfigPageConfig, FeaturePageConfig) {
		var _features = {};
		var _services = {};

		var Feature = function(name, version) {
			this.name = name;
			this.version = version;
			this.features = {};
		};

		/***
		 * Inner function. Replace the dependency of constructor.
		 * @param constructor
		 * @private
		 */
		Feature.prototype._replaceDependencies = function(constructor) {
			var i, srvName;
			var _constructor, _$inject;
			var fnText, argDecl;

			if($.isArray(constructor)) {
				_constructor = constructor[constructor.length - 1];
				_$inject = constructor.slice(0, -1);
			} else if(constructor.$inject) {
				_constructor = constructor;
				_$inject = constructor.$inject;
			} else {
				_$inject = [];
				_constructor = constructor;
				fnText = constructor.toString().replace(STRIP_COMMENTS, '');
				argDecl = fnText.match(FN_ARGS);
				$.each(argDecl[1].split(FN_ARG_SPLIT), function(i, arg) {
					arg.replace(FN_ARG, function(all, underscore, name) {
						_$inject.push(name);
					});
				});
			}
			_constructor.$inject = _$inject;

			for(i = 0 ; i < _$inject.length ; i += 1) {
				srvName = _$inject[i];
				_$inject[i] = this.features[srvName] || _$inject[i];
			}

			return _constructor;
		};

		/***
		 * Register a common service for feature usage. Common service will share between the feature. If you are coding customize feature, use 'Feature.service' is the better way.
		 * @param name
		 * @param constructor
		 */
		Feature.prototype.commonService = function(name, constructor) {
			if(!_services[name]) {
				featureProvider.service(name, constructor);
				_services[name] = this.name;
			} else {
				throw "Service '" + name + "' has already be registered by feature '" + _services[name] + "'";
			}
		};

		/***
		 * Register a service for feature usage.
		 * @param name
		 * @param constructor
		 */
		Feature.prototype.service = function(name, constructor) {
			var _serviceName;
			if(!this.features[name]) {
				_serviceName = "__FEATURE_" + this.name + "_" + name;
				featureProvider.service(_serviceName, this._replaceDependencies(constructor));
				this.features[name] = _serviceName;
			} else {
				console.warn("Service '" + name + "' has already be registered.");
			}
		};

		/***
		 * Create an navigation item in left navigation bar
		 * @param path
		 * @param title
		 * @param icon use Font Awesome. Needn't with 'fa fa-'.
		 */
		Feature.prototype.navItem = function(path, title, icon) {
			title = title || path;
			icon = icon || "question";

			FeaturePageConfig.addNavItem(this.name, {
				icon: icon,
				title: title,
				url: "#/" + this.name + "/" + path
			});
		};

		/***
		 * Register a controller.
		 * @param name
		 * @param constructor
		 */
		Feature.prototype.controller = function(name, constructor, htmlTemplatePath) {
			var _name = this.name + "_" + name;

			// Replace feature registered service
			constructor = this._replaceDependencies(constructor);

			// Register controller
			featureControllerProvider.register(_name, constructor);
			if(htmlTemplatePath) {
				featureControllerCustomizeHtmlTemplate[_name] = htmlTemplatePath;
			}

			return _name;
		};

		/***
		 * Register a configuration controller for admin usage.
		 * @param name
		 * @param constructor
		 */
		Feature.prototype.configController = function(name, constructor, htmlTemplatePath) {
			var _name = "config_" + this.name + "_" + name;

			// Replace feature registered service
			constructor = this._replaceDependencies(constructor);

			// Register controller
			featureControllerProvider.register(_name, constructor);
			if(htmlTemplatePath) {
				featureControllerCustomizeHtmlTemplate[_name] = htmlTemplatePath;
			}

			return _name;
		};

		/***
		 * Create an navigation item in left navigation bar for admin configuraion page
		 * @param path
		 * @param title
		 * @param icon use Font Awesome. Needn't with 'fa fa-'.
		 */
		Feature.prototype.configNavItem = function(path, title, icon) {
			title = title || path;
			icon = icon || "question";

			ConfigPageConfig.addNavItem(this.name, {
				icon: icon,
				title: title,
				url: "#/config/" + this.name + "/" + path
			});
		};

		// Register
		featureControllers.register = Feature.register = function(featureName) {
			_features[featureName] = _features[featureName] || new Feature(featureName);
			return _features[featureName];
		};

		// Page go
		Feature.go = function(feature, page, filter) {
			if(!filter) {
				$wrapState.go("page", {
					feature: feature,
					page: page
				}, 2);
			} else {
				$wrapState.go("pageFilter", {
					feature: feature,
					page: page,
					filter: filter
				}, 2);
			}
		};

		return Feature;
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
				resolve._navigationCheck = function($q, $wrapState, Site, Application) {
					var _deferred = $q.defer();

					$q.all(Site._promise(), Application._promise()).then(function() {
						var _match, i, tmpApp;
						var _site = Site.current();
						var _app = Application.current();

						// Check application
						if(_site && (
							!_app ||
							!_site.applicationList.set[_app.tags.application] ||
							!_site.applicationList.set[_app.tags.application].enabled
							)
						) {
							_match = false;

							for(i = 0 ; i < _site.applicationGroupList.length ; i += 1) {
								tmpApp = _site.applicationGroupList[i].enabledList[0];
								if(tmpApp) {
									_app = Application.current(tmpApp);
									_match = true;
									break;
								}
							}

							if(!_match) {
								_app = null;
								Application.current(null);
							}
						}
					}).finally(function() {
						_deferred.resolve();
					});

					return _deferred.promise;
				};
			}

			return resolve;
		}

		// Router
		var _featureBase = {
			templateUrl: function ($stateParams) {
				var _htmlTemplate = featureControllerCustomizeHtmlTemplate[$stateParams.feature + "_" + $stateParams.page];
				return  "public/feature/" + $stateParams.feature + "/page/" + (_htmlTemplate ||  $stateParams.page) + ".html?_=" + eagleApp._TRS();
			},
			controllerProvider: function ($stateParams) {
				return $stateParams.feature + "_" + $stateParams.page;
			},
			resolve: _resolve({featureCheck: true}),
			pageConfig: "FeaturePageConfig"
		};

		$urlRouterProvider.otherwise("/landing");
		$stateProvider
			// =================== Landing ===================
			.state('landing', {
				url: "/landing",
				templateUrl: "partials/landing.html?_=" + eagleApp._TRS(),
				controller: "landingCtrl",
				resolve: _resolve({featureCheck: true})
			})

			// ================ Authorization ================
			.state('login', {
				url: "/login",
				templateUrl: "partials/login.html?_=" + eagleApp._TRS(),
				controller: "authLoginCtrl",
				access: {skipCheck: true}
			})

			// ================ Configuration ================
			// Site
			.state('configSite', {
				url: "/config/site",
				templateUrl: "partials/config/site.html?_=" + eagleApp._TRS(),
				controller: "configSiteCtrl",
				pageConfig: "ConfigPageConfig",
				resolve: _resolve({roleType: 'ROLE_ADMIN'})
			})

			// Application
			.state('configApplication', {
				url: "/config/application",
				templateUrl: "partials/config/application.html?_=" + eagleApp._TRS(),
				controller: "configApplicationCtrl",
				pageConfig: "ConfigPageConfig",
				resolve: _resolve({roleType: 'ROLE_ADMIN'})
			})

			// Feature
			.state('configFeature', {
				url: "/config/feature",
				templateUrl: "partials/config/feature.html?_=" + eagleApp._TRS(),
				controller: "configFeatureCtrl",
				pageConfig: "ConfigPageConfig",
				resolve: _resolve({roleType: 'ROLE_ADMIN'})
			})

			// Feature configuration page
			.state('configFeatureDetail', $.extend({url: "/config/:feature/:page"}, {
				templateUrl: function ($stateParams) {
					var _htmlTemplate = featureControllerCustomizeHtmlTemplate[$stateParams.feature + "_" + $stateParams.page];
					return  "public/feature/" + $stateParams.feature + "/page/" + (_htmlTemplate ||  $stateParams.page) + ".html?_=" + eagleApp._TRS();
				},
				controllerProvider: function ($stateParams) {
					return "config_" + $stateParams.feature + "_" + $stateParams.page;
				},
				pageConfig: "ConfigPageConfig",
				resolve: _resolve({roleType: 'ROLE_ADMIN'})
			}))

			// =================== Feature ===================
			// Dynamic feature page
			.state('page', $.extend({url: "/:feature/:page"}, _featureBase))
			.state('pageFilter', $.extend({url: "/:feature/:page/:filter"}, _featureBase))
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
	eagleApp.controller('MainCtrl', function ($scope, $wrapState, $http, $injector, PageConfig, FeaturePageConfig, Site, Authorization, Entities, nvd3, Application, Feature, UI) {
		window.site = $scope.Site = Site;
		window.auth = $scope.Auth = Authorization;
		window.entities = $scope.Entities = Entities;
		window.application = $scope.Application = Application;
		window.pageConfig = $scope.PageConfig = PageConfig;
		window.featurePageConfig = $scope.FeaturePageConfig = FeaturePageConfig;
		window.feature = $scope.Feature = Feature;
		window.ui = $scope.UI = UI;
		window.nvd3 = nvd3;
		$scope.app = app;
		$scope.common = common;

		Object.defineProperty(window, "scope",{
			get: function() {
				return angular.element("[ui-view]").scope();
			}
		});

		// Clean up
		$scope.$on('$stateChangeStart', function (event, next, nextParam, current, currentParam) {
			console.log("[Switch] current ->", current, currentParam);
			console.log("[Switch] next ->", next, nextParam);
			// Page initialization
			PageConfig.reset();

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
					$wrapState.path("/dam");
				}
			}*/
		});

		$scope.$on('$stateChangeError', function (event, next, nextParam, current, currentParam, error) {
			console.error("[Switch] Error", arguments);
		});

		// Get side bar navigation item class
		$scope.getNavClass = function (page) {
			var path = page.url.replace(/^#/, '');

			if ($wrapState.path() === path) {
				PageConfig.pageTitle = PageConfig.pageTitle || page.title;
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