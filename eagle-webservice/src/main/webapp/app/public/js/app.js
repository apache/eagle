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

/* App Module */
var eagleApp = angular.module('eagleApp', ['ngRoute', 'ngCookies', 'eagleControllers', 'damControllers', 'eagle.service']);

eagleApp.config(function($routeProvider, ApplicationProvider) {
	'use strict';

	$routeProvider.when('/dam/summary', {
		templateUrl : 'partials/dam/summary.html',
		controller : 'summaryCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},

		// Authorization
	}).when('/dam/login', {
		templateUrl : 'partials/dam/login.html',
		controller : 'authLoginCtrl',
		access: {skipCheck: true},

		// Policy
	}).when('/dam/policyList', {
		templateUrl : 'partials/dam/policyList.html',
		controller : 'policyListCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},
	}).when('/dam/policyList/:dataSource', {
		templateUrl : 'partials/dam/policyList.html',
		controller : 'policyListCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},
	}).when('/dam/policyDetail/', {
		templateUrl : 'partials/dam/policyDetail.html',
		controller : 'policyDetailCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},
	}).when('/dam/policyDetail/:encodedRowkey', {
		templateUrl : 'partials/dam/policyDetail.html',
		controller : 'policyDetailCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},
	}).when('/dam/policyEdit/:encodedRowkey', {
		templateUrl : 'partials/dam/policyEdit.html',
		controller : 'policyEditCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},
	}).when('/dam/policyCreate/', {
		templateUrl : 'partials/dam/policyEdit.html',
		controller : 'policyCreateCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},

		// Alert
	}).when('/dam/alertList', {
		templateUrl : 'partials/dam/alertList.html',
		controller : 'alertListCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},
	}).when('/dam/alertList/:dataSource', {
		templateUrl : 'partials/dam/alertList.html',
		controller : 'alertListCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},
	}).when('/dam/alertDetail/:encodedRowkey', {
		templateUrl : 'partials/dam/alertDetail.html',
		controller : 'alertDetailCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},

		// Stream
	}).when('/dam/streamList', {
		templateUrl : 'partials/dam/streamList.html',
		controller : 'streamListCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},

		// Site
	}).when('/dam/siteList', {
		templateUrl : 'partials/dam/siteList.html',
		controller : 'siteListCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},
		access: {roles: ["ROLE_ADMIN"]},

		// Sensitivity
	}).when('/dam/sensitivitySummary', {
		templateUrl : 'partials/dam/sensitivitySummary.html',
		controller : 'sensitivitySummaryCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},
	}).when('/dam/sensitivity/:dataSrc', {
		templateUrl : 'partials/dam/sensitivity.html',
		controller : 'sensitivityCtrl',
		reloadOnSearch: false,
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},

		// User Profile
	}).when('/dam/userProfileList', {
		templateUrl : 'partials/dam/userProfileList.html',
		controller : 'userProfileListCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},
	}).when('/dam/userProfileDetail/:user', {
		templateUrl : 'partials/dam/userProfileDetail.html',
		controller : 'userProfileDetailCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization._promise();},
		},

		// Configuration
	}).when('/config/site', {
		templateUrl : 'partials/config/site.html',
		controller : 'configSiteCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization.rolePromise('ROLE_ADMIN');},
		},
	}).when('/config/application', {
		templateUrl : 'partials/config/application.html',
		controller : 'configApplicationCtrl',
		resolve: {
			site: function(Site) {return Site._promise();},
			auth: function(Authorization) {return Authorization.rolePromise('ROLE_ADMIN');},
		},

	}).otherwise({
		redirectTo : '/dam/summary'
	});
});

eagleApp.service('globalContent', function(Entities, $rootScope, $route, $location) {
	'use strict';

	var content = {
		pageTitle: "",
		pageSubTitle: "",
		pageList: [],
		navPath: [],
		navMapping: {},

		hideSite: false,
		lockSite: false,
		hideApplication: false,

		dataSrcList: [],

		setConfig: function(config) {
			// Clean up
			content.navPath = [];

			// Fill configuration
			$.extend(content, config);
		},
	};

	return content;
});

eagleApp.filter('parseJSON', function() {
	'use strict';

	return function(input, defaultVal) {
		return common.parseJSON(input, defaultVal);
	};
});

eagleApp.filter('split', function() {
	'use strict';

	return function(input, regex) {
		return input.split(regex);
	};
});

eagleApp.filter('reverse', function() {
	'use strict';

	return function(items) {
		return items.slice().reverse();
	};
});

eagleApp.controller('MainCtrl', function($scope, $location, $http, globalContent, Site, Authorization, Entities, nvd3, Application) {
	'use strict';

	window.globalContent = $scope.globalContent = globalContent;
	window.site = $scope.Site = $scope.site = Site;
	window.auth = $scope.Auth = $scope.auth = Authorization;
	window.entities = $scope.Entities = $scope.entities = Entities;
	window.application = $scope.Application = Application;
	window.nvd3 = nvd3;
	$scope.app = app;

	// Clean up
	$scope.$on('$routeChangeStart', function(event, next, current) {
		// Page initialization
		globalContent.pageTitle = "";
		globalContent.pageSubTitle = "";
		globalContent.hideSite = false;
		globalContent.lockSite = false;
		globalContent.hideApplication = false;
		globalContent.hideSidebar = false;
		globalContent.hideUser = false;

		// Authorization
		// > Login check
		if(!common.getValueByPath(next, "access.skipCheck", false)) {
			if(!Authorization.isLogin) {
				$location.path("/dam/login");
			}
		}

		// > Role control
		var _roles = common.getValueByPath(next, "access.roles", []);
		if(_roles.length && Authorization.userProfile.roles) {
			var _roleMatch = false;
			$.each(_roles, function(i, roleName) {
				if(Authorization.isRole(roleName)) {
					_roleMatch = true;
					return false;
				}
			});

			if(!_roleMatch) {
				$location.path("/dam");
			}
		}
	});

	// Get side bar navigation item class
	$scope.getNavClass = function(page) {
		var path = page.url.replace(/^#/, '');

		if ($location.path() == path) {
			globalContent.pageTitle = globalContent.pageTitle || page.title;
			return "active";
		} else {
			return "";
		}
	};

	// Get side bar navigation item class visible
	$scope.getNavVisible = function(page) {
		if(!page.roles) return true;

		for(var i = 0 ; i < page.roles.length ; i += 1) {
			var roleName = page.roles[i];
			if(Authorization.isRole(roleName)) {
				return true;
			}
		}

		return false;
	};

	// Authorization
	$scope.logout = function() {
		Authorization.logout();
		$location.path("/dam/login");
	};
});
