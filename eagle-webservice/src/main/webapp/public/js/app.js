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

'use strict';

/* App Module */
var eagleApp = angular.module('eagleApp', ['ngRoute', 'ngCookies', 'damControllers']);

eagleApp.config(function($routeProvider) {
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
	}).when('/dam/sensitivity', {
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

	}).otherwise({
		redirectTo : '/dam/summary'
	});
});

eagleApp.service('globalContent', function(Entities, $rootScope, $route, $location) {
	var content = {
		pageTitle: "",
		pageSubTitle: "",
		pageList: [],
		navPath: [],
		navMapping: {},

		hideSite: false,
		lockSite: false,

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

// Site
eagleApp.service('Site', function(Authorization, Entities, $rootScope, $route, $location, $q) {
	var _currentSite;
	var content = {};

	content.list = [];
	content.list.set = {};
	content.dataSrcList = [];

	content.current = function(site) {
		if(site) {
			var _prev = _currentSite;
			_currentSite = site;

			// Broadcast if site update
			if(!_prev || _prev.name !== _currentSite.name) {
				if(sessionStorage) {
					sessionStorage.setItem("site", _currentSite.name);
				}

				if(!content.hideSite) $route.reload();
			}
		}
		return _currentSite;
	};
	content.find = function(siteName) {
		return common.array.find(siteName, content.list, "name");
	};
	content.url = function(site, url) {
		if(arguments.length == 1) {
			url = site;
		} else {
			content.current(site);
		}
		$location.url(url);

		if ($rootScope.$$phase != '$apply' && $rootScope.$$phase != '$digest') {
			$rootScope.$apply();
		}
	};

	var _promise;
	content.refresh = function() {
		content.list = [];
		content.list.set = {};

		content.dataSrcList = Entities.queryEntities("AlertDataSourceService", '');
		content.dataSrcList._promise.success(function() {
			$.each(content.dataSrcList, function(i, dataSrc) {
				var _site = content.list.set[dataSrc.tags.site];
				if(!_site) {
					_site = content.list.set[dataSrc.tags.site] = {
						name: dataSrc.tags.site,
						dataSrcList: []
					};
					_site.dataSrcList.find = function(dataSrcName) {
						return common.array.find(dataSrcName, _site.dataSrcList, "tags.dataSource");
					};
					content.list.push(_site);
				}
				_site.dataSrcList.push(dataSrc);

				// UI visible check
				if($.inArray(dataSrc.tags.dataSource, app.config.dataSource.uiInvisibleList) !== -1) {
					dataSrc.hide = true;
				}
			});

			if(sessionStorage && content.find(sessionStorage.getItem("site"))) {
				content.current(content.find(sessionStorage.getItem("site")));
			} else {
				content.current(content.list[0]);
			}
		});

		_promise = content.dataSrcList._promise;
		return _promise;
	};

	content._promise = function() {
		if(!_promise) {
			content.refresh();
		}
		return _promise;
	};

	return content;
});

// Authorization
eagleApp.service('Authorization', function($http, $location, $cookies) {
	$http.defaults.withCredentials = true;

	var _path = "";
	var _userProfile = null;

	var content = {
		isLogin: true,	// Status mark. Work for UI status check, changed when eagle api return 403 authorization failure.
		login: function(username, password) {
			var _hash = btoa(username + ':' + password);
			return $http({
				url : app.getURL('userProfile'),
				method : "GET",
				headers: {
					'Authorization': "Basic " + _hash
				}
			}).then(function() {
				content.isLogin = true;
				return true;
			}, function() {
				return false;
			});
		},
		logout: function() {
			$http({
				url : app.getURL('logout'),
				method : "GET",
			});
		},
		path: function(path) {
			if(typeof path === "string") {
				_path = path;
			} else if(path === true) {
				$location.path(_path || "");
				_path = "";
			}
		},
	};

	var _promise;
	content.userProfile = {};
	content.isRole = function(role) {
		if(!content.userProfile.roles) return null;

		return content.userProfile.roles[role] === true;
	};

	content.refresh = function() {
		_promise = $http({
			url : app.getURL('userProfile'),
			method : "GET",
		}).then(function(data) {
			content.userProfile = data.data;

			// Role
			content.userProfile.roles = {};
			$.each(content.userProfile.authorities, function(i, role) {
				content.userProfile.roles[role.authority] = true;
			});
		});
		return _promise;
	};

	content._promise = function() {
		if(!_promise) {
			content.refresh();
		}
		return _promise;
	};

	return content;
});

eagleApp.service('Entities', function($http, $q, $rootScope, $location, Authorization) {
	// Query
	function _query(name, kvs) {
		kvs = kvs || {};
		var _list = [];
		var _condition = kvs._condition || {};
		var _addtionalCondition = _condition.additionalCondition || {};

		// Initial
		// > Condition
		delete kvs._condition;
		if(_condition) {
			kvs.condition = _condition.condition;
		}

		// > Values
		if(!kvs.values) {
			kvs.values = "*";
		} else if($.isArray(kvs.values)) {
			kvs.values = $.map(kvs.values, function(field) {
				return (field[0] === "@" ? '' : '@') + field;
			}).join(",");
		}

		var _url = app.getURL(name, kvs);

		// Fill special parameters
		// > Query by time duration
		if(_addtionalCondition._duration) {
			var _endTime = app.time.now();
			var _startTime = _endTime.clone().subtract(_addtionalCondition._duration, "ms");

			// Debug usage. Extend more time duration for end time
			if(_addtionalCondition.__ETD) {
				_endTime.add(_addtionalCondition.__ETD, "ms");
			}

			_addtionalCondition._startTime = _startTime;
			_addtionalCondition._endTime = _endTime;

			var _startTimeStr = _startTime.format("YYYY-MM-DD HH:mm:ss");
			var _endTimeStr = _endTime.clone().add(1, "s").format("YYYY-MM-DD HH:mm:ss");

			_url += "&startTime=" + _startTimeStr + "&endTime=" + _endTimeStr;
		} else if(_addtionalCondition._startTime && _addtionalCondition._endTime) {
			var _startTimeStr = _addtionalCondition._startTime.format("YYYY-MM-DD HH:mm:ss");
			var _endTimeStr = _addtionalCondition._endTime.clone().add(1, "s").format("YYYY-MM-DD HH:mm:ss");

			_url += "&startTime=" + _startTimeStr + "&endTime=" + _endTimeStr;
		}

		// > Query contains metric name
		if(_addtionalCondition._metricName) {
			_url += "&metricName=" + _addtionalCondition._metricName;
		}

		// > Customize page size
		if(_addtionalCondition._pageSize) {
			_url = _url.replace(/pageSize=\d+/, "pageSize=" + _addtionalCondition._pageSize);
		}

		// AJAX
		var canceler = $q.defer();
		_list._promise = $http.get(_url, {timeout: canceler.promise}).success(function(data) {
			_list.push.apply(_list, data.obj);
		});
		_list._promise.abort = function() {
			canceler.resolve();
		};

		_list._promise.then(function() {}, function(data) {
			if(data.status === 403) {
				Authorization.isLogin = false;
				$location.path("/dam/login");
			}
		});

		return _list;
	}
	function _post(url, entities) {
		var _list = [];
		_list._promise = $http({
			method: 'POST',
			url: url,
			headers: {
				"Content-Type": "application/json"
			},
			data: entities
		}).success(function(data) {
			_list.push.apply(_list, data.obj);
		});
		return _list;
	}
	function _delete(url) {
		var _list = [];
		_list._promise = $http({
			method: 'DELETE',
			url: url,
			headers: {
				"Content-Type": "application/json"
			},
		}).success(function(data) {
			_list.push.apply(_list, data.obj);
		});
		return _list;
	}
	function _parseCondition(condition) {
		var _this = this;
		_this.condition = "";
		_this.additionalCondition = {};

		if(typeof condition === "string") {
			_this.condition = condition;
		} else {
			_this.condition = $.map(condition, function(value, key) {
				if(!key.match(/^_/)) {
					if(value === undefined || value === null) {
						return '@' + key + '=~".*"';
					} else {
						return '@' + key + '="' + value + '"';
					}
				} else {
					_this.additionalCondition[key] = value;
					return null;
				}
			}).join(" AND ");
		}
		return _this;
	}

	var pkg = {
		_query: _query,
		_post: _post,

		updateEntity: function(serviceName, entities, config) {
			config = config || {};
			if(!$.isArray(entities)) entities = [entities];

			// Post clone entities
			var _entities = $.map(entities, function(entity) {
				var _entity = {};

				// Clone variables
				$.each(entity, function(key, value) {
					// Skip inner variables
					if(!key.match(/^__/)) {
						_entity[key] = entity[key];
					}
				});

				// Add timestamp
				if(config.timestamp !== false) {
					if(config.createTime !== false && !_entity.createdTime) {
						_entity.createdTime = new moment().valueOf();
					}
					if(config.lastModifiedDate !== false) {
						_entity.lastModifiedDate = new moment().valueOf();
					}
				}

				return _entity;
			});

			return _post(app.getURL("updateEntity", {serviceName: serviceName}), _entities);
		},

		deleteEntity: function(serviceName, entities) {
			if(!$.isArray(entities)) entities = [entities];

			var _entities = $.map(entities, function(entity) {
				return typeof entity === "object" ? entity.encodedRowkey : entity;
			});
			return _post(app.getURL("deleteEntity", {serviceName: serviceName}), _entities);
		},
		deleteEntities: function(serviceName, condition) {
			return _delete(app.getURL("deleteEntities", {serviceName: serviceName, condition: new _parseCondition(condition).condition}));
		},

		queryEntity: function(serviceName, encodedRowkey) {
			return _query("queryEntity", {serviceName: serviceName, encodedRowkey: encodedRowkey});
		},
		queryEntities: function(serviceName, condition, fields) {
			return _query("queryEntities", {serviceName: serviceName, _condition: new _parseCondition(condition), values: fields});
		},
		queryGroup: function(serviceName, condition, groupBy, fields) {
			return _query("queryGroup", {serviceName: serviceName, _condition: new _parseCondition(condition), groupBy: groupBy, values: fields});
		},
		querySeries: function(serviceName, condition, groupBy, fields, intervalmin) {
			var _cond = new _parseCondition(condition);
			var _list = _query("querySeries", {serviceName: serviceName, _condition: _cond, groupBy: groupBy, values: fields, intervalmin: intervalmin});
			_list._promise.success(function() {
				if(_list.length === 0) {
					_list._empty = true;
					_list._convert = true;

					for(var i = 0; i <= (_cond.additionalCondition._endTime.valueOf() - _cond.additionalCondition._startTime.valueOf()) / (1000 * 60 * intervalmin); i += 1) {
						_list.push(0);
					}
				} else if(_list.length === 1) {
					_list._convert = true;
					var _unit = _list.pop();
					_list.push.apply(_list, _unit.value[0]);
				}

				if(_list._convert) {
					var _current = _cond.additionalCondition._startTime.clone();
					$.each(_list, function(i, value) {
						_list[i] = [_current.valueOf(), value];
						_current.add(intervalmin, "m");
					});
				}
			});
			return _list;
		},

		query: function(path, params) {
			var _list = [];
			_list._promise = $http({
				method: 'GET',
				url: app.getURL("query") + path,
				params: params
			}).success(function(data) {
				_list.push.apply(_list, data.obj);
			});
			return _list;
		},

		dialog: function(data, callback) {
			if(data.success === false || (data.exception || "").trim()) {
				return $.dialog({
					title: "OPS",
					content: $("<pre>").html(data.exception)
				}, callback);
			}
			return false;
		},
	};
	return pkg;
});

eagleApp.filter('parseJSON', function() {
	return function(input, defaultVal) {
		return common.parseJSON(input, defaultVal);
	};
});

eagleApp.filter('split', function() {
	return function(input, regex) {
		return input.split(regex);
	};
});

eagleApp.filter('reverse', function() {
	return function(items) {
		return items.slice().reverse();
	};
});

eagleApp.controller('MainCtrl', function($scope, $location, $http, globalContent, Site, Authorization, Entities) {
	window.globalContent = $scope.globalContent = globalContent;
	window.site = $scope.site = Site;
	window.auth = $scope.auth = Authorization;
	window.entities = $scope.entities = Entities;
	$scope.app = app;

	// Clean up
	$scope.$on('$routeChangeStart', function(event, next, current) {
		// Page initialization
		globalContent.pageTitle = "";
		globalContent.pageSubTitle = "";
		globalContent.hideSite = false;
		globalContent.lockSite = false;
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
