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

	// ===========================================================
	// =                         Service                         =
	// ===========================================================
	// Feature page
	serviceModule.service('PageConfig', function() {
		var _tmplConfig = {
			pageTitle: "",
			pageSubTitle: "",

			hideSite: false,
			lockSite: false,
			hideApplication: false,
			hideSidebar: false,
			hideUser: false,

			// Current page navigation path
			navPath: [],

			navConfig: {}
		};

		var PageConfig = {};

		// Reset
		PageConfig.reset = function() {
			$.extend(PageConfig, _tmplConfig);
			PageConfig.navPath = [];
		};
		PageConfig.reset();

		// Create navigation path
		PageConfig.addNavPath = function(title, path) {
			PageConfig.navPath.push({
				title: title,
				path: path
			});
			return PageConfig;
		};

		return PageConfig;
	});

	// Feature page
	serviceModule.service('FeaturePageConfig', function(Application) {
		var config = {
			// Feature mapping pages
			_navItemMapping: {}
		};

		// Register feature controller
		config.addNavItem = function(feature, item) {
			var _navItemList = config._navItemMapping[feature] = config._navItemMapping[feature] || [];
			_navItemList.push(item);
		};

		// Page list
		Object.defineProperty(config, "pageList", {
			get: function() {
				var _app = Application.current();
				var _list = [];

				if(_app && _app.features) {
					$.each(_app.features, function(i, featureName) {
						_list = _list.concat(config._navItemMapping[featureName] || []);
					});
				}

				return _list;
			}
		});

		return config;
	});

	// Configuration page
	serviceModule.service('ConfigPageConfig', function(Application) {
		var _originPageList = [
			{icon: "server", title: "Sites", url: "#/config/site"},
			{icon: "cubes", title: "Applications", url: "#/config/application"},
			{icon: "leaf", title: "Features", url: "#/config/feature"}
		];

		var config = {
			_navItemMapping: {}
		};

		// Register feature controller
		config.addNavItem = function(feature, item) {
			var _navItemList = config._navItemMapping[feature] = config._navItemMapping[feature] || [];
			_navItemList.push(item);
		};

		// Page list
		Object.defineProperty(config, "pageList", {
			get: function() {
				var _list = _originPageList;

				$.each(Application.featureList, function(i, feature) {
					_list = _list.concat(config._navItemMapping[feature.name] || []);
				});

				return _list;
			}
		});

		return config;
	});
})();
