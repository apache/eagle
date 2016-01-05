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
	serviceModule.service('PageConfig', function($state) {
		var _tmplConfig = {
			pageTitle: "",
			pageSubTitle: "",

			hideSite: false,
			lockSite: false,
			hideApplication: false,
			hideSidebar: false,
			hideUser: false,

			navConfig: {}
		};

		var pageConfig = {};

		// Reset
		pageConfig.reset = function() {
			$.extend(pageConfig, _tmplConfig);
		};
		pageConfig.reset();

		return pageConfig;
	});

	// Feature page
	serviceModule.service('FeaturePageConfig', function() {
		var config = {
			_navItemMapping: {},

			pageList: [],
			navMapping: {}
		};

		config.addNavItem = function(feature, item) {
			var _navItemList = config._navItemMapping[feature] = config._navItemMapping[feature] || [];
			_navItemList.push(item);
		};

		return config;
	});

	// Configuration page
	serviceModule.service('ConfigPageConfig', function() {
		return config = {
			pageList: [
				{icon: "server", title: "Sites", url: "#/config/site"},
				{icon: "cubes", title: "Applications", url: "#/config/application"}
			],
			navMapping: {}
		};
	});
})();
