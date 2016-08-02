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

	// ============================================================
	// =                           Page                           =
	// ============================================================
	serviceModule.service('PageConfig', function() {
		function PageConfig() {
		}

		PageConfig.reset = function () {
			PageConfig.title = "";
			PageConfig.subTitle = "";
			PageConfig.navPath = [];
			PageConfig.hideTitle = false;
		};

		return PageConfig;
	});

	// ============================================================
	// =                          Portal                          =
	// ============================================================
	var defaultPortalList = [
		{name: "Home", icon: "home", path: "#/"},
		{name: "Insight", icon: "heartbeat", list: [
			{name: "Dashboards"},
			{name: "Metrics"}
		]},
		{name: "Alert", icon: "bell", list: [
			{name: "Explore Alerts", path: "#/alert/"},
			{name: "Manage Policies", path: "#/alert/policyList"},
			{name: "Define Policy", path: "#/alert/policyCreate"}
		]}
	];
	var adminPortalList = [
		{name: "Integration", icon: "puzzle-piece", list: [
			{name: "Sites", path: "#/integration/siteList"},
			{name: "Streams", path: "#/integration/streamList"}
		]}
	];

	serviceModule.service('Portal', function($wrapState, Site) {
		var Portal = {};

		var backHome = {name: "Back home", icon: "arrow-left", path: "#/"};
		var mainList = [];

		Portal.refresh = function () {
			// TODO: check admin
			mainList = defaultPortalList.concat(adminPortalList);

			var siteList = $.map(Site.list, function (site) {
				return {
					name: site.siteName,
					path: "#/site/" + site.siteId
				};
			});

			mainList.push({
				name: "Sites", icon: "server", list: siteList});
		};

		Object.defineProperty(Portal, 'list', {
			get: function () {
				var isSite = /^site/.test($wrapState.state.current.name || "");
				return isSite ? [backHome] : mainList;
			}
		});


		// Initialization
		Site.onReload(Portal.refresh);

		Portal.refresh();

		return Portal;
	});
}());
