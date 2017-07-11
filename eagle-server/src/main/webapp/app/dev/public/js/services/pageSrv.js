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
	serviceModule.service('PageConfig', function($wrapState) {
		function PageConfig() {
		}

		PageConfig.reset = function () {
			PageConfig.title = "";
			PageConfig.subTitle = "";
			PageConfig.navPath = [];
			PageConfig.hideTitle = false;
		};

		var cachedNavPath = [];
		var cachedGenNavPath = [];
		PageConfig.getNavPath = function () {
			if (cachedNavPath !== PageConfig.navPath || cachedGenNavPath.length !== cachedNavPath.length) {
				cachedNavPath = PageConfig.navPath || [];
				cachedGenNavPath = $.map(cachedNavPath, function (navPath) {
					var pathEntity = $.extend({}, navPath);

					if (!pathEntity.path || !pathEntity.param) return pathEntity;

					// Parse param as `key=value` format
					var params = {};
					$.each(pathEntity.param, function (i, param) {
						if (!param) return;

						var match = param.match(/^([^=]+)(=(.*))?$/);
						var key = match[1];
						var value = match[3];
						params[key] = value !== undefined ? value : $wrapState.param[key];
					});

					// Generate path with param
					var path = "/" + pathEntity.path.replace(/^[\\\/]/, "");
					if (params.siteId) {
						pathEntity.path = "/site/" + $wrapState.param.siteId + path;
						delete params.siteId;
					} else {
						pathEntity.path = path;
					}
					pathEntity.path += '?' + $.map(params, function (value, key) {
						return key + '=' + value;
					}).join('&');

					return pathEntity;
				});
			}
			return cachedGenNavPath;
		};

		return PageConfig;
	});

	// ============================================================
	// =                          Portal                          =
	// ============================================================
	serviceModule.service('Portal', function($wrapState, Site, Application, Auth) {
		var defaultPortalList = [
			{name: "Overview", icon: "home", path: "#/"},
		];

		var Portal = {};

		var mainPortalList = [];
		var sitePortalList = [];
		var connectedMainPortalList = [];
		var sitePortals = {};

		Portal.register = function (portal, isSite) {
			(isSite ? sitePortalList : mainPortalList).push(portal);
		};

		function getDefaultSitePortal(site) {
			var alertPortal =  [
				{name: "Alerts", path: "#/site/" + site.siteId + "/alerts"},
				{name: "Policies", path: "#/site/" + site.siteId + "/policies"},
				{name: "Streams", path: "#/site/" + site.siteId + "/streams"},
			];

			Auth.getPromise().then(function () {
				if (Auth.isAdmin) {
					alertPortal.push(
						{name: "Policy Prototypes", path: "#/site/" + site.siteId + "/policy/prototypes"},
						{name: "Define Policy", path: "#/site/" + site.siteId + "/policy/create"}
					);
				}
			});

			return [
				{name: site.siteName || site.siteId + " Home", icon: "home", path: "#/site/" + site.siteId},
				{name: "Alert", icon: "bell", list: alertPortal},
			];
		}

		function convertSitePortal(site, portal) {
			portal = $.extend({}, portal, {
				path: portal.path ? "#/site/" + site.siteId + "/" + portal.path.replace(/^[\\\/]/, "") : null
			});

			if(portal.list) {
				portal.list = $.map(portal.list, function (portal) {
					return convertSitePortal(site, portal);
				});
			}

			return portal;
		}

		/**
		 * Merge navigation item if same name
		 */
		function mergePortalList(list) {
			var mergedList = [];
			$.each(list, function (i, portal) {
				var mergedPortal = common.array.find(portal.name, mergedList, ['name']);
				if (mergedPortal && portal.list && mergedPortal.list) {
					mergedPortal.list = mergedPortal.list.concat(portal.list);
				} else {
					mergedList.push($.extend({}, portal));
				}
			});
			return mergedList;
		}

		Portal.refresh = function () {
			// Main level
			connectedMainPortalList = defaultPortalList.concat();
			connectedMainPortalList = mergePortalList(connectedMainPortalList);

			// Site level
			sitePortals = {};
			$.each(Site.list, function (i, site) {
				var tmpSitePortalList = getDefaultSitePortal(site).concat($.map(sitePortalList, function (portal) {
					var hasApp = !!common.array.find(portal.application, site.applicationList, "descriptor.type");
					if(hasApp) {
						return convertSitePortal(site, portal);
					}
				}));

				sitePortals[site.siteId] = mergePortalList(tmpSitePortalList);
			});
		};

		Object.defineProperty(Portal, 'list', {
			get: function () {
				var match = $wrapState.path().match(/^\/site\/([^\/]*)/);
				if(match && match[1]) {
					return sitePortals[match[1]];
				} else {
					return connectedMainPortalList;
				}
			}
		});


		// Initialization
		Site.onReload(Portal.refresh);

		Portal.refresh();

		return Portal;
	});
}());
