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

	serviceModule.service('Site', function($q, $wrapState, Entity, Application) {
		var Site = {};
		var reloadListenerList = [];

		Site.list = [];

		// Link with application
		function linkApplications(siteList, ApplicationList) {
			$.each(siteList, function (i, site) {
				var applications = common.array.find(site.siteId, ApplicationList, 'site.siteId', true);

				$.each(applications, function (i, app) {
					app.descriptor = app.descriptor || {};
					var oriApp = Application.providers[app.descriptor.type];
					Object.defineProperty(app, 'origin', {
						configurable: true,
						get: function () {
							return oriApp;
						}
					});
				});

				Object.defineProperties(site, {
					applicationList: {
						configurable: true,
						get: function () {
							return applications;
						}
					}
				});
			});
		}

		// Load sites
		Site.reload = function () {
			var list = Site.list = Entity.query('sites');
			list._promise.then(function () {
				linkApplications(list, Application.list);
				$.each(reloadListenerList, function (i, listener) {
					listener(Site);
				});
			});
			return Site;
		};

		Site.onReload = function (func) {
			reloadListenerList.push(func);
		};

		// Find Site
		Site.find = function (siteId) {
			return common.array.find(siteId, Site.list, 'siteId');
		};

		Site.getPromise = function (config) {
			var siteList = Site.list;

			return $q.all([siteList._promise, Application.getPromise()]).then(function() {
				// Site check
				if(config.site !== false && siteList.length === 0) {
					$wrapState.go('setup', 1);
					return $q.reject(Site);
				}

				// Application check
				if(config.application !== false && Application.list.length === 0) {
					$wrapState.go('integration.site', {id: siteList[0].siteId}, 1);
					return $q.reject(Site);
				}

				return Site;
			});
		};

		// Initialization
		Application.onReload(function () {
			Site.reload();
		});

		Site.reload();

		return Site;
	});
}());
