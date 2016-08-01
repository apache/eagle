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

		Site.list = [];

		// Load sites
		Site.reload = function () {
			var list = Site.list = Entity.query('sites');
			list._promise.then(function () {
				$.each(list, function (i, site) {
					var applications = common.array.find(site.siteId, Application.list, 'site.siteId', true);
					$.each(applications, function (i, app) {
						app.descriptor = app.descriptor || {};
						var oriApp = Application.providers[app.descriptor.type];
						Object.defineProperty(app, 'origin', {
							get: function () {
								return oriApp;
							}
						});
					});

					Object.defineProperties(site, {
						applicationList: {
							get: function () {
								return applications;
							}
						}
					});
				});
			});
		};

		// Find Site
		Site.find = function (siteId) {
			return common.array.find(siteId, Site.list, 'siteId');
		};

		Site.getPromise = function (config) {
			return $q.all([Site.list._promise, Application.getPromise()]).then(function(dataList) {
				// Site check
				if(config.site !== false && Site.list.length === 0) {
					$wrapState.go('setup', 1);
					return $q.reject(Site);
				}

				// Application check
				if(config.application !== false && Application.list.length === 0) {
					$wrapState.go('integration.site', {id: Site.list[0].siteId}, 1);
					return $q.reject(Site);
				}

				return Site;
			});
		};

		// Initialization
		Site.reload();

		return Site;
	});
}());
