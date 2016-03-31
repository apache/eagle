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
	serviceModule.service('Site', function($rootScope, $wrapState, $location, $q, Entities, Application) {
		var _currentSite;
		var Site = {};
		var _promise;

		Site.list = [];
		Site.list.set = {};

		Site.current = function(site) {
			if(site) {
				var _prev = _currentSite;
				_currentSite = site;

				// Keep current site and reload page
				if(!_prev || _prev.tags.site !== _currentSite.tags.site) {
					if(sessionStorage) {
						sessionStorage.setItem("site", _currentSite.tags.site);
					}

					if(!$wrapState.current.abstract && $wrapState.current.name !== "login") {
						console.log("[Site]", "Switch. Reload.");
						$wrapState.reload();
					}
				}
			}
			return _currentSite;
		};
		Site.find = function(siteName) {
			return common.array.find(siteName, Site.list, "tags.site");
		};
		Site.url = function(site, url) {
			console.warn("[Site] Site.url is a deprecated function.");
			if(arguments.length == 1) {
				url = site;
			} else {
				Site.current(site);
			}
			$wrapState.url(url);

			if ($rootScope.$$phase != '$apply' && $rootScope.$$phase != '$digest') {
				$rootScope.$apply();
			}
		};

		Site.currentSiteApplication = function() {
			var _app = Application.current();
			if(!_app) return null;

			return _currentSite.applicationList.set[_app.tags.application];
		};

		Site.reload = function() {
			var _applicationList;

			if(Site.list && Site.list._promise) Site.list._promise.abort();

			Site.list = Entities.queryEntities("SiteDescService", '');
			Site.list.set = {};
			_applicationList = Entities.queryEntities("SiteApplicationService", '');

			_promise = $q.all([Site.list._promise, _applicationList._promise, Application._promise()]).then(function() {
				// Fill site set
				$.each(Site.list, function(i, site) {
					var _list = [];
					var _appGrp = {};
					var _appGrpList = [];
					_list.set = {};
					Site.list.set[site.tags.site] = site;

					// Find application
					_list.find = function(applicationName) {
						return common.array.find(applicationName, _list, "tags.application");
					};

					// Define properties
					Object.defineProperties(site, {
						applicationList: {
							get: function() {
								return _list;
							}
						},
						applicationGroup: {
							get: function() {
								return _appGrp;
							}
						},
						applicationGroupList: {
							get: function() {
								return _appGrpList;
							}
						}
					});
				});

				// Fill site application mapping
				$.each(_applicationList, function(i, siteApplication) {
					var _site = Site.list.set[siteApplication.tags.site];
					var _application = Application.find(siteApplication.tags.application);
					var _appGroup, _configObj;

					if(!_site) {
						console.warn("[Site] Application not match site:", siteApplication.tags.site, "-", siteApplication.tags.application);
					} else if(!_application) {
						console.warn("[Site] Application not found:", siteApplication.tags.site, "-", siteApplication.tags.application);
					} else {
						_configObj = common.properties.parse(siteApplication.config, {});
						Object.defineProperties(siteApplication, {
							application: {
								get: function () {
									return _application;
								}
							},
							configObj: {
								get: function () {
									return _configObj;
								}
							}
						});

						_site.applicationList.push(siteApplication);
						_site.applicationList.set[siteApplication.tags.application] = siteApplication;

						_appGroup = _site.applicationGroup[_application.group] = _site.applicationGroup[_application.group] || [];
						_appGroup.push(_application);
					}
				});

				// Fill site application group attributes
				$.each(Site.list, function(i, site) {
					$.each(site.applicationGroup, function(grpName, grpList) {
						var grp = {
							name: grpName,
							list: grpList,
							enabledList: $.grep(grpList, function(application) {return site.applicationList.set[application.tags.application].enabled;}),
							disabledList: $.grep(grpList, function(application) {return !site.applicationList.set[application.tags.application].enabled;})
						};

						site.applicationGroupList.push(grp);
					});

					site.applicationGroupList.sort(function(a, b) {
						if(a.name === b.name) return 0;
						if(a.name === "Others") return 1;
						if(b.name === "Others") return -1;
						return a.name < b.name ? -1 : 1;
					});
				});

				// Set current site
				if(sessionStorage && Site.find(sessionStorage.getItem("site"))) {
					Site.current(Site.find(sessionStorage.getItem("site")));
				} else {
					Site.current(Site.list[0]);
				}

				return Site;
			});

			return _promise;
		};

		Site._promise = function() {
			if(!_promise) {
				Site.reload();
			}
			return _promise;
		};

		return Site;
	});
})();