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
	serviceModule.service('Site', function(Authorization, Entities, $rootScope, $route, $location) {
		'use strict';

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
})();