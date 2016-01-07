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

	var eagleControllers = angular.module('eagleControllers');
	// =============================================================
	// =                       Configuration                       =
	// =============================================================
	eagleControllers.controller('configSiteCtrl', function ($scope, PageConfig, Site) {
		'use strict';

		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;

		// =================== Site ===================
		$scope.site = Site.current() || Site.list[0];
		$scope.setSite = function (site) {
			$scope.site = site;
		};

		$scope.sites = {};
		$.each(Site.list, function(i, site) {
			$scope.sites[site.name] = {
				app: $.extend({}, site.app)
			};
		});

		$scope.saveAll = function() {
			$.each(Site.list, function(i, site) {
				site.app = $scope.sites[site.name].app;

				// TODO: Ajax update entities
			});
		};
	});

	eagleControllers.controller('configApplicationCtrl', function ($scope, PageConfig, Application) {
		'use strict';

		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;

		// ================ Application ================
		$scope.application = Application.current() || Application.list[0];
		$scope.setApplication = function (application) {
			$scope.application = application;
		};

		$scope.applications = {};
		$.each(Application.list, function(i, app) {
			$scope.applications[app.name] = {
				feature: $.extend({}, app.feature)
			};
		});

		$scope.saveAll = function() {
			$.each(Application.list, function(i, app) {
				app.feature = $scope.applications[app.name].feature;

				// TODO: Ajax update entities
			});
		};


		/*globalContent.setConfig(configPageConfig);

		// ================ Application ================
		$scope.application = Application.list[0];
		$scope.setApplication = function (application) {
			$scope.application = application;
		};*/
	});
})();