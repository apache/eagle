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
	eagleControllers.controller('configSiteCtrl', function ($scope, configPageConfig, globalContent, Site, Application) {
		'use strict';

		globalContent.hideApplication = true;
		globalContent.setConfig(configPageConfig);

		// =================== Site ===================
		$scope.site = Site.list[0];
		$scope.setSite = function (site) {
			$scope.site = site;
		};
	});

	eagleControllers.controller('configApplicationCtrl', function ($scope, configPageConfig, globalContent, Application) {
		'use strict';

		globalContent.hideApplication = true;
		globalContent.setConfig(configPageConfig);

		// ================ Application ================
		$scope.application = Application.list[0];
		$scope.setApplication = function (application) {
			$scope.application = application;
		};
	});
});