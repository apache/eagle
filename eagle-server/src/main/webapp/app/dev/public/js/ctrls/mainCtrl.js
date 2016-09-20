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

	// ======================================================================================
	// =                                        Home                                        =
	// ======================================================================================
	eagleControllers.controller('homeCtrl', function ($scope, $wrapState, PageConfig) {
		PageConfig.title = "Home";
	});

	// ======================================================================================
	// =                                       Set Up                                       =
	// ======================================================================================
	eagleControllers.controller('setupCtrl', function ($wrapState, $scope, PageConfig, Entity, Site) {
		PageConfig.hideTitle = true;

		$scope.lock = false;
		$scope.siteId = "sandbox";
		$scope.siteName = "Sandbox";
		$scope.description = "";

		$scope.createSite = function () {
			$scope.lock = true;

			Entity.create("sites", {
				siteId: $scope.siteId,
				siteName: $scope.siteName,
				description: $scope.description
			})._then(function () {
				Site.reload();
				$wrapState.go('home');
			}, function (res) {
				$.dialog({
					title: "OPS!",
					content: res.message
				});
			}).finally(function () {
				$scope.lock = false;
			});
		};
	});
}());
