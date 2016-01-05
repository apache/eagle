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

// =============================================================
// =                     User Profile List                     =
// =============================================================
damControllers.controller('authLoginCtrl', function(globalContent, Site, Authorization, $scope) {
	'use strict';

	globalContent.hideSidebar = true;
	globalContent.hideApplication = true;
	globalContent.hideSite = true;
	globalContent.hideUser = true;

	$scope.username = "";
	$scope.password = "";
	$scope.lock = false;

	// UI
	setTimeout(function() {
		$("#username").focus();
	});

	// Login
	$scope.login = function(event, forceSubmit) {
		if($scope.lock) return;

		if(event.which === 13 || forceSubmit) {
			$scope.lock = true;

			Authorization.login($scope.username, $scope.password).then(function(success) {
				if(success) {
					Site.refresh();
					Authorization.refresh();
					Authorization.path(true);
				} else {
					$.dialog({
						title: "OPS",
						content: "User name or password not correct."
					}).on("hidden.bs.modal", function() {
						$("#username").focus();
					});
				}
			}).finally(function() {
				$scope.lock = false;
			});
		}
	};
});