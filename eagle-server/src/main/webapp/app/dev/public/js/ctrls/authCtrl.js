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

	eagleControllers.controller('loginCtrl', function ($scope, $wrapState, Auth) {
		$scope.username = '';
		$scope.password = '';

		$scope.login = function () {
			Auth.login($scope.username, $scope.password).then(function (result) {
				if (result) {
					$wrapState.go('home');
				} else {
					$.dialog({
						title: 'OPS',
						content: 'Username or password not correct.'
					});
				}
			});
		};

		$scope.onKeyPress = function (event) {
			if (event.which === 13) {
				$scope.login();
			}
		};

		$('[name="username"]').focus();
	});
})();
