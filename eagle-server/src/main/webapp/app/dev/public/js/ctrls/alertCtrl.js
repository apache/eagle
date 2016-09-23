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

	// TODO: Mock data
	var publishmentTypes = [
		{
			"type": "email",
			"className": "org.apache.eagle.alert.engine.publisher.impl.AlertEmailPublisher",
			"description": "send alert to email",
			"enabled":true,
			"fields": [{"name":"sender"},{"name":"recipients"},{"name":"subject"},{"name":"smtp.server", "value":"host1"},{"name":"connection", "value":"plaintext"},{"name":"smtp.port", "value": "25"}]
		},
		{
			"type": "kafka",
			"className": "org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher",
			"description": "send alert to kafka bus",
			"enabled":true,
			"fields": [{"name":"kafka_broker","value":"sandbox.hortonworks.com:6667"},{"name":"topic"}]
		}
	];


	// ======================================================================================
	// =                                        Main                                        =
	// ======================================================================================
	eagleControllers.controller('alertCtrl', function ($scope, $wrapState, PageConfig) {
		PageConfig.title = "Alert";
		$scope.getState = function() {
			return $wrapState.current.name;
		};
	});

	// ======================================================================================
	// =                                        List                                        =
	// ======================================================================================
	eagleControllers.controller('alertListCtrl', function ($scope, $wrapState, PageConfig) {
		PageConfig.subTitle = "Explore Alerts";
	});

	// ======================================================================================
	// =                                     Policy List                                    =
	// ======================================================================================
	eagleControllers.controller('policyListCtrl', function ($scope, $wrapState, PageConfig, Entity, UI) {
		PageConfig.subTitle = "Manage Policies";

		$scope.policyList = Entity.queryMetadata("policies");

		$scope.deletePolicy = function (item) {
			UI.deleteConfirm(item.name)(function (entity, closeFunc) {
				Entity.deleteMetadata("policies/" + item.name)._promise.finally(function () {
					closeFunc();
					$scope.policyList._refresh();
				});
			});
		};
	});

	// ======================================================================================
	// =                                    Policy Create                                   =
	// ======================================================================================
	function connectPolicyEditController(entity, args) {
		var newArgs = [entity];
		Array.prototype.push.apply(newArgs, args);
		/* jshint validthis: true */
		policyEditController.apply(this, newArgs);
	}
	function policyEditController(policy, $scope, $wrapState, PageConfig, Entity) {
		$scope.policy = policy;
	}

	eagleControllers.controller('policyCreateCtrl', function ($scope, $wrapState, PageConfig, Entity) {
		PageConfig.subTitle = "Define Alert Policy";
		connectPolicyEditController({}, arguments);
	});
	eagleControllers.controller('policyEditCtrl', function ($scope, $wrapState, PageConfig, Entity) {
		PageConfig.subTitle = "Edit Alert Policy";
		var args = arguments;

		// TODO: Wait for backend data update
		$scope.policyList = Entity.queryMetadata("policies");
		$scope.policyList._promise.then(function () {
			var policy = $scope.policyList.find(function (entity) {
				return entity.name === $wrapState.param.name;
			});

			if(policy) {
				connectPolicyEditController(policy, args);
			} else {
				$.dialog({
					title: "OPS",
					content: "Policy '" + $wrapState.param.name + "' not found!"
				}, function () {
					$wrapState.go("alert.policyList");
				});
			}
		});

	});
}());
