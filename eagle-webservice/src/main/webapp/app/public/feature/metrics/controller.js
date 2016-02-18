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

	var featureControllers = angular.module('featureControllers');
	var feature = featureControllers.register("metrics");

	// ==============================================================
	// =                       Initialization                       =
	// ==============================================================
	/*var MATRIX_LIST = [
		{
			name: "jmx",
			list: [
				{
					sourcePrefix: "nn_jmx_metric_",
					matrix: "hadoop.namenode.fsnamesystem.hastate",
				},
			]
		},
		{
			name: "test",
			list: [
				{
					sourcePrefix: "nn_jmx_metric_",
					matrix: "hadoop.namenode.fsnamesystem.hastate",
				},
			]
		}
	];*/

	// ==============================================================
	// =                          Function                          =
	// ==============================================================

	// ==============================================================
	// =                         Controller                         =
	// ==============================================================

	// ========================= Dashboard ==========================
	feature.navItem("dashboard", "Metrics Dashboard", "line-chart");

	feature.controller('dashboard', function(PageConfig, Site, $scope, $q, Application, Entities) {
		$scope.dashboard = {
			groups: []
		};

		// TODO: Customize data load
		setTimeout(function() {
			// TODO: Mock for user data
			$scope.dashboard = {
				groups: [
					{
						name: "JMX",
						charts: [
							{
								dataSource: "nn_jmx_metric_sandbox",
								metrics: "hadoop.namenode.dfs.missingblocks",
								aggregations: ["max"]
							},
						]
					}
				]
			};
		}, 100);
	});
})();