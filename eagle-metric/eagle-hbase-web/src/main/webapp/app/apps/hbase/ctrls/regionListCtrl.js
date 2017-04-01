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

 (function () {
	/**
	 * `register` without params will load the module which using require
	 */
	 register(function (hadoopMetricApp) {

	 	hadoopMetricApp.controller("regionListCtrl", function ($wrapState, $scope, PageConfig, METRIC) {

			// Initialization
			PageConfig.title = "HBASE RegionServers";
			$scope.tableScope = {};
			$scope.live = METRIC.STATUS_LIVE;
			$scope.dead = METRIC.STATUS_DEAD;
			$scope.site = $wrapState.param.siteId;
			$scope.status = $wrapState.param.status;
			$scope.searchPathList = [["tags", "hostname"], ["tags", "rack"], ["tags", "site"], ["status"]];
			$scope.regionserverList = METRIC.regionserverList($scope.site, $scope.status);


			function getCommonHeatMapSeries(name, data) {
				return {
					name: name,
					type: "heatmap",
					data: data,
					itemStyle: { //
						normal: {
							borderColor: "#FFF"
						}
					}
				};
			}

			function getCommonHeatMapOption() {
				return {
					animation: false,
					tooltip: {
						trigger: 'item'
					},
					grid: { bottom: "50" },
					visualMap: {
						categories: ['live', 'dead'],
						calculable: true,
						orient: 'horizontal',
						left: 'right',
						inRange: {
							color: ["#00a65a", "#ffdc62", "#dd4b39"]
						}
					}
				};
			}

			// region server heatmap chart
			$scope.regionserverList._promise.then(function () {
				var regionServer_status = [];
				var regionServer_status_maxCount = 0;
				var x = 0;
				var y = 1;
				var split = 5;
				$.each($scope.regionserverList,
					/**
					 * @param {number} i
					 * @param {RegionServer} regionServer
					 */
					 function (i, regionServer) {
					 	if(x === split){
					 		x = 0;
					 		y = y + 1;
					 	}else{
					 		x = x +1;
					 	}
					 	regionServer_status.push([x, y, regionServer.tags.hostname, regionServer.status || "-"])
					 });
				console.log(regionServer_status);
				$scope.healthStatusSeries = [getCommonHeatMapSeries("Health", regionServer_status)];
				console.log($scope.healthStatusSeries);
				$scope.healthStatusOption = getHealthHeatMapOption();
				console.log($scope.healthStatusOption);

				function getHealthHeatMapOption() {
					var option = getCommonHeatMapOption();
					return [common.merge(option, {
						tooltip: {
							formatter: function (point) {
								if(point.data) {
									return point.data[0] + ': <span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
									point.data[1];
								}
								return "";
							}
						}
					})];
				}

			});
		});


	});
})();
