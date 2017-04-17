/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
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

		hadoopMetricApp.controller("backupMasterListCtrl", function ($wrapState, $scope, PageConfig, METRIC) {

			// Initialization
			PageConfig.title = "HBASE BackupMasters";
			$scope.tableScope = {};
			$scope.site = $wrapState.param.siteId;
			$scope.searchPathList = [["tags", "hostname"], ["tags", "rack"], ["tags", "site"], ["status"]];
			$scope.backupMasterList = METRIC.hbaseMaster($scope.site, "standby", 1000);

			$scope.masterAll = METRIC.hbaseMaster($scope.site);


			function getCommonHeatMapSeries(name, data) {
				return {
					name: name,
					type: 'heatmap',
					data: data,
					label: {
						normal: {
							show: true,
							formatter: function (point) {
								if(point.data) {
									return point.data[3];
								}
								return "";
							}
						}
					},
					itemStyle: {
						normal: {
							borderColor: "#FFF"
						},
						emphasis: {
							shadowBlur: 10,
							shadowColor: 'rgba(0, 0, 0, 0.5)'
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
					xAxis: {
						show: false,
						data: [0,-1,-2,-3,-4],
						splitArea: {show: true}
					},
					yAxis: [{
						show: false,
						splitArea: {show: true},
						axisTick: {show: false}
					}],
					grid: {
						left: "1%",
						right: "1%",
						top: "60",
						bottom: "60"
					},
					visualMap: {
						categories: ['active', 'standby'],
						calculable: true,
						orient: 'horizontal',
						right: "2%",
						inRange: {
							color: ['#00a65a', '#dd4b39']
						}
					}
				};
			}


			// region server heatmap chart
			$scope.masterAll._promise.then(function () {
				var master_status = [];
				var master_status_category = [];
				var x = -1;
				var y = 0;
				var split = 5;
				$.each($scope.masterAll,
					/**
					 * @param {number} i
					 * @param {RegionServer} master
					 */
					function (i, master) {
						if(x === split){
							x = 0;
							y = y - 1;
						}else{
							x = x +1;
						}
						master_status.push([x, y, 0, master.tags.hostname, master.tags.rack, master.status || "-"])
					});
				for(var i = 0;i <= split; i++){
					master_status_category.push(i);
				}
				$scope.healthStatusSeries = [getCommonHeatMapSeries("Health", master_status)];
				$scope.healthStatusOption = getHealthHeatMapOption();
				$scope.healthStatusCategory = master_status_category;
				$scope.heatmapHeight = {
					'height': getHeight(y)
				};

				function getHeight(x){
					return (Math.abs(x-1)*30 + 140) + "px"
				}

				function getHealthHeatMapOption() {
					var option = getCommonHeatMapOption();
					return common.merge(option, {
						tooltip: {
							formatter: function (point) {
								if(point.data) {
									return point.data[3] + '<br/>'
									+ 'status: <span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' + point.data[5] + '<br/>'
									+ 'rack: ' +  point.data[4] + '<br/>';
								}
								return "";
							}
						}
					});
				}

			});

		});
	});
})();
