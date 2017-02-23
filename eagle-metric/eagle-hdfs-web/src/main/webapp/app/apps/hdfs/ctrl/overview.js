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
	register(function (hdfsMetricApp) {
		hdfsMetricApp.controller("overviewCtrl", function ($q, $wrapState, $scope, PageConfig, HDFSMETRIC, Time) {
			PageConfig.title = 'HDFS';

			var cache = {};
			$scope.site = $wrapState.param.siteId;

			var namenodeInfo = HDFSMETRIC.getListByRoleName("HdfsServiceInstance", "namenode", $scope.site);

			$scope.switchNamenode = function (namenode) {
				$scope.metricList = [];
				$.each($scope.chartList, function (i) {
					var chart = $scope.chartList[i];
					var chartname = chart.name;
					$scope.metricList[chartname] = {
						title: chartname,
						series: {},
						option: {},
						loading: true,
						promises: []
					};
				});
				$scope.namenode = namenode;
				$scope.refresh();
			};

			var storageOption = {
				animation: false,
				tooltip: {
					formatter: function (points) {
						return points[0].name + "<br/>" +
							$.map(points, function (point) {
								return '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
									point.seriesName + ": " +
									common.number.abbr(point.value, true, 0);
							}).reverse().join("<br/>");
					}
				},
				yAxis: [{
					axisLabel: {
						formatter: function (value) {
							return common.number.abbr(value, true);
						}
					}
				}]
			};

			var digitalOption = {
				animation: false,
				tooltip: {
					formatter: function (points) {
						return points[0].name + "<br/>" +
							$.map(points, function (point) {
								return '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
									point.seriesName + ": " +
									common.number.abbr(point.value, false, 0);
							}).reverse().join("<br/>");
					}
				},
				yAxis: [{
					axisLabel: {
						formatter: function (value) {
							return common.number.abbr(value, false);
						}
					}
				}]
			};
			var startTime = Time.startTime();
			var endTime = Time.endTime();
			var interval = Time.diffInterval(startTime, endTime);
			var intervalMin = interval / 1000 / 60;
			var trendStartTime = Time.align(startTime, interval);
			var trendEndTime = Time.align(endTime, interval);
			$scope.site = $wrapState.param.siteId;
			function generateHdfsMetric(name, flag) {
				var result = cache[name] || namenodeInfo._promise.then(function (res) {
						$scope.activeNamenodeList = res;
						$scope.type = $wrapState.param.hostname || $scope.namenode || res[0].tags.hostname;
						var hostname = $scope.namenode || res[0].tags.hostname;
						var jobCond = {
							site: $scope.site,
							component: "namenode",
							host: hostname
						};
						return HDFSMETRIC.aggMetricsToEntities(HDFSMETRIC.hadoopMetricsAggregation(jobCond, name, ["site"], "avg(value)", intervalMin, trendStartTime, trendEndTime), flag)
							._promise.then(function (list) {
								var metricFlag = $.map(list, function (metrics) {
									return metrics[0].flag;
								});
								return [metricFlag, list];
							});
					});
				return result;
			}

			function sumMetrics(site, role, groups, filed, limit) {
				var jobCond = {
					site: site,
					role: role
				};
				return HDFSMETRIC.aggHadoopInstance("HdfsServiceInstance", jobCond, groups, filed, limit);
			}

			$scope.chartList = [
				{
					name: "MemoryUsage",
					metrics: ["nonheap", "heap"],
					linename: ["nonheap", "heap"],
					option: storageOption
				},
				{
					name: "DFSCapacity",
					metrics: ["fsnamesystemstate", "capacityused", "capacityremaining"],
					linename: ["total", "used", "remainning"],
					option: storageOption
				},
				{
					name: "Blocks",
					metrics: ["blockstotal", "missingblocks", "corruptblocks"],
					linename: ["blockstotal", "missingblocks", "corruptblocks"],
					option: digitalOption
				},
				{
					name: "Filestotal",
					metrics: ["filestotal"],
					linename: ["filestotal"],
					option: digitalOption
				},
				{
					name: "Underreplicatedblocks",
					metrics: ["underreplicatedblocks"],
					linename: ["underreplicatedblocks"],
					option: {}
				},
				{
					name: "LastCheckpointTime",
					metrics: ["lastcheckpointtime"],
					linename: ["lastcheckpointtime"],
					option: digitalOption
				},
				{
					name: "TransactionsSinceLastCheckpoint",
					metrics: ["transactionssincelastcheckpoint"],
					linename: ["transactionssincelastcheckpoint"],
					option: digitalOption
				},
				{
					name: "LastWrittenTransactionId",
					metrics: ["lastwrittentransactionid"],
					linename: ["lastwrittentransactionid"],
					option: digitalOption
				},
				{
					name: "SnapshottableDirectories",
					metrics: ["snapshottabledirectories"],
					linename: ["snapshottabledirectories"],
					option: {}
				},
				{
					name: "Snapshots",
					metrics: ["snapshots"],
					linename: ["snapshots"],
					option: {}
				},
				{
					name: "RpcAvgTime",
					metrics: ["rpcqueuetimeavgtime", "rpcprocessingtimeavgtime"],
					linename: ["queuetime", "processingtime"],
					option: {}
				},
				{
					name: "NumOpenConnections",
					metrics: ["numopenconnections"],
					linename: ["numopenconnections"],
					option: {}
				},
				{
					name: "CallQueueLength",
					metrics: ["callqueuelength"],
					linename: ["callqueuelength"],
					option: {}
				}
			];

			$scope.metricList = [];
			$.each($scope.chartList, function (i) {
				var chart = $scope.chartList[i];
				var chartname = chart.name;
				$scope.metricList[chartname] = {
					title: chartname,
					series: {},
					option: {},
					loading: true,
					promises: []
				};
			});

			$scope.refresh = function () {
				HDFSMETRIC.getMetricObj().then(function(res) {
					var namenodeMetricList = res.namenode;
					$.each($scope.chartList, function (i) {
						var chart = $scope.chartList[i];
						var metricList = chart.metrics;
						$.each(metricList, function (j) {
							var metricKey = metricList[j];
							var metricpromise = generateHdfsMetric(namenodeMetricList[metricKey], metricKey);
							var chartname = chart.name;
							$scope.metricList[chartname].promises.push(metricpromise);
						});
					});
					$.each($scope.chartList, function (k) {
						var chart = $scope.chartList[k];
						var chartname = chart.name;
						$q.all($scope.metricList[chartname].promises).then(function (resp) {
							var series = [];
							for(var r=0; r < resp.length; r+=1) {
								var rs = resp[r][1];
								if(rs.length > 0) {
									series.push(rs);
								}
							}
							$scope.metricList[chartname] = HDFSMETRIC.mergeMetricToOneSeries(chartname, series, chart.linename, chart.option);
						});
					});
				});

				HDFSMETRIC.countHadoopRole("HdfsServiceInstance", $scope.site, "active", "namenode", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.namenodeactivenum = data.value[0];
					});
				});
				HDFSMETRIC.countHadoopRole("HdfsServiceInstance", $scope.site, "standby", "namenode", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.namenodestandbynum = data.value[0];
					});
				});
				HDFSMETRIC.countHadoopRole("HdfsServiceInstance", $scope.site, "live", "datanode", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.datanodehealtynum = data.value[0];
					});
				});
				HDFSMETRIC.countHadoopRole("HdfsServiceInstance", $scope.site, "dead", "datanode", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.datanodeunhealtynum = data.value[0];
					});
				});
				sumMetrics($scope.site, "datanode", ["site"], "sum(configuredCapacityTB)")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.capacityNum = data.value[0];
					});
				});
				sumMetrics($scope.site, "datanode", ["site"], "sum(usedCapacityTB)")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.usedCapacityNum = data.value[0];
					});
				});
			};

			Time.onReload(function () {
				cache = {};
				$scope.refresh();
			}, $scope);
			$scope.refresh();
		});
	});
})();
