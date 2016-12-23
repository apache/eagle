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
		hadoopMetricApp.controller("regionDetailCtrl", function ($q, $wrapState, $scope, PageConfig, Time, METRIC) {
			var cache = {};
			$scope.site = $wrapState.param.siteId;
			$scope.hostname = $wrapState.param.hostname;
			PageConfig.title = 'RegionServer ' + "(" + $scope.hostname + ")";
			$scope.metricList = [];
			Time.autoRefresh = false;

			var METRIC_NAME = [
				"hadoop.memory.nonheapmemoryusage.used",
				"hadoop.memory.heapmemoryusage.used",
				"hadoop.bufferpool.direct.memoryused",
				"hadoop.hbase.jvm.gccount",
				"hadoop.hbase.jvm.gctimemillis",
				"hadoop.hbase.ipc.ipc.queuesize",
				"hadoop.hbase.ipc.ipc.numcallsingeneralqueue",
				"hadoop.hbase.ipc.ipc.numactivehandler",
				"hadoop.hbase.ipc.ipc.queuecalltime_99th_percentile",
				"hadoop.hbase.ipc.ipc.processcalltime_99th_percentile",
				"hadoop.hbase.ipc.ipc.queuecalltime_num_ops",
				"hadoop.hbase.ipc.ipc.processcalltime_num_ops",
				"hadoop.hbase.regionserver.server.regioncount",
				"hadoop.hbase.regionserver.server.storecount",
				"hadoop.hbase.regionserver.server.memstoresize",
				"hadoop.hbase.regionserver.server.storefilesize",
				"hadoop.hbase.regionserver.server.totalrequestcount",
				"hadoop.hbase.regionserver.server.readrequestcount",
				"hadoop.hbase.regionserver.server.writerequestcount",
				"hadoop.hbase.regionserver.server.splitqueuelength",
				"hadoop.hbase.regionserver.server.compactionqueuelength",
				"hadoop.hbase.regionserver.server.flushqueuelength",
				"hadoop.hbase.regionserver.server.blockcachesize",
				"hadoop.hbase.regionserver.server.blockcachehitcount",
				"hadoop.hbase.regionserver.server.blockcounthitpercent"
			];


			$scope.refresh = function () {
				var startTime = Time.startTime();
				var endTime = Time.endTime();

				var promies = [];
				$.each(METRIC_NAME, function (i, metric_name) {
					promies.push(generateHbaseMetric(metric_name, 20, startTime, endTime));
				});
				promies.push(METRIC.regionserverStatus($scope.hostname, $scope.site));

				$q.all(promies).then(function (res) {

					var sizeoption = {
						animation: false,
						tooltip: {
							formatter: function (points) {
								return points[0].name + "<br/>" +
									$.map(points, function (point) {
										return '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
											point.seriesName + ": " +
											common.number.abbr(point.value, true);
									}).reverse().join("<br/>");
							}
						},
						legend: {
							x: 'center', y: 'bottom'
						},
						areaStyle: {normal: {}},
						yAxis: [{
							axisLabel: {
								formatter: function (value) {
									return common.number.abbr(value, true);
								}
							}
						}]
					};

					var gctimeoption = {
						legend: {
							x: 'center', y: 'bottom'
						},
						yAxis: [{
							axisLabel: {
								formatter: function (value) {
									return value / 1000 + ' S';
								}
							}
						}]
					};
					$scope.metricList = [];
					$scope.metricList.push(mergeSeries("Memory Usage", [res[0], res[1]], ["nonheap", "heap"], sizeoption));
					$scope.metricList.push(mergeSeries("Direct Memory Usage", [res[2]], ["directmemory"], sizeoption));
					$scope.metricList.push(mergeSeries("GC count", [res[3]], ["GC count"], {}));
					$scope.metricList.push(mergeSeries("GC TimeMillis", [res[4]], ["GC TimeMillis"], gctimeoption));
					$scope.metricList.push(mergeSeries("QueueSize", [res[5]], ["QueueSize"], {}));
					$scope.metricList.push(mergeSeries("NumCallsInGeneralQueue", [res[6]], ["NumCallsInGeneralQueue"], {}));
					$scope.metricList.push(mergeSeries("NumActiveHandler", [res[7]], ["NumActiveHandler"], {}));
					$scope.metricList.push(mergeSeries("IPC Queue Time (99th)", [res[8]], ["IPC Queue Time (99th)"], {}));
					$scope.metricList.push(mergeSeries("IPC Process Time (99th)", [res[9]], ["IPC Process Time (99th)"], {}));
					$scope.metricList.push(mergeSeries("QueueCallTime_num_ops", [res[10]], ["QueueCallTime_num_ops"], {}));
					$scope.metricList.push(mergeSeries("ProcessCallTime_num_ops", [res[11]], ["ProcessCallTime_num_ops"], {}));
					$scope.metricList.push(mergeSeries("RegionCount", [res[12]], ["RegionCount"], {}));
					$scope.metricList.push(mergeSeries("StoreCount", [res[13]], ["StoreCount"], {}));
					$scope.metricList.push(mergeSeries("MemStoreSize", [res[14]], ["MemStoreSize"], sizeoption));
					$scope.metricList.push(mergeSeries("StoreFileSize", [res[15]], ["StoreFileSize"], sizeoption));
					$scope.metricList.push(mergeSeries("TotalRequestCount", [res[16]], ["TotalRequestCount"], {}));
					$scope.metricList.push(mergeSeries("ReadRequestCount", [res[17]], ["ReadRequestCount"], {}));
					$scope.metricList.push(mergeSeries("WriteRequestCount", [res[18]], ["WriteRequestCount"], {}));
					$scope.metricList.push(mergeSeries("SlitQueueLength", [res[19]], ["SlitQueueLength"], {}));
					$scope.metricList.push(mergeSeries("CompactionQueueLength", [res[20]], ["CompactionQueueLength"], {}));
					$scope.metricList.push(mergeSeries("FlushQueueLength", [res[21]], ["FlushQueueLength"], {}));
					$scope.metricList.push(mergeSeries("BlockCacheSize", [res[22]], ["BlockCacheSize"], sizeoption));
					$scope.metricList.push(mergeSeries("BlockCacheHitCount", [res[23]], ["BlockCacheHitCount"], {}));
					$scope.metricList.push(mergeSeries("BlockCacheCountHitPercent", [res[24]], ["BlockCacheCountHitPercent"], {}));

					$scope.regionstatus = res[25];
				});
			};
			Time.onReload(function () {
				cache = {};
				$scope.refresh();
			}, $scope);
			$scope.refresh();


			function generateHbaseMetric(name, limit, startTime, endTime) {
				limit = limit || 100;
				var hbaseMetric;

				$scope.site = $wrapState.param.siteId;
				var condition = {
					site: $scope.site,
					component: "regionserver",
					host: $scope.hostname
				};
				hbaseMetric = METRIC.hbaseMetrics(condition, name, startTime, endTime, limit);
				return hbaseMetric._promise;
			}

			function mergeSeries(title, metrics, linename, option) {
				var series = [];
				$.each(metrics, function (i, metric) {
					series.push(METRIC.metricsToSeries(linename[i], metric, option));
				});
				return {
					title: title,
					series: series,
					option: option || {}
				};
			}
		});
	});
})
();
//# sourceURL=regionDetailCtrl.js
