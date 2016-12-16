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
	 * `register` is global function that for application to set up 'controller', 'service', 'directive', 'route' in Eagle
	 */
	var hadoopMetricApp = register(['ngRoute', 'ngAnimate', 'ui.router', 'eagle.service']);

	hadoopMetricApp.route("HadoopMetric", {
		url: "/hadoopMetric/",
		site: true,
		templateUrl: "partials/overview.html",
		controller: "overviewCtrl",
	}).route("HadoopMetric_HDFS", {
		url: "/hadoopMetric/hdfs",
		site: true,
		templateUrl: "partials/hdfs/index.html",
		controller: "hdfsCtrl",
	});

	hadoopMetricApp.portal({
		name: "Services", icon: "heartbeat", list: [
			{name: "Overview", path: "hadoopMetric/"},
			{name: "HDFS", path: "hadoopMetric/hdfs"},
		]
	}, true);

	hadoopMetricApp.service("METRIC", function ($q, $http, Time, Site, Application) {
		var METRIC = window._METRIC = {};

		METRIC.QUERY_HBASE_METRICS = '${baseURL}/rest/entities?query=GenericMetricService[${condition}]{*}&metricName=${metric}&pageSize=${limit}';
		METRIC.QUERY_HBASE_METRICS_WITHTIME = '${baseURL}/rest/entities?query=GenericMetricService[${condition}]{*}&metricName=${metric}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';;


		/**
		 * Fetch query content with current site application configuration
		 * @param {string} queryName
		 */
		var getQuery = METRIC.getQuery = function (queryName, siteId) {
			var baseURL;
			siteId = siteId || Site.current().siteId;
			var app = Application.find("HADOOP_METRIC_MONITOR", siteId)[0];
			var host = app.configuration["service.host"];
			var port = app.configuration["service.port"];

			if (!host && !port) {
				baseURL = "";
			} else {
				if (host === "localhost" || !host) {
					host = location.hostname;
				}
				if (!port) {
					port = location.port;
				}
				baseURL = "http://" + host + ":" + port;
			}

			return common.template(METRIC["QUERY_" + queryName], {baseURL: "http://192.168.7.192:9090"});
		};

		function wrapList(promise) {
			var _list = [];
			_list._done = false;

			_list._promise = promise.then(
				/**
				 * @param {{}} res
				 * @param {{}} res.data
				 * @param {{}} res.data.obj
				 */
				function (res) {
					_list.splice(0);
					Array.prototype.push.apply(_list, res.data.obj);
					_list._done = true;
					return _list;
				});
			return _list;
		}

		function toFields(fields) {
			return (fields || []).length > 0 ? $.map(fields, function (field) {
				return "@" + field;
			}).join(",") : "*";
		}

		METRIC.metricsToSeries = function (name, metrics, option, rawData) {
			if (arguments.length === 4 && typeof rawData === "object") {
				option = rawData;
				rawData = false;
			}

			var data = $.map(metrics, function (metric) {
				console.log(metric);
				return rawData ? metric.value[0] : {
					x: metric.timestamp,
					y: metric.value[0]
				};
			});
			return $.extend({
				name: name,
				showSymbol: false,
				type: "line",
				data: data
			}, option || {});
		};

		METRIC.get = function (url, params) {
			return $http({
				url: url,
				method: "GET",
				params: params
			});
		};

		METRIC.condition = function (condition) {
			return $.map(condition, function (value, key) {
				return "@" + key + '="' + value + '"';
			}).join(" AND ");
		};


		METRIC.hbaseMetrics = function (condition, metric, limit) {
			var config = {
				condition: METRIC.condition(condition),
				metric: metric,
				limit: limit || 10000
			};

			var metrics_url = common.template(getQuery("HBASE_METRICS"), config);
			var _list = wrapList(METRIC.get(metrics_url));
			_list._promise.then(function () {
				_list.reverse();
			});
			return _list;
		};
		return METRIC;
	});

	hadoopMetricApp.requireCSS("style/index.css");
	hadoopMetricApp.require("widgets/availabilityChart.js");
	hadoopMetricApp.require("ctrls/overview.js");
	hadoopMetricApp.require("ctrls/hdfs.js");
})();
//# sourceURL=index.js
