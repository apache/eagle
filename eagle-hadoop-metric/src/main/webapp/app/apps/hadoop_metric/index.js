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
		url: "/hadoopService/HBase/overview?startTime&endTime",
		site: true,
		templateUrl: "partials/overview.html",
		controller: "overviewCtrl",
		resolve: {time: true}
	}).route("HadoopMetric_HDFS", {
		url: "/hadoopService/hdfs",
		site: true,
		templateUrl: "partials/hdfs/overview.html",
		controller: "hdfsCtrl",
		resolve: {time: true}
	}).route("regionDetail", {
		url: "/hadoopService/HBase/regionDetail/:hostname",
		site: true,
		templateUrl: "partials/region/regionDetail.html",
		controller: "regionDetailCtrl",
		resolve: {time: true}
	}).route("regionList", {
		url: "/hadoopService/HBase/regionList",
		site: true,
		templateUrl: "partials/region/regionList.html",
		controller: "regionListCtrl"
	}).route("backupMasterList", {
		url: "/hadoopService/HBase/backupMasterList",
		site: true,
		templateUrl: "partials/backupMasterList.html",
		controller: "backupMasterListCtrl"
	}).route("masterDetail", {
		url: "/hadoopService/HBase/:hostname?startTime&endTime",
		site: true,
		reloadOnSearch: false,
		templateUrl: "partials/overview.html",
		controller: "overviewCtrl",
		resolve: {time: true}
	});

	hadoopMetricApp.portal({
		name: "Services", icon: "heartbeat", list: [
			{name: "HBase", path: "hadoopService/HBase/overview"},
			{name: "HDFS", path: "hadoopService/hdfs"}
		]
	}, true);

	hadoopMetricApp.service("METRIC", function ($q, $http, Time, Site, Application) {
		var METRIC = window._METRIC = {};
		METRIC.STATUS_LIVE = "live";
		METRIC.STATUS_DEAD = "dead";
		METRIC.QUERY_HBASE_METRICS = '${baseURL}/rest/entities?query=GenericMetricService[${condition}]{*}&metricName=${metric}&pageSize=${limit}';

		METRIC.QUERY_HADOOP_INSTANCE = '${baseURL}/rest/entities?query=${service}[${condition}]{*}&pageSize=${limit}';
		METRIC.QUERY_HADOOP_INSTANCE_AGG = "${baseURL}/rest/entities?query=${service}[${condition}]<${groups}>{${field}}&pageSize=${limit}";
		METRIC.QUERY_HADOOP_METRICS_INTERVAL = '${baseURL}/rest/entities?query=GenericMetricService[${condition}]<${groups}>{${field}}${order}${top}&metricName=${metric}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}&intervalmin=${intervalMin}&timeSeries=true';

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

			return common.template(METRIC["QUERY_" + queryName], {baseURL: baseURL});
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

		METRIC.hbaseMomentMetric = function (condition, metric, limit) {
			var config = {
				condition: METRIC.condition(condition),
				metric: metric,
				limit: limit || 10000
			};

			var metrics_url = common.template(getQuery("HBASE_METRICS"), config);
			return METRIC.get(metrics_url);
		};

		METRIC.aggHadoopInstance = function (service, condition, groups, field, limit) {
			var fields = field.split(/\s*,\s*/);
			var fieldStr = $.map(fields, function (field, index) {
				var matches = field.match(/^([^\s]*)(\s+.*)?$/);
				if (matches[2]) {
					orderId = index;
				}
				return matches[1];
			}).join(", ");
			var config = {
				service: service,
				condition: METRIC.condition(condition),
				groups: toFields(groups),
				field: fieldStr,
				limit: limit || 10000
			};
			var metrics_url = common.template(getQuery("HADOOP_INSTANCE_AGG"), config);
			return wrapList(METRIC.get(metrics_url));
		};

		METRIC.countHadoopRole = function(service, site, status, role, groups, filed, limit) {
			var jobCond = {
				site: site,
				status: status,
				role: role
			};
			return METRIC.aggHadoopInstance(service, jobCond, groups, filed, limit);
		};

		METRIC.hadoopMetricsAggregation = function (condition, metric, groups, field, intervalMin, startTime, endTime, top, limit) {
			var fields = field.split(/\s*,\s*/);
			var orderId = -1;
			var fieldStr = $.map(fields, function (field, index) {
				var matches = field.match(/^([^\s]*)(\s+.*)?$/);
				if (matches[2]) {
					orderId = index;
				}
				return matches[1];
			}).join(", ");


			var config = {
				condition: METRIC.condition(condition),
				startTime: Time.format(startTime),
				endTime: Time.format(endTime),
				metric: metric,
				groups: toFields(groups),
				field: fieldStr,
				order: orderId === -1 ? "" : ".{" + fields[orderId] + "}",
				top: top ? "&top=" + top : "",
				intervalMin: intervalMin,
				limit: limit || 10000
			};

			var metrics_url = common.template(getQuery("HADOOP_METRICS_INTERVAL"), config);
			var _list = wrapList(METRIC.get(metrics_url));
			_list._aggInfo = {
				groups: groups,
				startTime: Time(startTime).valueOf(),
				interval: intervalMin * 60 * 1000
			};
			_list._promise.then(function () {
				_list.reverse();
			});
			return _list;
		};

		METRIC.aggMetricsToEntities = function (list, param, flatten) {
			var _list = [];
			_list.done = false;
			_list._promise = list._promise.then(function () {
				var _startTime = list._aggInfo.startTime;
				var _interval = list._aggInfo.interval;

				$.each(list, function (i, obj) {
					var tags = {};
					$.each(list._aggInfo.groups, function (j, group) {
						tags[group] = obj.key[j];
					});

					var _subList = $.map(obj.value[0], function (value, index) {
						return {
							timestamp: _startTime + index * _interval,
							value: [value],
							tags: tags,
							flag: param
						};
					});

					if (flatten) {
						_list.push.apply(_list, _subList);
					} else {
						_list.push(_subList);
					}
				});
				_list.done = true;
				return _list;
			});
			return _list;
		};

		METRIC.hadoopHostStatus = function (service, condition, limit) {
			var config = {
				service: service,
				condition: METRIC.condition(condition),
				limit: limit || 10000
			};

			var metrics_url = common.template(getQuery("HADOOP_INSTANCE"), config);
			return wrapList(METRIC.get(metrics_url));
		};

		METRIC.getHadoopHostByStatusAndRole = function (service, siteId, status,role, limit) {
			var condition = {
				site: siteId,
				role: role,
				status: status
			};
			return METRIC.hadoopHostStatus(service, condition, limit);
		};

		METRIC.getStatusByRoleAndHost = function (service, hostname, role, siteid) {
			var hoststateinfo;
			var condition = {
				site: siteid,
				role: role,
				hostname: hostname
			};
			hoststateinfo = METRIC.hadoopHostStatus(service, condition);
			return hoststateinfo;
		};

		METRIC.getListByRoleName = function (service, role, siteid) {
			var hoststateinfos;
			var condition = {
				site: siteid,
				role: role
			};
			hoststateinfos = METRIC.hadoopHostStatus(service, condition);
			return hoststateinfos;
		};

		METRIC.getMetricObj = function () {
			var deferred = $q.defer();
			$http.get("apps/hadoop_metric/config.json").success(function (resp) {
				deferred.resolve(resp);
			});
			return deferred.promise;
		};

		METRIC.mergeMetricToOneSeries = function (metricTitle, metrics, legendName, dataOption, option) {
			var series = [];

			$.each(metrics, function (i, metricMap) {
				if (typeof metricMap !== 'undefined') {
					series.push(METRIC.metricsToSeries(legendName[i], metricMap[0], option));
				}
			});
			return {
				title: metricTitle,
				series: series,
				option: dataOption || {},
				loading: false
			};
		};

		return METRIC;
	});

	hadoopMetricApp.requireCSS("style/index.css");
	hadoopMetricApp.require("widgets/availabilityChart.js");
	hadoopMetricApp.require("ctrls/overview.js");
	hadoopMetricApp.require("ctrls/hdfs.js");
	hadoopMetricApp.require("ctrls/regionDetailCtrl.js");
	hadoopMetricApp.require("ctrls/regionListCtrl.js");
	hadoopMetricApp.require("ctrls/backupMasterListCtrl.js");
})();
