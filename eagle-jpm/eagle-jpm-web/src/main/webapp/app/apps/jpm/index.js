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
	var jpmApp = register(['ngRoute', 'ngAnimate', 'ui.router', 'eagle.service']);

	jpmApp.route("jpmList", {
		url: "/jpm/list?startTime&endTime",
		site: true,
		templateUrl: "partials/job/list.html",
		controller: "listCtrl",
		resolve: { time: true }
	}).route("jpmOverview", {
		url: "/jpm/overview?startTime&endTime",
		site: true,
		templateUrl: "partials/job/overview.html",
		controller: "overviewCtrl",
		resolve: { time: true }
	}).route("jpmStatistics", {
		url: "/jpm/statistics",
		site: true,
		templateUrl: "partials/job/statistic.html",
		controller: "statisticCtrl"
	}).route("jpmDetail", {
		url: "/jpm/detail/:jobId",
		site: true,
		templateUrl: "partials/job/detail.html",
		controller: "detailCtrl"
	}).route("jpmJobTask", {
		url: "/jpm/jobTask/:jobId?startTime&endTime",
		site: true,
		templateUrl: "partials/job/task.html",
		controller: "jobTaskCtrl"
	}).route("jpmCompare", {
		url: "/jpm/compare/:jobDefId?from&to",
		site: true,
		reloadOnSearch: false,
		templateUrl: "partials/job/compare.html",
		controller: "compareCtrl"
	});

	jpmApp.portal({name: "YARN Jobs", icon: "taxi", list: [
		{name: "Overview", path: "jpm/overview"},
		{name: "Job Statistics", path: "jpm/statistics"},
		{name: "Job List", path: "jpm/list"}
	]}, true);

	jpmApp.service("JPM", function ($q, $http, Time, Site, Application) {
		var JPM = window._JPM = {};

		// TODO: timestamp support
		JPM.QUERY_LIST = '${baseURL}/rest/entities?query=${query}[${condition}]{${fields}}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';
		JPM.QUERY_GROUPS = '${baseURL}/rest/entities?query=${query}[${condition}]<${groups}>{${field}}${order}${top}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';
		JPM.QUERY_GROUPS_INTERVAL = '${baseURL}/rest/entities?query=${query}[${condition}]<${groups}>{${field}}${order}${top}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}&intervalmin=${intervalMin}&timeSeries=true';
		JPM.QUERY_METRICS = '${baseURL}/rest/entities?query=GenericMetricService[${condition}]{*}&metricName=${metric}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';
		JPM.QUERY_METRICS_AGG = '${baseURL}/rest/entities?query=GenericMetricService[${condition}]<${groups}>{${field}}${order}${top}&metricName=${metric}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';
		JPM.QUERY_METRICS_INTERVAL = '${baseURL}/rest/entities?query=GenericMetricService[${condition}]<${groups}>{${field}}${order}${top}&metricName=${metric}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}&intervalmin=${intervalMin}&timeSeries=true';
		JPM.QUERY_MR_JOBS = '${baseURL}/rest/mrJobs/search';
		JPM.QUERY_JOB_LIST = '${baseURL}/rest/mrJobs?query=%s[${condition}]{${fields}}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';
		JPM.QUERY_JOB_STATISTIC = '${baseURL}/rest/mrJobs/jobCountsByDuration?site=${site}&timeDistInSecs=${times}&startTime=${startTime}&endTime=${endTime}&jobType=${jobType}';
		JPM.QUERY_TASK_STATISTIC = '${baseURL}/rest/mrTasks/taskCountsByDuration?jobId=${jobId}&site=${site}&timeDistInSecs=${times}&top=${top}';

		JPM.QUERY_MR_JOB_COUNT = '${baseURL}/rest/mrJobs/runningJobCounts';
		//JPM.QUERY_MR_JOB_METRIC_TOP = '${baseURL}eagle-service/rest/mrJobs/jobMetrics/entities';

		/**
		 * Fetch query content with current site application configuration
		 * @param {string} queryName
		 */
		var getQuery = JPM.getQuery = function(queryName, siteId) {
			var baseURL;
			siteId = siteId || Site.current().siteId;
			var app = Application.find("JPM_WEB_APP", siteId)[0];
			var host = app.configuration["service.host"];
			var port = app.configuration["service.port"];

			if(!host && !port) {
				baseURL = "";
			} else {
				if(host === "localhost" || !host) {
					host = location.host;
				}
				if(!port) {
					port = location.port;
				}
				baseURL = "http://" + host + ":" + port;
			}

			return common.template(JPM["QUERY_" + queryName], {baseURL: baseURL});
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

		JPM.get = function (url, params) {
			return $http({
				url: url,
				method: "GET",
				params: params
			});
		};

		JPM.condition = function (condition) {
			return $.map(condition, function (value, key) {
				return "@" + key + '="' + value + '"';
			}).join(" AND ");
		};

		/**
		 * Fetch eagle query list
		 * @param query
		 * @param condition
		 * @param {[]?} groups
		 * @param {string} field
		 * @param {number|null} intervalMin
		 * @param startTime
		 * @param endTime
		 * @param {(number|null)?} top
		 * @param {number?} limit
		 * @return {[]}
		 */
		JPM.groups = function (query, condition, groups, field, intervalMin, startTime, endTime, top, limit) {
			var fields = field.split(/\s*,\s*/);
			var orderId = -1;
			var fieldStr = $.map(fields, function (field, index) {
				var matches = field.match(/^([^\s]*)(\s+.*)?$/);
				if(matches[2]) {
					orderId = index;
				}
				return matches[1];
			}).join(", ");

			var config = {
				query: query,
				condition: JPM.condition(condition),
				startTime: Time.format(startTime),
				endTime: Time.format(endTime),
				groups: toFields(groups),
				field: fieldStr,
				order: orderId === -1 ? "" : ".{" + fields[orderId] + "}",
				top: top ? "&top=" + top : "",
				intervalMin: intervalMin,
				limit: limit || 100000
			};

			var metrics_url = common.template(intervalMin ? getQuery("GROUPS_INTERVAL") : getQuery("GROUPS"), config);
			var _list = wrapList(JPM.get(metrics_url));
			_list._aggInfo = {
				groups: groups,
				startTime: Time(startTime).valueOf(),
				interval: intervalMin * 60 * 1000
			};
			_list._promise.then(function () {
				if(top) _list.reverse();
			});
			return _list;
		};

		/**
		 * Fetch eagle query list
		 * @param {string} query
		 * @param {{}?} condition
		 * @param {(string|number|{})?} startTime
		 * @param {(string|number|{})?} endTime
		 * @param {[]?} fields
		 * @param {number?} limit
		 * @return {[]}
		 */
		JPM.list = function (query, condition, startTime, endTime, fields, limit) {
			var config = {
				query: query,
				condition: JPM.condition(condition),
				startTime: Time.format(startTime),
				endTime: Time.format(endTime),
				fields: toFields(fields),
				limit: limit || 10000
			};

			return wrapList(JPM.get(common.template(getQuery("LIST"), config)));
		};

		/**
		 * Fetch job list
		 * @param condition
		 * @param startTime
		 * @param endTime
		 * @param {[]?} fields
		 * @param {number?} limit
		 * @return {[]}
		 */
		JPM.jobList = function (condition, startTime, endTime, fields, limit) {
			var config = {
				condition: JPM.condition(condition),
				startTime: Time.format(startTime),
				endTime: Time.format(endTime),
				fields: toFields(fields),
				limit: limit || 10000
			};

			var jobList_url = common.template(getQuery("JOB_LIST"), config);
			return wrapList(JPM.get(jobList_url));
		};

		/**
		 * Fetch job metric list
		 * @param condition
		 * @param metric
		 * @param startTime
		 * @param endTime
		 * @param {number?} limit
		 * @return {[]}
		 */
		JPM.metrics = function (condition, metric, startTime, endTime, limit) {
			var config = {
				condition: JPM.condition(condition),
				startTime: Time.format(startTime),
				endTime: Time.format(endTime),
				metric: metric,
				limit: limit || 10000
			};

			var metrics_url = common.template(getQuery("METRICS"), config);
			var _list = wrapList(JPM.get(metrics_url));
			_list._promise.then(function () {
				_list.reverse();
			});
			return _list;
		};

		/**
		 * Fetch job metric list
		 * @param {{}} condition
		 * @param {string} metric
		 * @param {[]} groups
		 * @param {string} field
		 * @param {number|null|false} intervalMin
		 * @param startTime
		 * @param endTime
		 * @param {number?} top
		 * @param {number?} limit
		 * @return {[]}
		 */
		JPM.aggMetrics = function (condition, metric, groups, field, intervalMin, startTime, endTime, top, limit) {
			var fields = field.split(/\s*,\s*/);
			var orderId = -1;
			var fieldStr = $.map(fields, function (field, index) {
				var matches = field.match(/^([^\s]*)(\s+.*)?$/);
				if(matches[2]) {
					orderId = index;
				}
				return matches[1];
			}).join(", ");

			var config = {
				condition: JPM.condition(condition),
				startTime: Time.format(startTime),
				endTime: Time.format(endTime),
				metric: metric,
				groups: toFields(groups),
				field: fieldStr,
				order: orderId === -1 ? "" : ".{" + fields[orderId] + "}",
				top: top ? "&top=" + top : "",
				intervalMin: intervalMin,
				limit: limit || 100000
			};

			var metrics_url = common.template(intervalMin ? getQuery("METRICS_INTERVAL") : getQuery("METRICS_AGG"), config);
			var _list = wrapList(JPM.get(metrics_url));
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

		JPM.aggMetricsToEntities = function (list, flatten) {
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
							tags: tags
						};
					});

					if(flatten) {
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

		/**
		 * Fetch job duration distribution
		 * @param {string} site
		 * @param {string} jobType
		 * @param {string} times
		 * @param {{}} startTime
		 * @param {{}} endTime
		 */
		JPM.jobDistribution = function (site, jobType, times, startTime, endTime) {
			var url = common.template(getQuery("JOB_STATISTIC"), {
				site: site,
				jobType: jobType,
				times: times,
				startTime: Time.format(startTime),
				endTime: Time.format(endTime)
			});
			return JPM.get(url);
		};

		JPM.taskDistribution = function (site, jobId, times, top) {
			var url = common.template(getQuery("TASK_STATISTIC"), {
				site: site,
				jobId: jobId,
				times: times,
				top: top || 10
			});
			return JPM.get(url);
		};

		/**
		 * Get job list by sam jobDefId
		 * @param {string} site
		 * @param {string|undefined?} jobDefId
		 * @param {string|undefined?} jobId
		 * @return {[]}
		 */
		JPM.findMRJobs = function (site, jobDefId, jobId) {
			return wrapList(JPM.get(getQuery("MR_JOBS"), {
				site: site,
				jobDefId: jobDefId,
				jobId: jobId
			}));
		};

		/**
		 * Convert Entity list data to Chart supported series
		 * @param name
		 * @param metrics
		 * @param {{}|boolean?} rawData
		 * @param {{}?} option
		 * @return {{name: *, symbol: string, type: string, data: *}}
		 */
		JPM.metricsToSeries = function(name, metrics, rawData, option) {
			if(arguments.length === 3 && typeof rawData === "object") {
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
				symbol: 'none',
				type: "line",
				data: data
			}, option || {});
		};

		JPM.metricsToInterval = function (metricList, interval) {
			if(metricList.length === 0) return [];

			var list = $.map(metricList, function (metric) {
				var timestamp = Math.floor(metric.timestamp / interval) * interval;
				var remainderPtg = (metric.timestamp % interval) / interval;
				return {
					timestamp: remainderPtg < 0.5 ? timestamp : timestamp + interval,
					value: [metric.value[0]]
				};
			});

			var resultList = [list[0]];
			for(var i = 1 ; i < list.length ; i += 1) {
				var start = list[i - 1];
				var end = list[i];

				var distance = (end.timestamp - start.timestamp);
				if(distance > 0) {
					var steps = distance / interval;
					var des = (end.value[0] - start.value[0]) / steps;
					for (var j = 1; j <= steps; j += 1) {
						resultList.push({
							timestamp: start.timestamp + j * interval,
							value: [start.value[0] + des * j]
						});
					}
				}
			}
			return resultList;
		};

		JPM.getStateClass = function (state) {
			switch ((state || "").toUpperCase()) {
				case "NEW":
				case "NEW_SAVING":
				case "SUBMITTED":
				case "ACCEPTED":
					return "warning";
				case "RUNNING":
					return "info";
				case "SUCCESS":
				case "SUCCEEDED":
					return "success";
				case "FINISHED":
					return "primary";
				case "FAILED":
					return "danger";
			}
			return "default";
		};

		return JPM;
	});

	jpmApp.requireCSS("style/index.css");
	jpmApp.require("widget/jobStatistic.js");
	jpmApp.require("ctrl/overviewCtrl.js");
	jpmApp.require("ctrl/statisticCtrl.js");
	jpmApp.require("ctrl/listCtrl.js");
	jpmApp.require("ctrl/detailCtrl.js");
	jpmApp.require("ctrl/jobTaskCtrl.js");
	jpmApp.require("ctrl/compareCtrl.js");
})();
