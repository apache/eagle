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
		url: "/jpm/list",
		site: true,
		templateUrl: "partials/job/list.html",
		controller: "listCtrl"
	}).route("jpmDetail", {
		url: "/jpm/detail/:jobId",
		site: true,
		templateUrl: "partials/job/detail.html",
		controller: "detailCtrl"
	});

	jpmApp.portal({name: "JPM", icon: "home", path: "jpm/list"}, true);

	jpmApp.service("JPM", function ($q, $http, Time) {
		// TODO: mock auth
		var _hash = btoa('eagle:secret');

		// TODO: timestamp support
		var QUERY_LIST = 'http://phxapdes0005.stratus.phx.ebay.com:8080/eagle-service/rest/entities?query=${query}[${condition}]{${fields}}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';
		var QUERY_METRICS = 'http://phxapdes0005.stratus.phx.ebay.com:8080/eagle-service/rest/entities?query=GenericMetricService[${condition}]{*}&metricName=${metric}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';

		var JPM = {};

		JPM.get = function (url) {
			$http.defaults.withCredentials = true;
			var promise = $http({
				url: url,
				method: "GET",
				headers: {'Authorization': "Basic " + _hash}
			}).then(function (res) {
				return res.data.obj;
			});
			$http.defaults.withCredentials = false;
			return promise;
		};

		JPM.condition = function (condition) {
			return $.map(condition, function (value, key) {
				return "@" + key + '="' + value + '"'
			}).join(" AND ");
		};

		/**
		 * Fetch eagle query list
		 * @param query
		 * @param condition
		 * @param startTime
		 * @param endTime
		 * @param {[]?} fields
		 * @param {number?} limit
		 * @return {[]}
		 */
		JPM.list = function (query, condition, startTime, endTime, fields, limit) {
			var _list = [];
			_list._done = false;

			var config = {
				query: query,
				condition: JPM.condition(condition),
				startTime: moment(startTime).format(Time.FORMAT),
				endTime: moment(endTime).format(Time.FORMAT),
				fields: fields ? $.map(fields, function (field) {
					return "@" + field;
				}).join(",") : "*",
				limit: limit || 10000
			};

			_list._promise = JPM.get(common.template(QUERY_LIST, config));
			_list._promise.then(function (data) {
				_list.splice(0);
				Array.prototype.push.apply(_list, data);
				_list._done = true;
			});
			return _list;
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
			var _list = [];
			_list._done = false;

			var runningList = JPM.list("RunningJobExecutionService", condition, startTime, endTime, fields, limit);
			var historyList = JPM.list("JobExecutionService", condition, startTime, endTime, fields, limit);

			runningList._promise.then(function (data) {
				_list.splice(0);
				Array.prototype.push.apply(_list, data);
			});

			_list._promise = $q.all([runningList._promise, historyList._promise]).then(function (args) {
				_list.splice(0);
				Array.prototype.push.apply(_list, args[0]);
				Array.prototype.push.apply(_list, args[1]);
				_list._done = true;

				return _list;
			});

			return _list;
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
				startTime: moment(startTime).format(Time.FORMAT),
				endTime: moment(endTime).format(Time.FORMAT),
				metric: metric,
				limit: limit || 10000
			};

			var _list = [];
			var metrics_url = common.template(QUERY_METRICS, config);
			_list._done = false;
			_list._promise = JPM.get(metrics_url);
			_list._promise.then(function (data) {
				_list.splice(0);
				_list._done = true;
				Array.prototype.push.apply(_list, data);
				_list.reverse();
			});

			return _list;
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

	jpmApp.require("ctrl/listCtrl.js");
	jpmApp.require("ctrl/detailCtrl.js");
})();
