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

	jpmApp.service("JPM", function ($http, Time) {
		// TODO: mock auth
		var _hash = btoa('eagle:secret');

		// TODO: timestamp support
		var QUERY_LIST = 'http://phxapdes0005.stratus.phx.ebay.com:8080/eagle-service/rest/list?query=JobExecutionService[${condition}]{${fields}}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';
		//var QUERY_LIST = 'http://phxapdes0005.stratus.phx.ebay.com:8080/eagle-service/rest/list?query=RunningJobExecutionService[${condition}]{${fields}}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';

		var JPM = {};

		/**
		 * Fetch job list
		 * @param site
		 * @param startTime
		 * @param endTime
		 * @return {[]}
		 */
		JPM.list = function (condition, startTime, endTime, fields, limit) {
			var _list = [];

			var url = common.template(QUERY_LIST, {
				condition: $.map(condition, function (value, key) {
					return "@" + key + '="' + value + '"'
				}).join(" AND "),
				startTime: moment(startTime).format(Time.FORMAT),
				endTime: moment(endTime).format(Time.FORMAT),
				fields: fields ? $.map(fields, function (field) {
					return "@" + field;
				}).join(",") : "*",
				limit: limit || 10000
			});

			$http.defaults.withCredentials = true;
			_list._done = false;
			_list._promise = $http({
				url: url,
				method: "GET",
				headers: {
					'Authorization': "Basic " + _hash
				}
			});
			$http.defaults.withCredentials = false;

			/**
			 * @param {{}} res
			 * @param {{}} res.data
			 * @param {Array} res.data.obj
			 */
			_list._promise.then(function (res) {
				_list._done = true;
				_list.splice(0);
				Array.prototype.push.apply(_list, res.data.obj);
				return _list
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
