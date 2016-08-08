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

	jpmApp.route({
		url: "/jpm/list",
		site: true,
		templateUrl: "partials/job/list.html",
		controller: "listCtrl"
	});

	jpmApp.portal({name: "JPM", icon: "home", path: "jpm/list"}, true);

	jpmApp.service("JPM", function ($http, Time) {
		// TODO: mock auth
		$http.defaults.withCredentials = true;
		var _hash = btoa('eagle:secret');

		// TODO: timestamp support
		var QUERY_LIST = 'http://phxapdes0005.stratus.phx.ebay.com:8080/eagle-service/rest/list?query=JobExecutionService[@site="${site}"]{${fields}}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';
		//var QUERY_LIST = 'http://phxapdes0005.stratus.phx.ebay.com:8080/eagle-service/rest/list?query=RunningJobExecutionService[@site="${site}"]{${fields}}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';

		var JPM = {};

		/**
		 * Fetch job list
		 * @param site
		 * @param startTime
		 * @param endTime
		 * @return {[]}
		 */
		JPM.list = function (site, startTime, endTime, fields, limit) {
			var _list = [];

			var url = common.template(QUERY_LIST, {
				site: site,
				startTime: moment(startTime).format(Time.FORMAT),
				endTime: moment(endTime).format(Time.FORMAT),
				fields: fields ? $.map(fields, function (field) {
					return "@" + field;
				}).join(",") : "*",
				limit: limit || 10000
			});

			_list._done = false;
			_list._promise = $http({
				url: url,
				method: "GET",
				headers: {
					'Authorization': "Basic " + _hash
				}
			});
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

		return JPM;
	});

	jpmApp.require("ctrl/listCtrl.js");
	jpmApp.require("ctrl/detailCtrl.js");
})();
