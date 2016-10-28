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

(function() {
	'use strict';

	var serviceModule = angular.module('eagle.service');

	var _host = "";
	if(localStorage) {
		_host = localStorage.getItem("host") || "";
	}

	serviceModule.service('Entity', function($http, $q) {
		function Entity() {}

		function wrapList(list, promise) {
			list._done = false;
			list._promise = promise.then(function (res) {
				var data = res.data;
				list.splice(0);
				Array.prototype.push.apply(list, data.data);
				list._done = true;

				return res;
			});
			return withThen(list);
		}

		function withThen(list) {
			list._then = list._promise.then.bind(list._promise);
			return list;
		}

		// Dev usage. Set rest api source
		Entity._host = function (host) {
			console.warn("This function only used for development usage.");
			if(host) {
				_host = host.replace(/[\\\/]+$/, "");
				if(localStorage) {
					localStorage.setItem("host", _host);
				}
			}
			return _host;
		};

		Entity.query = function (url, param) {
			var list = [];
			list._refresh = function () {
				var config = {};
				if(param) config.params = param;
				return wrapList(list, $http.get(_host + "/rest/" + url, config));
			};

			return list._refresh();
		};

		Entity.create = Entity.post = function (url, entity) {
			var list = [];
			return wrapList(list, $http({
				method: 'POST',
				url: _host + "/rest/" + url,
				headers: {
					"Content-Type": "application/json"
				},
				data: entity
			}));
		};

		Entity.delete = function (url, uuid) {
			var list = [];
			return wrapList(list, $http({
				method: 'DELETE',
				url: _host + "/rest/" + url,
				headers: {
					"Content-Type": "application/json"
				},
				data: {uuid: uuid}
			}));
		};

		/**
		 * Merge 2 array into one. Will return origin list before target list is ready. Then fill with target list.
		 * @param oriList
		 * @param tgtList
		 * @return {[]}
		 */
		Entity.merge = function (oriList, tgtList) {
			oriList = oriList || [];

			var list = [].concat(oriList);
			list._done = tgtList._done;
			list._refresh = tgtList._refresh;
			list._promise = tgtList._promise;

			list._promise.then(function () {
				list.splice(0);
				Array.prototype.push.apply(list, tgtList);
				list._done = true;
			});

			list = withThen(list);

			return list;
		};

		// TODO: metadata will be removed
		Entity.queryMetadata = function (url, param) {
			var metaList = Entity.query('metadata/' +  url, param);
			var _refresh = metaList._refresh;

			function process() {
				metaList._then(function (res) {
					var data = res.data;
					if(!$.isArray(data)) {
						data = [data];
					}

					metaList.splice(0);
					Array.prototype.push.apply(metaList, data);
				});
			}

			metaList._refresh = function () {
				_refresh();
				process();
			};

			process();

			return metaList;
		};

		Entity.deleteMetadata = function (url) {
			return {
				_promise: $http.delete(_host + "/rest/metadata/" + url).then(function (res) {
					console.log(res);
				})
			};
		};

		return Entity;
	});
}());
