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

	serviceModule.service('CompatibleEntity', function($http, Time) {
		function CompatibleEntity() {}

		function wrapList(list, promise) {
			list._done = false;
			list._promise = promise.then(function (res) {
				var data = res.data;
				list.splice(0);
				Array.prototype.push.apply(list, data.obj);
				list._done = true;

				return res;
			});
			return withThen(list);
		}

		function withThen(list) {
			list._then = list._promise.then.bind(list._promise);
			return list;
		}

		function parseCondition(condition) {
			condition = condition || {};
			return $.map(condition, function (value, key) {
				return '@' + key + '="' + value + '"';
			}).join(' AND ');
		}

		function parseFields(fields) {
			fields = fields || [];
			if(fields.length === 0) return '*';

			return $.map(fields, function (field) {
				return '@' + field;
			}).join(',');
		}

		CompatibleEntity.QUERY_LIST = '/rest/entities?query=${query}[${condition}]{${fields}}&pageSize=${size}&startTime=${startTime}&endTime=${endTime}';

		CompatibleEntity.query = function (queryName, param) {
			var list = [];
			param = param || {};

			list._refresh = function () {
				var myParam = $.extend({
					size: 10000,
				}, param || {}, {
					condition: parseCondition(param.condition),
					fields: parseFields(param.fields),
					startTime: Time.format(param.startTime),
					endTime: Time.format(param.endTime)
				});

				var url = common.template(CompatibleEntity["QUERY_" + queryName], myParam);
				return wrapList(list, $http.get(_host + url));
			};

			return list._refresh();
		};

		return CompatibleEntity;
	});
})();
