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

	serviceModule.service('CompatibleEntity', function($authHttp, Time) {
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
			fields = $.isArray(fields) ? fields : (
				fields && fields !== '*' ? fields.split(/\s*,\s*/) : []
			);
			if(fields.length === 0) return {
				fieldStr: '*',
				fields: [],
				order: ''
			};

			var fieldList = [];
			var fieldEntities = [];
			var orderId = -1;
			var fieldStr = $.map(fields, function (field, index) {
				var matches = field.match(/^([^\s]*)(\s+.*)?$/);
				var fieldName = matches[1];
				if(matches[2]) {
					orderId = index;
				}
				var fieldMatches = fieldName.match(/^(.+)\((.+)\)$/);
				if (fieldName === 'count') {
					fieldEntities.push({
						method: 'count',
						name: 'count',
					});
					fieldList.push('count');
					return fieldName;
				}
				else if (fieldMatches) {
					fieldEntities.push({
						method: fieldMatches[1],
						name: fieldMatches[2],
					});
					fieldList.push(fieldName);
					return fieldName;
				} else {
					fieldEntities.push({
						method: '',
						name: fieldName,
					});
					fieldList.push('@' + fieldName);
					return '@' + fieldName;
				}
			}).join(", ");

			return {
				fieldStr: fieldStr,
				fields: fieldList,
				fieldEntities: fieldEntities,
				order: orderId === -1 ? "" : ".{" + fields[orderId] + "}"
			};
		}

		CompatibleEntity.QUERY_LIST = '/rest/entities?query=${query}[${condition}]{${fields}}&pageSize=${limit}';
		CompatibleEntity.QUERY_GROUPS = '/rest/entities?query=${query}[${condition}]<${groups}>{${fields}}&pageSize=${limit}';
		CompatibleEntity.QUERY_GROUPS_INTERVAL = '/rest/entities?query=${query}[${condition}]<${groups}>{${fields}}${order}${top}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}&intervalmin=${intervalMin}&timeSeries=true';
		CompatibleEntity.QUERY_METRICS_INTERVAL = '/rest/entities?query=GenericMetricService[${condition}]<${groups}>{${fields}}${order}${top}&metricName=${metric}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}&intervalmin=${intervalMin}&timeSeries=true';

		CompatibleEntity.query = function (queryName, param) {
			var list = [];
			param = param || {};

			list._refresh = function () {
				var myParam = $.extend({
					limit: 10000,
				}, param || {}, {
					condition: parseCondition(param.condition),
					fields: parseFields(param.fields).fieldStr,
					startTime: Time.format(param.startTime),
					endTime: Time.format(param.endTime)
				});

				var url_tpl = CompatibleEntity["QUERY_" + queryName];
				if(param.startTime || param.endTime) {
					url_tpl += '&startTime=${startTime}&endTime=${endTime}';
				}

				var url = common.template(url_tpl, myParam);
				return wrapList(list, $authHttp.get(_host + url));
			};

			return list._refresh();
		};

		/**
		 *
		 * @param {string} queryName
		 * @param {string} param.query
		 * @param {{}?} param.condition
		 * @param {string|[]} param.groups
		 * @param {string|[]} param.fields
		 * @param {number?} param.limit
		 */
		CompatibleEntity.groups = function (param) {
			return CompatibleEntity.query('GROUPS', $.extend({}, param, {
				groups: parseFields(param.groups).fields,
			}));
		};

		/**
		 *
		 * @param {string} queryName
		 * @param {{}?} param.condition
		 * @param {string|[]} param.groups
		 * @param {string|[]} param.fields
		 * @param {string} param.metric
		 * @param {{}} param.startTime
		 * @param {{}} param.endTime
		 * @param {number?} param.top
		 * @param {number?} param.limit
		 * @param {number?} param.intervalMin
		 */
		CompatibleEntity.timeSeries = function (param) {
			param = param || {};
			var fields = parseFields(param.fields);
			var startTime = new Time(param.startTime || 'startTime');
			var endTime = new Time(param.endTime || 'endTime');

			var startTimestamp = startTime.valueOf();
			var intervalMin = param.intervalMin ? param.intervalMin : Time.diffInterval(startTime, endTime) / 1000 / 60;
			var interval = intervalMin * 1000 * 60;

			var innerList = CompatibleEntity.query(param.metric ? 'METRICS_INTERVAL' : 'GROUPS_INTERVAL', $.extend({}, param, {
				groups: parseFields(param.groups).fields,
				order: fields.order,
				top: param.top ? "&top=" + param.top : "",
				startTime: startTime,
				endTime: endTime,
				intervalMin: intervalMin,
			}));

			var list = [];
			list._done = false;
			list._promise = innerList._promise;

			innerList._promise.then(function () {
				var displayKey = innerList.length > 1;

				// Generate series
				var series = $.map(innerList, function (group) {
					return $.map(fields.fieldEntities, function (entity, i) {
						var data = group.value[i];
						return {
							name: (displayKey ? group.key.join(',') + '-' : '') + (entity.method || entity.name),
							type: 'line',
							showSymbol: false,
							data: $.map(data, function (value, index) {
								return {
									x: startTimestamp + interval * index,
									y: value
								};
							}),

							group: group,
						};
					});
				});

				list.splice(0);
				Array.prototype.push.apply(list, series);
				list._done = true;
			});

			return list;
		};

		return CompatibleEntity;
	});
})();
