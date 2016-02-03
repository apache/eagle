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
	serviceModule.service('Entities', function($http, $q, $rootScope, $location, Authorization) {
		var pkg;

		// Query
		function _query(name, kvs) {
			kvs = kvs || {};
			var _list = [];
			var _condition = kvs._condition || {};
			var _addtionalCondition = _condition.additionalCondition || {};
			var _startTime, _endTime;
			var _startTimeStr, _endTimeStr;

			// Initial
			// > Condition
			delete kvs._condition;
			if(_condition) {
				kvs.condition = _condition.condition;
			}

			// > Values
			if(!kvs.values) {
				kvs.values = "*";
			} else if($.isArray(kvs.values)) {
				kvs.values = $.map(kvs.values, function(field) {
					return (field[0] === "@" ? '' : '@') + field;
				}).join(",");
			}

			var _url = app.getURL(name, kvs);

			// Fill special parameters
			// > Query by time duration
			if(_addtionalCondition._duration) {
				_endTime = app.time.now();
				_startTime = _endTime.clone().subtract(_addtionalCondition._duration, "ms");

				// Debug usage. Extend more time duration for end time
				if(_addtionalCondition.__ETD) {
					_endTime.add(_addtionalCondition.__ETD, "ms");
				}

				_addtionalCondition._startTime = _startTime;
				_addtionalCondition._endTime = _endTime;

				_startTimeStr = _startTime.format("YYYY-MM-DD HH:mm:ss");
				_endTimeStr = _endTime.clone().add(1, "s").format("YYYY-MM-DD HH:mm:ss");

				_url += "&startTime=" + _startTimeStr + "&endTime=" + _endTimeStr;
			} else if(_addtionalCondition._startTime && _addtionalCondition._endTime) {
				_startTimeStr = _addtionalCondition._startTime.format("YYYY-MM-DD HH:mm:ss");
				_endTimeStr = _addtionalCondition._endTime.clone().add(1, "s").format("YYYY-MM-DD HH:mm:ss");

				_url += "&startTime=" + _startTimeStr + "&endTime=" + _endTimeStr;
			}

			// > Query contains metric name
			if(_addtionalCondition._metricName) {
				_url += "&metricName=" + _addtionalCondition._metricName;
			}

			// > Customize page size
			if(_addtionalCondition._pageSize) {
				_url = _url.replace(/pageSize=\d+/, "pageSize=" + _addtionalCondition._pageSize);
			}

			// AJAX
			var canceler = $q.defer();
			_list._promise = $http.get(_url, {timeout: canceler.promise}).success(function(data) {
				_list.push.apply(_list, data.obj);
			});
			_list._promise.abort = function() {
				canceler.resolve();
			};

			_list._promise.then(function() {}, function(data) {
				if(data.status === 403) {
					Authorization.needLogin();
				}
			});

			return _list;
		}
		function _post(url, entities) {
			var _list = [];
			_list._promise = $http({
				method: 'POST',
				url: url,
				headers: {
					"Content-Type": "application/json"
				},
				data: entities
			}).success(function(data) {
				_list.push.apply(_list, data.obj);
			});
			return _list;
		}
		function _delete(url) {
			var _list = [];
			_list._promise = $http({
				method: 'DELETE',
				url: url,
				headers: {
					"Content-Type": "application/json"
				}
			}).success(function(data) {
				_list.push.apply(_list, data.obj);
			});
			return _list;
		}
		function ParseCondition(condition) {
			var _this = this;
			_this.condition = "";
			_this.additionalCondition = {};

			if(typeof condition === "string") {
				_this.condition = condition;
			} else {
				_this.condition = $.map(condition, function(value, key) {
					if(!key.match(/^_/)) {
						if(value === undefined || value === null) {
							return '@' + key + '=~".*"';
						} else {
							return '@' + key + '="' + value + '"';
						}
					} else {
						_this.additionalCondition[key] = value;
						return null;
					}
				}).join(" AND ");
			}
			return _this;
		}

		pkg = {
			_query: _query,
			_post: _post,

			updateEntity: function(serviceName, entities, config) {
				var _url;
				config = config || {};
				if(!$.isArray(entities)) entities = [entities];

				// Post clone entities
				var _entities = $.map(entities, function(entity) {
					var _entity = {};

					// Clone variables
					$.each(entity, function(key) {
						// Skip inner variables
						if(!key.match(/^__/)) {
							_entity[key] = entity[key];
						}
					});

					// Add timestamp
					if(config.timestamp !== false) {
						if(config.createTime !== false && !_entity.createdTime) {
							_entity.createdTime = new moment().valueOf();
						}
						if(config.lastModifiedDate !== false) {
							_entity.lastModifiedDate = new moment().valueOf();
						}
					}

					return _entity;
				});

				// Check for url hook
				if(config.hook) {
					_url = app.getUpdateURL(serviceName);
				} else {
					_url = app.getURL("updateEntity", {serviceName: serviceName});
				}

				return _post(_url, _entities);
			},

			deleteEntity: function(serviceName, entities) {
				if (!$.isArray(entities)) entities = [entities];

				var _entities = $.map(entities, function (entity) {
					return typeof entity === "object" ? entity.encodedRowkey : entity;
				});
				return _post(app.getURL("deleteEntity", {serviceName: serviceName}), _entities);
			},
			deleteEntities: function(serviceName, condition) {
				return _delete(app.getURL("deleteEntities", {serviceName: serviceName, condition: new ParseCondition(condition).condition}));
			},
			delete: function(serviceName, kvs) {
				var _deleteURL = app.getDeleteURL(serviceName);
				return _delete(common.template(_deleteURL, kvs));
			},

			queryEntity: function(serviceName, encodedRowkey) {
				return _query("queryEntity", {serviceName: serviceName, encodedRowkey: encodedRowkey});
			},
			queryEntities: function(serviceName, condition, fields) {
				return _query("queryEntities", {serviceName: serviceName, _condition: new ParseCondition(condition), values: fields});
			},
			queryGroup: function(serviceName, condition, groupBy, fields) {
				return _query("queryGroup", {serviceName: serviceName, _condition: new ParseCondition(condition), groupBy: groupBy, values: fields});
			},
			querySeries: function(serviceName, condition, groupBy, fields, intervalmin) {
				var _cond = new ParseCondition(condition);
				var _list = _query("querySeries", {serviceName: serviceName, _condition: _cond, groupBy: groupBy, values: fields, intervalmin: intervalmin});
				_list._promise.success(function() {
					if(_list.length === 0) {
						_list._empty = true;
						_list._convert = true;

						for(var i = 0; i <= (_cond.additionalCondition._endTime.valueOf() - _cond.additionalCondition._startTime.valueOf()) / (1000 * 60 * intervalmin); i += 1) {
							_list.push(0);
						}
					} else if(_list.length === 1) {
						_list._convert = true;
						var _unit = _list.pop();
						_list.push.apply(_list, _unit.value[0]);
					}

					if(_list._convert) {
						var _current = _cond.additionalCondition._startTime.clone();
						$.each(_list, function(i, value) {
							_list[i] = {
								x: _current.valueOf(),
								y: value
							};
							_current.add(intervalmin, "m");
						});
					}
				});
				return _list;
			},

			query: function(path, params) {
				var _list = [];
				_list._promise = $http({
					method: 'GET',
					url: app.getURL("query") + path,
					params: params
				}).success(function(data) {
					_list.push.apply(_list, data.obj);
				});
				return _list;
			},

			dialog: function(data, callback) {
				if(data.success === false || (data.exception || "").trim()) {
					return $.dialog({
						title: "OPS",
						content: $("<pre>").html(data.exception)
					}, callback);
				}
				return false;
			}
		};
		return pkg;
	});
})();