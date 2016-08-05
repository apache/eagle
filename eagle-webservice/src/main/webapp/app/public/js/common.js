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
	'use strict';

	var common = window.common = {};

	// ============================ Common ============================
	common.template = function (str, list) {
		$.each(list, function(key, value) {
			var _regex = new RegExp("\\$\\{" + key + "\\}", "g");
			str = str.replace(_regex, value);
		});
		return str;
	};

	common.getValueByPath = function (unit, path, defaultValue) {
		if(unit === null || unit === undefined) throw "Unit or path can't be empty!";
		if(path === "" || path === null || path === undefined) return unit;

		path = path.replace(/\[(\d+)\]/g, ".$1").replace(/^\./, "").split(/\./);
		$.each(path, function(i, path) {
			unit = unit[path];
			if(unit === null || unit === undefined) {
				unit = null;
				return false;
			}
		});
		if(unit === null && defaultValue !== undefined) {
			unit = defaultValue;
		}
		return unit;
	};

	common.setValueByPath = function(unit, path, value) {
		if(!unit || typeof path !== "string" || path === "") throw "Unit or path can't be empty!";

		var _inArray = false;
		var _end = 0;
		var _start = 0;
		var _unit = unit;

		function _nextPath(array) {
			var _key = path.slice(_start, _end);
			if(_inArray) {
				_key = _key.slice(0, -1);
			}
			if(!_unit[_key]) {
				if(array) {
					_unit[_key] = [];
				} else {
					_unit[_key] = {};
				}
			}
			_unit = _unit[_key];
		}

		for(; _end < path.length ; _end += 1) {
			if(path[_end] === ".") {
				_nextPath(false);
				_start = _end + 1;
				_inArray = false;
			} else if(path[_end] === "[") {
				_nextPath(true);
				_start = _end + 1;
				_inArray = true;
			}
		}

		_unit[path.slice(_start, _inArray ? -1 : _end)] = value;

		return unit;
	};

	common.parseJSON = function (str, defaultVal) {
		try {
			str = (str + "").trim();
			if(Number(str).toString() === str) throw "Number format";
			return JSON.parse(str);
		} catch(err) {
			if(defaultVal === undefined) {
				console.warn("Can't parse JSON: " + str);
			}
		}
		return defaultVal === undefined ? null : defaultVal;
	};

	common.stringify = function(json) {
		return JSON.stringify(json, function(key, value) {
			if(/^(_|\$)/.test(key)) return undefined;
			return value;
		});
	};

	common.isEmpty = function(val) {
		if($.isArray(val)) {
			return val.length === 0;
		} else {
			return val === null || val === undefined;
		}
	};

	common.extend = function(target, origin) {
		$.each(origin, function(key, value) {
			if(/^(_|\$)/.test(key)) return;

			target[key] = value;
		});
		return target;
	};

	// ============================ Array =============================
	common.array = {};

	common.array.find = function(val, list, path, findAll, caseSensitive) {
		path = path || "";
		val = caseSensitive === false ? (val || "").toUpperCase() : val;

		var _list = $.grep(list, function(unit) {
			if(caseSensitive === false) {
				return val === (common.getValueByPath(unit, path) || "").toUpperCase();
			} else {
				return val === common.getValueByPath(unit, path);
			}
		});
		return findAll ? _list : (_list.length === 0 ? null : _list[0]);
	};

	common.array.minus = function (list1, list2, path1, path2) {
		if(arguments.length === 3) path2 = path1;
		var list = [];
		$.each(list1, function (i, item) {
			var val1 = common.getValueByPath(item, path1);
			if(!common.array.find(val1, list2, path2)) {
				list.push(item);
			}
		});
		return list;
	};

	// =========================== Deferred ===========================
	common.deferred = {};


	common.deferred.all = function (deferredList) {
		var deferred = $.Deferred();
		var successList = [];
		var failureList = [];
		var hasFailure = false;
		var rest = deferredList.length;
		function doCheck() {
			rest -= 1;
			if(rest === 0) {
				if(hasFailure) {
					deferred.reject(failureList);
				} else {
					deferred.resolve(successList);
				}
			}
		}

		$.each(deferredList, function (i, deferred) {
			if(deferred && deferred.then) {
				deferred.then(function (data) {
					successList[i] = data;
				}, function (data) {
					failureList[i] = data;
					hasFailure = true;
				}).always(doCheck);
			} else {
				successList[i] = deferred;
				doCheck();
			}
		});

		return deferred;
	};
})();
