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

	var scope = {};
	if(typeof window != 'undefined') {
		scope = window;
	} else if(typeof self != 'undefined') {
		scope = self;
	}
	var common = scope.common = {};

	// ============================ Common ============================
	common.template = function (str, list) {
		$.each(list, function(key, value) {
			var _regex = new RegExp("\\$\\{" + key + "\\}", "g");
			str = str.replace(_regex, value);
		});
		return str;
	};

	common.getValueByPath = function (unit, path, defaultValue) {
		if(unit === null || unit === undefined) throw "Unit can't be empty!";
		if(path === "" || path === null || path === undefined) return unit;

		if(typeof path === "string") {
			path = path.replace(/\[(\d+)\]/g, ".$1").replace(/^\./, "").split(/\./);
		}
		for(var i = 0 ; i < path.length ; i += 1) {
			unit = unit[path[i]];
			if(unit === null || unit === undefined) {
				unit = null;
				break;
			}
		}
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

	function merge(obj1, obj2) {
		$.each(obj2, function (key, value) {
			var oriValue = obj1[key];

			if(typeof oriValue === "object" && typeof value === "object" && !common.isEmpty(value)) {
				merge(oriValue, value);
			} else {
				obj1[key] = value;
			}
		});
	}

	common.merge = function (mergedObj) {
		for(var i = 1 ; i < arguments.length ; i += 1) {
			var obj = arguments[i];
			merge(mergedObj, obj);
		}

		return mergedObj;
	};

	// ============================ String ============================
	common.string = {};
	common.string.safeText = function (str) {
		return str
			.replace(/&/g, '&amp;')
			.replace(/</g, '&lt;')
			.replace(/>/g, '&gt;');
	};

	common.string.preFill = function (str, key, len) {
		str = str + "";
		len = len || 2;
		while(str.length < len) {
			str = key + str;
		}
		return str;
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

	// ============================ Number ============================
	common.number = {};

	common.number.isNumber = function (num) {
		return typeof num === "number" && !isNaN(num);
	};

	common.number.toFixed = function (num, fixed) {
		if(!common.number.isNumber(num)) return "-";
		num = Number(num);
		return num.toFixed(fixed || 0);
	};

	common.number.format = function (num, fixed) {
		if(!common.number.isNumber(num)) return "-";
		return common.number.toFixed(num, fixed).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
	};

	common.number.abbr = function (number, isByte, decPlaces) {
		decPlaces = decPlaces || 2;
		decPlaces = Math.pow(10, decPlaces);
		const abbrev = isByte ? ['KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'] : ['K', 'M', 'B', 'T', 'Q'];
		const base = isByte ? 1024 : 1000;
		const sign = number < 0 ? -1 : 1;
		var unit = '';
		number = Math.abs(number);

		for(var i = abbrev.length - 1; i >= 0; i--) {
			const size = Math.pow(base, i + 1);
			if(size <= number) {
				number = Math.round(number * decPlaces / size) / decPlaces;
				if((number === base) && (i < abbrev.length - 1)) {
					number = 1;
					i++;
				}
				unit = abbrev[i];
				break;
			}
		}
		unit = unit ? unit : "";
		return number * sign + unit;
	}

	common.number.compare = function (num1, num2) {
		if(!common.number.isNumber(num1) || !common.number.isNumber(num2)) return "-";
		if(num1 === 0) return 'N/A';
		return (num2 - num1) / num1;
	};

	common.number.inRange = function (rangList, num) {
		for(var i = 0 ; i < rangList.length - 1 ; i += 1) {
			var start = rangList[i];
			var end = rangList[i + 1];
			if(start <= num && num < end) return i;
		}
		return rangList.length - 1;
	};
})();
