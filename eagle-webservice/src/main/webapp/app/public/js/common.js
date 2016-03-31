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

var common = {};

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

// ====================== Format ======================
common.format = {};

/*
 * Format date to string. Support number, string, Date instance. Will auto convert time zone offset(Moment instance will keep time zone).
 */
common.format.date = function(val, type) {
	if(val === undefined || val === null) return "";

	if(typeof val === "number" || typeof val === "string" || val instanceof Date) {
		val = app.time.offset(val);
	}
	switch(type) {
	case 'date':
		return val.format("YYYY-MM-DD");
	case 'time':
		return val.format("HH:mm:ss");
	case 'datetime':
		return val.format("YYYY-MM-DD HH:mm:ss");
	case 'mixed':
		return val.format("YYYY-MM-DD HH:mm");
	default:
		return val.format("YYYY-MM-DD HH:mm:ss") + (val.utcOffset() === 0 ? '[UTC]' : '');
	}
};

// ===================== Property =====================
common.properties = {};

common.properties.parse = function (str, defaultValue) {
	var regex = /\s*([\w\.]+)\s*=\s*(.*?)\s*([\r\n]+|$)/g;
	var match, props = {};
	var hasValue = false;
	while((match = regex.exec(str)) !== null) {
		props[match[1]] = match[2];
		hasValue = true;
	}
	props = hasValue ? props : defaultValue;
	props.getValueByPath = function (path) {
		if(props[path] !== undefined) return props[path];
		var subProps = {};
		var prefixPath = path + ".";
		$.each(props, function (key, value) {
			if(typeof value === "string" && key.indexOf(prefixPath) === 0) {
				subProps[key.replace(prefixPath, "")] = value;
			}
		});
		return subProps;
	};

	return props;
};

common.properties.check = function (str) {
	var pass = true;
	var regex = /^\s*[\w\.]+\s*=(.*)$/;
	$.each((str || "").trim().split(/[\r\n\s]+/g), function (i, line) {
		if(!regex.test(line)) {
			pass = false;
			return false;
		}
	});
	return pass;
};

// ====================== Array =======================
common.array = {};

common.array.sum = function(list, path) {
	var _sum = 0;
	if(list) {
		$.each(list, function(i, unit) {
			var _val = common.getValueByPath(unit, path);
			if(typeof _val === "number") {
				_sum += _val;
			}
		});
	}
	return _sum;
};

common.array.max = function(list, path) {
	var _max = null;
	if(list) {
		$.each(list, function(i, unit) {
			var _val = common.getValueByPath(unit, path);
			if(typeof _val === "number" && (_max === null || _max < _val)) {
				_max = _val;
			}
		});
	}
	return _max;
};

common.array.bottom = function(list, path, count) {
	var _list = list.slice();

	_list.sort(function(a, b) {
		var _a = common.getValueByPath(a, path, null);
		var _b = common.getValueByPath(b, path, null);

		if(_a === _b) return 0;
		if(_a < _b || _a === null) {
			return -1;
		} else {
			return 1;
		}
	});
	return !count ? _list : _list.slice(0, count);
};
common.array.top = function(list, path, count) {
	var _list = common.array.bottom(list, path);
	_list.reverse();
	return !count ? _list : _list.slice(0, count);
};

common.array.find = function(val, list, path, findAll) {
	path = path || "";
	var _list = $.grep(list, function(unit) {
		return val === common.getValueByPath(unit, path);
	});
	return findAll ? _list : (_list.length === 0 ? null : _list[0]);
};

common.array.filter = function(val, list, path) {
	return common.array.find(val, list, path, true);
};

common.array.count = function(list, val, path) {
	if(arguments.length === 1) {
		return list.length;
	} else {
		return common.array.find(val, list, path, true).length;
	}
};

common.array.remove = function(val, list) {
	for(var i = 0 ; i < list.length ; i += 1) {
		if(list[i] === val) {
			list.splice(i, 1);
			i -= 1;
		}
	}
};

common.array.insert = function(val, list, index) {
	list.splice(index, 0, val);
};

common.array.moveOffset = function(item, list, offset) {
	var _index = $.inArray(item, list);
	var _tgtPos = _index + offset;
	if(_tgtPos < 0 || _tgtPos >= list.length) return;

	common.array.remove(item, list);
	common.array.insert(item, list, _index + offset);
};

// ======================= Map ========================
common.map = {};

common.map.toArray = function(map) {
	return $.map(map, function(unit) {
		return unit;
	});
};

// ======================= Math =======================
common.math = {};

common.math.distance = function(x1,y1,x2,y2) {
	var a = x1 - x2;
	var b = y1 - y2;
	return Math.sqrt(a * a + b * b);
};
