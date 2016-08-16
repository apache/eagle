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

var __sortTable_generateFilteredList;

(function () {
	'use strict';

	var isArray;
	if(typeof $ !== "undefined") {
		isArray = $.isArray;
	} else {
		isArray = Array.isArray;
	}

	function hasContent(object, content, depth) {
		var i, keys;

		depth = depth || 1;
		if(!content) return true;
		if(depth > 10) return false;

		content = (content + "").toUpperCase();

		if(object === undefined || object === null) {
			return false;
		} else if(isArray(object)) {
			for(i = 0 ; i < object.length ; i += 1) {
				if(hasContent(object[i], content, depth + 1)) return true;
			}
		} else if(typeof object === "object") {
			keys = Object.keys(object);
			for(i = 0 ; i < keys.length ; i += 1) {
				var value = object[keys[i]];
				if(hasContent(value, content, depth + 1)) return true;
			}
		} else {
			return (object + "").toUpperCase().indexOf(content) >= 0;
		}

		return false;
	}

	__sortTable_generateFilteredList = function(list, search, order, orderAsc) {
		'use strict';

		var _list;

		if (search) {
			_list = [];
			for(var i = 0 ; i < list.length ; i += 1) {
				if(hasContent(list[i], search)) _list.push(list[i]);
			}
		} else {
			_list = list;
		}

		if (order) {
			if (orderAsc) {
				_list.sort(function (obj1, obj2) {
					var val1 = common.getValueByPath(obj1, order);
					var val2 = common.getValueByPath(obj2, order);

					if (val1 === val2) {
						return 0;
					} else if (val1 === null || val1 === undefined || val1 < val2) {
						return -1;
					}
					return 1;
				});
			} else {
				_list.sort(function (obj1, obj2) {
					var val1 = common.getValueByPath(obj1, order);
					var val2 = common.getValueByPath(obj2, order);

					if (val1 === val2) {
						return 0;
					} else if (val1 === null || val1 === undefined || val1 < val2) {
						return 1;
					}
					return -1;
				});
			}
		}

		return _list;
	}
})();
