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

	function hasContentByPathList(object, content, pathList) {
		for(var i = 0 ; i < pathList.length ; i += 1) {
			var path = pathList[i];
			var value = common.getValueByPath(object, path);
			if((value + "").toUpperCase().indexOf(content) >= 0) {
				return true;
			}
		}
		return false;
	}

	function hasContent(object, content, depth) {
		var i, keys;

		depth = depth || 1;
		if(!content) return true;
		if(depth > 10) return false;

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

	__sortTable_generateFilteredList = function(list, search, order, orderAsc, searchPathList) {
		var i, _list;
		var _search = (search + "").toUpperCase();

		if (search) {
			_list = [];
			if(searchPathList) {
				for(i = 0 ; i < list.length ; i += 1) {
					if(hasContentByPathList(list[i], _search, searchPathList)) _list.push(list[i]);
				}
			} else {
				for(i = 0 ; i < list.length ; i += 1) {
					if(hasContent(list[i], _search)) _list.push(list[i]);
				}
			}
		} else {
			_list = list;
		}

		if (order) {
			common.array.doSort(_list, order, orderAsc);
		}

		return _list;
	};
})();
