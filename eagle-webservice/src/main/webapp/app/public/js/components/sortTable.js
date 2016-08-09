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

	var eagleComponents = angular.module('eagle.components');

	function hasContent(object, content, depth) {
		var i, keys;

		depth = depth || 1;
		if(!content) return true;
		if(depth > 10) return false;

		if(object === undefined || object === null) {
			return false;
		} else if($.isArray(object)) {
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
			return (object + "").indexOf(content) >= 0;
		}

		return false;
	}

	eagleComponents.directive('sortTable', function($compile) {
		return {
			restrict: 'AE',
			scope: true,
			//terminal: true,
			priority: 1001,

			/**
			 * @param $scope
			 * @param $element
			 * @param {{}} $attrs
			 * @param {string} $attrs.sortTable
			 */
			controller: function($scope, $element, $attrs) {
				// Initialization
				$scope.pageNumber = 1;
				$scope.pageSize = 10;
				$scope.maxSize = 10;
				$scope.search = "";
				$scope.orderKey = "";
				$scope.orderAsc = true;

				// Functions
				$scope.doSort = function(path) {
					if($scope.orderKey === path) {
						$scope.orderAsc = !$scope.orderAsc;
					} else {
						$scope.orderKey = path;
						$scope.orderAsc = true;
					}
				};
				$scope.checkSortClass = function(key) {
					if($scope.orderKey === key) {
						return "fa sort-mark " + ($scope.orderAsc ? "fa-sort-asc" : "fa-sort-desc");
					}
					return "fa fa-sort sort-mark";
				};

				var cacheSearch = "";
				var cacheOrder = "";
				var cacheOrderAsc = null;
				var cacheFilteredList = null;
				$scope.getFilteredList = function () {
					if(
						cacheSearch !== $scope.search ||
						cacheOrder !== $scope.orderKey ||
						cacheOrderAsc !== $scope.orderAsc ||
						!cacheFilteredList
					) {
						cacheSearch = $scope.search;
						cacheOrder = $scope.orderKey;
						cacheOrderAsc = $scope.orderAsc;

						cacheFilteredList = $scope.$parent[$attrs.sortTable] || [];

						if(cacheSearch) {
							cacheFilteredList = $.grep(cacheFilteredList, function (obj) {
								return hasContent(obj, cacheSearch);
							});
						}

						if(cacheOrder) {
							if (cacheOrderAsc) {
								cacheFilteredList.sort(function (obj1, obj2) {
									var val1 = common.getValueByPath(obj1, cacheOrder);
									var val2 = common.getValueByPath(obj2, cacheOrder);

									if (val1 === val2) {
										return 0;
									} else if (val1 < val2) {
										return -1;
									}
									return 1;
								});
							} else {
								cacheFilteredList.sort(function (obj1, obj2) {
									var val1 = common.getValueByPath(obj1, cacheOrder);
									var val2 = common.getValueByPath(obj2, cacheOrder);

									if (val1 === val2) {
										return 0;
									} else if (val1 < val2) {
										return 1;
									}
									return -1;
								});
							}
						}
					}

					return cacheFilteredList;
				};

				// Week watch. Will not track each element
				$scope.$watch($attrs.sortTable + ".length", function () {
					cacheFilteredList = null;
				});
			},
			compile: function ($element) {
				var contents = $element.contents().remove();

				return {
					post: function preLink($scope, $element, $attrs) {
						$element.append(contents);

						// Search Box
						var $search = $(
							'<div class="search-box">' +
							'<input type="search" class="form-control input-sm" placeholder="Search" ng-model="search" />' +
							'<span class="fa fa-search" />' +
							'</div>'
						).insertBefore($element.find("table"));
						$compile($search)($scope);

						// Sort Column
						$element.find("table [sortpath]").each(function () {
							var $this = $(this);
							var _sortpath = $this.attr("sortpath");
							$this.attr("ng-click", "doSort('" + _sortpath + "')");
							$this.prepend('<span ng-class="checkSortClass(\'' + _sortpath + '\')"></span>');
							$compile($this)($scope);
						});

						// Repeat Items
						var $tr = $element.find("table [ts-repeat], table > tbody > tr").filter(":first");
						//$tr.attr("ng-repeat", 'item in (filteredList = (' + $attrs.sortTable + ' | filter: search | orderBy: orderKey)).slice((pageNumber - 1) * pageSize, pageNumber * pageSize) track by $index');
						$tr.attr("ng-repeat", 'item in getFilteredList().slice((pageNumber - 1) * pageSize, pageNumber * pageSize) track by $index');
						$compile($tr)($scope);

						// Page Navigation
						var $navigation = $(
							'<div class="navigation-bar clearfix">' +
							'<span>' +
							'show {{(pageNumber - 1) * pageSize + 1}} to {{pageNumber * pageSize}} of {{getFilteredList().length}} items' +
							'</span>' +
							'<uib-pagination total-items="getFilteredList().length" ng-model="pageNumber" boundary-links="true" items-per-page="pageSize" max-size="maxSize"></uib-pagination>' +
							'</div>'
						).appendTo($element);
						$compile($navigation)($scope);
					}
				};
			}
		};
	});
})();
