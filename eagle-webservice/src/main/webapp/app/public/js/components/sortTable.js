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

	eagleComponents.directive('sortTable', function($compile) {
		'use strict';

		return {
			restrict: 'AE',
			scope: true,
			terminal: true,
			priority: 1001,
			controller: function($scope, $element, $attrs) {
				// Initialization
				$scope.pageNumber = 1;
				$scope.pageSize = 10;
				$scope.maxSize = 10;
				$scope.search = "";

				// Functions
				$scope.doSort = function(path) {
					if($scope.orderKey === path) {
						$scope.orderKey = "-" + path;
					} else {
						$scope.orderKey = path;
					}
				};
				$scope.checkSortClass = function(key) {
					if($scope.orderKey === key) {
						return "fa fa-sort-asc sort-mark";
					} else if($scope.orderKey === "-" + key) {
						return "fa fa-sort-desc sort-mark";
					}
					return "fa fa-sort sort-mark";
				};
			},
			compile: function () {
				return {
					pre: function preLink($scope, $element, $attrs) {
						// Search Box
						var $search = $(
							'<div class="search-box">' +
							'<input type="search" class="form-control input-sm" placeholder="Search" ng-model="search" />' +
							'<span class="fa fa-search" />' +
							'</div>'
						).prependTo($element);
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
						$tr.attr("ng-repeat", 'item in (filteredList = (' + $attrs.sortTable + ' | filter: search | orderBy: orderKey)).slice((pageNumber - 1) * pageSize, pageNumber * pageSize) track by $index');
						$compile($tr)($scope);

						// Page Navigation
						var $navigation = $(
							'<div class="navigation-bar clearfix">' +
								'<span>' +
									'show {{(pageNumber - 1) * pageSize + 1}} to {{pageNumber * pageSize}} of {{filteredList.length}} items' +
								'</span>' +
								'<uib-pagination total-items="filteredList.length" ng-model="pageNumber" boundary-links="true" items-per-page="pageSize" max-size="maxSize"></uib-pagination>' +
							'</div>'
						).appendTo($element);
						$compile($navigation)($scope);
					}
				}
			}
		};
	});
})();
