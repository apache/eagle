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
'use strict';

eagleComponents.directive('sorttable', function($compile) {
	return {
		restrict : 'AE',
		scope: {
			source: '=',
			search: '=?search',
			searchfunc: "=?searchfunc",

			orderKey: "@?sort",

			maxSize: "=?maxSize",
		},
		controller: function($scope, $element, $attrs) {
			// Initialization
			$scope.app = app;
			$scope.common = common;
			$scope._parent = $scope.$parent;

			$scope.pageNumber = $scope.pageNumber || 1;
			$scope.pageSize = $scope.pageSize || 10;

			$scope.maxSize = $scope.maxSize || 10;

			// Search box
			if($scope.search !== false) {
				var $search = $(
					'<div style="overflow:hidden;">' +
						'<div class="row">' +
							'<div class="col-xs-4">' +
								'<div class="search-box">' +
									'<input type="search" class="form-control input-sm" placeholder="Search" ng-model="search" />' +
									'<span class="fa fa-search"></span>' +
								'</div>' +
							'</div>' +
						'</div>' +
					'</div>'
				).prependTo($element);
				$compile($search)($scope);
			}

			// List head
			$scope.doSort = function(path) {
				if($scope.orderKey === path) {
					$scope.orderKey = "-" + path;
				} else {
					$scope.orderKey = path;
				}
			};
			$scope.checkSortClass = function(key) {
				if($scope.orderKey === key) {
					return "fa fa-sort-asc";
				} else if($scope.orderKey === "-" + key) {
					return "fa fa-sort-desc";
				}
				return "fa fa-sort";
			};

			var $listHead = $element.find("thead > tr");
			$listHead.find("> th").each(function() {
				var $th = $(this);
				$th.addClass("noSelect");

				var _sortpath = $th.attr("sortpath");
				if(_sortpath) {
					$th.attr("ng-click", "doSort('" + _sortpath + "')");
					$th.prepend('<span ng-class="checkSortClass(\'' + _sortpath + '\')"></span>');
				}
			});
			$compile($listHead)($scope);

			// List body
			var $listBody = $element.find("tbody > tr");
			$listBody.attr("ng-repeat", 'item in (filteredList = (source | filter: ' + ($scope.searchfunc ? 'searchfunc' : 'search') + ' | orderBy: orderKey)).slice((pageNumber - 1) * pageSize, pageNumber * pageSize)');
			$compile($listBody)($scope);

			// Navigation
			var $navigation = $(
				'<div style="overflow:hidden;">' +
					'<div class="row">' +
						'<div class="col-xs-5">' +
							'show {{(pageNumber - 1) * pageSize + 1}} to {{pageNumber * pageSize}} of {{filteredList.length}} items' +
						'</div>' +
						'<div class="col-xs-7 text-right">' +
							'<pagination total-items="filteredList.length" ng-model="pageNumber" boundary-links="true" items-per-page="pageSize" num-pages="numPages" max-size="maxSize"></pagination>' +
						'</div>' +
					'</div>' +
				'</div>'
			).appendTo($element);
			$compile($navigation)($scope);
		},
		replace: false
	};
});
