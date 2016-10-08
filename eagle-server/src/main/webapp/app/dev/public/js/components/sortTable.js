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
		return {
			restrict: 'AE',
			scope: true,
			//terminal: true,
			priority: 1001,

			/**
			 * @param $scope
			 * @param $element
			 * @param {{}} $attrs
			 * @param {string} $attrs.sortTable			Data source
			 * @param {string?} $attrs.isSorting		Will bind parent variable of sort state
			 * @param {string?} $attrs.scope			Will bind parent variable of current scope
			 * @param {string?} $attrs.sortpath			Default sort path
			 * @param {[]?} $attrs.searchPathList		Filter search path list
			 */
			controller: function($scope, $element, $attrs) {
				var sortmatch;
				var worker;
				var worker_id = 0;
				if(typeof(Worker) !== "undefined") {
					worker = new Worker("public/js/worker/sortTableWorker.js?_=" + window._TRS());
				}

				// Initialization
				$scope.pageNumber = 1;
				$scope.pageSize = 10;
				$scope.maxSize = 10;
				$scope.search = "";
				$scope.orderKey = "";
				$scope.orderAsc = true;

				if($attrs.sortpath) {
					sortmatch = $attrs.sortpath.match(/^(-)?(.*)$/);
					if(sortmatch[1]) {
						$scope.orderAsc = false;
					}
					$scope.orderKey = sortmatch[2];
				}

				// UI - Column sort
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

				// List filter & sort
				function setState(bool) {
					if(!$attrs.isSorting) return;

					$scope.$parent[$attrs.isSorting] = bool;
				}


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

						var fullList = $scope.$parent[$attrs.sortTable] || [];
						if(!cacheFilteredList) cacheFilteredList = fullList;

						if(!worker) {
							cacheFilteredList = __sortTable_generateFilteredList(fullList, cacheSearch, cacheOrder, cacheOrderAsc, $scope.$parent[$attrs.searchPathList]);
							setState(false);
						} else {
							worker_id += 1;
							setState(true);
							var list = JSON.stringify(fullList);
							worker.postMessage({
								search: cacheSearch,
								order: cacheOrder,
								orderAsc: cacheOrderAsc,
								searchPathList: $scope.$parent[$attrs.searchPathList],
								list: list,
								id: worker_id
							});
						}
					}

					return cacheFilteredList;
				};

				// Week watch. Will not track each element
				$scope.$watch($attrs.sortTable + ".length", function () {
					cacheFilteredList = null;
				});

				function workMessage(event) {
					var data = event.data;
					if(worker_id !== data.id) return;

					setState(false);
					cacheFilteredList = data.list;
					$scope.$apply();
				}
				worker.addEventListener("message", workMessage);

				$scope.$on('$destroy', function() {
					worker.removeEventListener("message", workMessage);
				});

				// Scope bind
				if($attrs.scope) {
					$scope.$parent[$attrs.scope] = $scope;
				}
			},
			compile: function ($element) {
				var contents = $element.contents().remove();

				return {
					post: function preLink($scope, $element) {
						$scope.defaultPageSizeList = [10, 25, 50, 100];

						$element.append(contents);

						// Tool Container
						var $toolContainer = $(
							'<div class="tool-container clearfix">' +
							'</div>'
						).insertBefore($element.find("table"));

						// Search Box
						var $search = $(
							'<div class="search-box">' +
							'<input type="search" class="form-control input-sm" placeholder="Search" ng-model="search" />' +
							'<span class="fa fa-search" />' +
							'</div>'
						).appendTo($toolContainer);
						$compile($search)($scope);

						// Page Size
						var $pageSize = $(
							'<div class="page-size">' +
							'Show' +
							'<select class="form-control" ng-model="pageSize" convert-to-number>' +
							'<option ng-repeat="size in pageSizeList || defaultPageSizeList track by $index">{{size}}</option>' +
							'</select>' +
							'Entities' +
							'</div>'
						).appendTo($toolContainer);
						$compile($pageSize)($scope);

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

	eagleComponents.directive('convertToNumber', function() {
		return {
			require: 'ngModel',
			link: function(scope, element, attrs, ngModel) {
				ngModel.$parsers.push(function(val) {
					return parseInt(val, 10);
				});
				ngModel.$formatters.push(function(val) {
					return '' + val;
				});
			}
		};
	});
})();
