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

eagleComponents.directive('tabs', function() {
	'use strict';

	return {
		restrict: 'AE',
		transclude: {
			'header': '?header',
			'pane': 'pane',
			'footer': '?footer'
		},
		scope : {
			title: "@?title",
			icon: "@",
			selected: "@?selected",
			holder: "=?holder",
			sortableModel: "=?sortableModel",

			menuList: "=?menu"
		},

		controller: function($scope, $element, $attrs, $timeout) {
			var transDuration = $.fn.tab.Constructor.TRANSITION_DURATION;
			var transTimer = null;
			var _holder, _holder_updateTimes;

			var $header, $footer;

			$scope.paneList = [];
			$scope.selectedPane = null;
			$scope.inPane = null;
			$scope.activePane = null;

			// ================== Function ==================
			$scope.getPaneList = function() {
				return !$scope.title ? $scope.paneList : $scope.paneList.slice().reverse();
			};

			$scope.setSelect = function(pane) {
				if(typeof pane === "string") {
					pane = common.array.find(pane, $scope.paneList, "title");
				}

				$scope.activePane = $scope.selectedPane || pane;
				$scope.selectedPane = pane;
				$scope.inPane = null;

				if(transTimer) $timeout.cancel(transTimer);
				transTimer = $timeout(function() {
					$scope.activePane = $scope.selectedPane;
					$scope.inPane = $scope.selectedPane;
				}, transDuration);
			};

			$scope.getMenuList = function() {
				if($scope.selectedPane && $scope.selectedPane.menuList) {
					return $scope.selectedPane.menuList;
				}
				return $scope.menuList;
			};

			$scope.tabSwitchUpdate = function (src, tgt) {
				var _srcIndex = $.inArray(src.data, $scope.sortableModel);
				var _tgtIndex = $.inArray(tgt.data, $scope.sortableModel);

				if(_srcIndex !== -1 && _tgtIndex !== -1) {
					$scope.sortableModel[_srcIndex] = tgt.data;
					$scope.sortableModel[_tgtIndex] = src.data;
				}
			};

			// =================== Linker ===================
			function _linkerProperties(pane) {
				Object.defineProperties(pane, {
					selected: {
						get: function () {
							return $scope.selectedPane === this;
						}
					},
					active: {
						get: function () {
							return $scope.activePane === this;
						}
					},
					in: {
						get: function () {
							return $scope.inPane === this;
						}
					}
				});
			}

			this.addPane = function(pane) {
				$scope.paneList.push(pane);

				// Register properties
				_linkerProperties(pane);

				// Update select pane
				if(pane.title === $scope.selected || !$scope.selectedPane) {
					$scope.setSelect(pane);
				}
			};

			this.deletePane = function(pane) {
				common.array.remove(pane, $scope.paneList);

				if($scope.selectedPane === pane) {
					$scope.selectedPane = $scope.activePane = $scope.inPane = $scope.paneList[0];
				}
			};

			// ===================== UI =====================
			$header = $element.find("> .nav-tabs-custom > .box-body");
			$footer = $element.find("> .nav-tabs-custom > .box-footer");

			$scope.hasHeader = function() {
				return !!$header.children().length;
			};
			$scope.hasFooter = function() {
				return !!$footer.children().length;
			};

			// ================= Interface ==================
			_holder_updateTimes = 0;
			_holder = {
				scope: $scope,
				element: $element,
				setSelect: $scope.setSelect
			};

			Object.defineProperty(_holder, 'selectedPane', {
				get: function() {return $scope.selectedPane;}
			});

			$scope.$watch("holder", function(newValue, oldValue) {
				// Holder times update
				setTimeout(function() {
					_holder_updateTimes = 0;
				}, 0);
				_holder_updateTimes += 1;
				if(_holder_updateTimes > 100) throw "Holder conflict";

				$scope.holder = _holder;
			});
		},

		template :
			'<div class="nav-tabs-custom">' +
				// Menu
				'<div class="box-tools pull-right" ng-if="getMenuList() && getMenuList().length">' +
					'<div ng-repeat="menu in getMenuList() track by $index" class="inline">' +
						// Button
						'<button class="btn btn-box-tool" ng-click="menu.func($event)" ng-if="!menu.list"' +
							' uib-tooltip="{{menu.title}}" tooltip-enable="menu.title" tooltip-append-to-body="true">' +
							'<span class="fa fa-{{menu.icon}}"></span>' +
						'</button>' +

						// Dropdown Group
						'<div class="btn-group" ng-if="menu.list">' +
							'<button class="btn btn-box-tool dropdown-toggle" data-toggle="dropdown"' +
								' uib-tooltip="{{menu.title}}" tooltip-enable="menu.title" tooltip-append-to-body="true">' +
								'<span class="fa fa-{{menu.icon}}"></span>' +
							'</button>' +
							'<ul class="dropdown-menu left" role="menu">' +
								'<li ng-repeat="item in menu.list track by $index" ng-class="{danger: item.danger, disabled: item.disabled}">' +
									'<a ng-click="!item.disabled && item.func($event)" ng-class="{strong: item.strong}">' +
										'<span class="fa fa-{{item.icon}}"></span> {{item.title}}' +
									'</a>' +
								'</li>' +
							'</ul>' +
						'</div>' +
					'</div>' +
				'</div>' +

				'<ul uie-sortable sortable-enabled="!!sortableModel" sortable-update-func="tabSwitchUpdate" ng-model="paneList" class="nav nav-tabs" ng-class="{\'pull-right\': title}">' +
					// Tabs
					'<li ng-repeat="pane in getPaneList() track by $index" ng-class="{active: selectedPane === pane}">' +
						'<a ng-click="setSelect(pane);">{{pane.title}}</a>' +
					'</li>' +

					// Title
					'<li class="pull-left header" ng-if="title">' +
						'<i class="fa fa-{{icon}}" ng-if="icon"></i> {{title}}' +
					'</li>' +

				'</ul>' +
				'<div class="box-body" ng-transclude="header" ng-show="paneList.length && hasHeader()"></div>' +
				'<div class="tab-content" ng-transclude="pane"></div>' +
				'<div class="box-footer" ng-transclude="footer" ng-show="paneList.length && hasFooter()"></div>' +
			'</div>'
	};
}).directive('pane', function() {
	'use strict';

	return {
		require : '^tabs',
		restrict : 'AE',
		transclude : true,
		scope : {
			title : '@',
			data: '=?data',
			menuList: "=?menu"
		},
		link : function(scope, element, attrs, tabsController) {
			tabsController.addPane(scope);
			scope.$on('$destroy', function() {
				tabsController.deletePane(scope);
			});
		},
		template : '<div class="tab-pane fade" ng-class="{active: active, in: in}" ng-transclude></div>',
		replace : true
	};
}).directive('footer', function() {
	'use strict';

	return {
		require : '^tabs',
		restrict : 'AE',
		transclude : true,
		scope : {},
		controller: function($scope, $element) {
		},
		template : '<div ng-transclude></div>',
		replace : true
	};
});