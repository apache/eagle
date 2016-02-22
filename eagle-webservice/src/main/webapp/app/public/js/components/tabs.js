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

			menuList: "=?menu"
		},

		controller: function($scope, $element, $attrs, $timeout) {
			var transDuration = $.fn.tab.Constructor.TRANSITION_DURATION;
			var transTimer = null;

			var $header, $footer;

			$scope.paneList = [];
			$scope.selectedPane = null;
			$scope.activePane = null;

			// ================== Function ==================
			$scope.getPaneList = function() {
				return !$scope.title ? $scope.paneList : $scope.paneList.slice().reverse();
			};

			$scope.setSelect = function(pane) {
				$scope.activePane = $scope.selectedPane || pane;
				$scope.selectedPane = pane;

				transTimer && $timeout.cancel(transTimer);
				transTimer = $timeout(function() {
					$scope.activePane = $scope.selectedPane;
				}, transDuration);
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
							return $scope.selectedPane === this;
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
					$scope.selectedPane = $scope.paneList[0];
					$scope.activePane = $scope.paneList[0];
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
		},

		template :
			'<div class="nav-tabs-custom">' +
				'<ul class="nav nav-tabs" ng-class="{\'pull-right\': title}">' +
					// Tabs
					'<li ng-repeat="pane in getPaneList() track by $index" ng-class="{active: selectedPane === pane}">' +
						'<a ng-click="setSelect(pane);">{{pane.title}}</a>' +
					'</li>' +

					// Title
					'<li class="pull-left header" ng-if="title">' +
						'<i class="fa fa-{{icon}}" ng-if="icon"></i> {{title}}' +
					'</li>' +

					// Menu
					'<li class="pull-right" ng-if="menuList && menuList.length">' +
						'<a ng-repeat="menu in menuList track by $index" class="text-muted" ng-click="menu.func($event)"' +
							' uib-tooltip="{{menu.title}}" tooltip-enable="menu.title" tooltip-append-to-body="true">' +
							'<span class="fa fa-{{menu.icon}}"></span>' +
						'</a>' +
					'</li>' +
				'</ul>' +
				'<div class="box-body" ng-transclude="header" ng-show="hasHeader()"></div>' +
				'<div class="tab-content" ng-transclude="pane"></div>' +
				'<div class="box-footer" ng-transclude="footer" ng-show="hasFooter()"></div>' +
			'</div>'
	};
}).directive('pane', function() {
	'use strict';

	return {
		require : '^tabs',
		restrict : 'AE',
		transclude : true,
		scope : {
			title : '@'
		},
		controller: function($scope, $element, $animate) {
			$animate.enabled(false, $element);
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