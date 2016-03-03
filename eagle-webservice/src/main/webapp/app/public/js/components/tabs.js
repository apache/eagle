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
		restrict : 'AE',
		transclude : true,
		scope : {
			title: "@",
			icon: "@",
			selected: "@?selected",

			inner: "=?inner"
		},
		controller: function($scope, $element, $attrs, $timeout) {
			var _selected = null;

			var panes = $scope.panes = [];

			$scope.getList = function() {
				if($scope.inner) {
					return $scope.panes;
				} else {
					return $scope.panes.slice().reverse();
				}
			};

			$scope.select = function(pane, updateBind) {
				angular.forEach(panes, function(pane) {
					pane.selected = false;
				});
				pane.selected = true;
				_selected = pane;

				if(updateBind !== false && $attrs.selected) {
					$scope.$parent[$attrs.selected] = _selected.title;
				}
			};

			this.addPane = function(pane) {
				if (panes.length === 0 || ($attrs.selected && $scope.$parent[$attrs.selected] === pane.title)) {
					$scope.select(pane, false);
				}
				panes.push(pane);
			};

			// Listen tab selected change
			if($attrs.selected) {
				$scope.$parent.$watch($attrs.selected, function(value) {
					$.each(panes, function(i, pane) {
						if(value === pane.title) {
							$scope.select(pane, false);
							return false;
						}
					});
				});
			}
		},
		template : '<div ng-class="inner ? \'\' : \'nav-tabs-custom\'">' +
			'<ul class="nav nav-tabs ui-sortable-handle" ng-class="inner ? \'\' : \'pull-right\'">' +
				'<li ng-repeat="pane in getList()" ng-class="{active:pane.selected}">' +
					'<a href="" ng-click="select(pane)">{{pane.title}}</a>' +
				'</li>' +
				'<li class="pull-left header"><i class="fa fa-{{icon}}"></i> {{title}}</li>' +
			'</ul>' +
			'<div class="tab-content" ng-transclude></div>' +
		'</div>',
		replace : true
	};
}).directive('pane', function() {
	return {
		require : '^tabs',
		restrict : 'AE',
		transclude : true,
		scope : {
			title : '@'
		},
		controller: function($scope, $element, $timeout) {
			// Initialization
			var $innerScope = angular.element($element).scope();
			$innerScope.app = app;
			$innerScope.common = common;
			$innerScope._parent = $scope.$parent.$parent.$parent;
		},
		link : function(scope, element, attrs, tabsController) {
			tabsController.addPane(scope);
		},
		template : '<div class="tab-pane" ng-class="{active: selected}" ng-transclude="parent">' + '</div>',
		replace : true
	};
});