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

	eagleComponents.directive('widget', function($compile, Site) {
		return {
			restrict: 'AE',
			priority: 1001,

			controller: function($scope, $element, $attrs) {
			},
			compile: function ($element) {
				var $content = $element.contents().remove();

				return {
					post: function preLink($scope, $element) {
						var widget = $scope.widget;
						$scope.site = Site.current();

						// If not set widget, skip dynamic render. (AdminLTE use `data-widget` as attr key)
						if (!widget) {
							$element.append($content);
							return;
						}

						if(widget.renderFunc) {
							// Prevent auto compile if return false
							if(widget.renderFunc($element, $scope, $compile) !== false) {
								$compile($element.contents())($scope);
							}
						} else {
							$element.append("Widget don't provide render function:" + widget.application + " - " + widget.name);
						}
					}
				};
			}
		};
	});
})();
