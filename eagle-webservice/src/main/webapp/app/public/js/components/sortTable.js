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

	// =======================================================================================
	// =                                       Wrapper                                       =
	// =======================================================================================
	eagleComponents.directive('sortTable', function($compile) {
		'use strict';

		return {
			restrict: 'AE',
			controller: function($scope, $element, $attrs) {
				//console.log("Controller >>>", $scope, $element, $attrs);
				//console.log($element.html());
			},
			/*compile: function ($element, $attrs, transclude) {
				return {
					pre: function preLink($scope, $element, $attrs, controller) {
						console.log("Pre >>>", $scope, $element, $attrs, controller);
						console.log($element.html());
					},
					post: function preLink($scope, $element, $attrs, controller) {
						console.log("Post >>>", $scope, $element, $attrs, controller);
						console.log($element.html());
					}
				}
			}*/
		};
	});

	// =======================================================================================
	// =                                       Wrapper                                       =
	// =======================================================================================
	eagleComponents.directive('stRepeat', function($compile) {
		'use strict';

		return {
			terminal: true,
			//multiElement: true,
			require: "^sortTable",
			priority: 1001,
			controller: function () {
			},
			compile: function ($element, $attrs, transclude) {
				return {
					pre: function($scope, $element, $attrs, controller) {
						var repeatStr = $element.attr("st-repeat") || $element.attr("ng-repeat");
						console.log(repeatStr);
						$element.removeAttr("st-repeat");
						$element.attr("ng-repeat", "item in dddList track by $index");
						$compile($element)($scope);
					}
				};
			}
		};
	});
})();
