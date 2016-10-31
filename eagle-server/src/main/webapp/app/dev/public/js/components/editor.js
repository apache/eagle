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

	eagleComponents.directive('editor', function() {
		return {
			restrict: 'AE',
			require: 'ngModel',

			link: function($scope, $element, $attrs, $ctrl) {
				$element.innerHeight(21 * Number($attrs.rows || 10));

				var updateId;
				function updateScope(value) {
					clearTimeout(updateId);

					updateId = setTimeout(function () {
						$ctrl.$setViewValue(value);
					}, 0);
				}

				var editLock = false;

				var editor = ace.edit($element[0]);
				var session = editor.getSession();
				editor.container.style.lineHeight = 1.5;
				editor.setOptions({
					fontSize: "14px"
				});
				editor.setTheme("ace/theme/tomorrow");
				editor.$blockScrolling = Infinity;
				editor.getSession().on('change', function(event) {
					editLock = true;

					var value = session.getValue();
					updateScope(value);

				});
				session.setUseWorker(false);
				session.setUseWrapMode(true);
				session.setMode("ace/mode/sql");

				$scope.$watch($attrs.ngModel, function (newValue) {
					if(editLock) {
						editLock = false;
						return;
					}

					session.setValue(newValue || "");
				});

				$scope.$on('$destroy', function() {
					editor.destroy();
				});
			},
			template: '<div class="form-control"></div>',
			replace: true
		};
	});
})();
