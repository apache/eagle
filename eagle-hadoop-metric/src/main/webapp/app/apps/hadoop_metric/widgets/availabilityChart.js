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

(function () {
	/**
	 * `register` without params will load the module which using require
	 */
	register(function (hadoopMetricApp) {
		hadoopMetricApp.directive("hadoopMetricWidget", function () {
			return {
				restrict: 'AE',
				controller: function($scope) {
					console.log('~~>', $scope.site);
				},
				template:
				'<div class="small-box bg-red">' +
					'TODO: Availability Chart Widget' +
				'</div>',
				replace: true
			};
		});

		/**
		 * Customize the widget content. Return false will prevent auto compile.
		 * @param {{}} $element
		 * @param {function} $element.append
		 */
		function registerWidget($element) {
			$element.append(
				$("<div hadoop-metric-widget data-site='site'>")
			);
		}

		hadoopMetricApp.widget("availabilityChart", registerWidget, true);
	});
})();
