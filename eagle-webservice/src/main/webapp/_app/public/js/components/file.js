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

eagleComponents.directive('file', function($compile) {
	'use strict';

	return {
		restrict : 'A',
		scope: {
			filepath: "=?filepath",
		},
		controller: function($scope, $element, $attrs) {
			// Watch change(Only support clean the data)
			if($attrs.filepath) {
				$scope.$parent.$watch($attrs.filepath, function(value) {
					if(!value) $element.val(value);
				});
			}

			// Bind changed value
			$element.on("change", function() {
				var _path = $(this).val();
				if($attrs.filepath) {
					common.setValueByPath($scope.$parent, $attrs.filepath, _path);
					$scope.$parent.$apply();
				}
			});

			$scope.$on('$destroy',function(){
				$element.off("change");
			});
		},
		replace: false
	};
});
