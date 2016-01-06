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

	var serviceModule = angular.module('eagle.service');
	serviceModule.service('$wrapState', function($state) {
		var $wrapState = {};
		var _targetState;

		// Go
		$wrapState.go = function(state, force) {
			setTimeout(function() {
				_targetState = null;
			});

			if(_targetState !== state || force) {
				if($state.current && $state.current.name === state) {
					$state.reload();
				} else {
					$state.go(state);
				}
				_targetState = state;
				return true;
			}
			return false;
		};

		// Reload
		$wrapState.reload = function() {
			$state.reload();
		};

		Object.defineProperties($wrapState, {
			// Origin $state
			origin: {
				get: function() {
					return $state;
				}
			},
			// Current
			current: {
				get: function() {
					return $state.current;
				}
			}
		});

		return $wrapState;
	});
})();