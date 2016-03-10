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

	var eagleSrv = angular.module('eagle.service', []);

	eagleSrv.provider('ServiceError', function() {
		var errorContainer = {
			list: [],
			newError: function(err) {
				err._read = false;
				errorContainer.list.unshift(err);
			},
			showError: function(err) {
				err._read = true;
				$.dialog({
					size: "large",
					title: err.title,
					content: $("<pre>").html(err.description)
				});
			},
			clearAll: function() {
				errorContainer.list = [];
			}
		};

		Object.defineProperty(errorContainer, 'hasUnread', {
			get: function() {
				return !!common.array.find(false, errorContainer.list, "_read");
			}
		});

		this.$get = function() {
			return errorContainer;
		};
	});

	eagleSrv.config(function ($httpProvider, ServiceErrorProvider) {
		$httpProvider.interceptors.push(function ($q, $timeout) {
			return {
				response: function (response) {
					var data = response.data;
					if(data.exception) {
						console.log(response);
						ServiceErrorProvider.$get().newError({
							title: "Http Request Error",
							description: "URL:\n" + response.config.url + "\n\nParams:\n" + JSON.stringify(response.config.params, null, "\t") + "\n\nException:\n" + data.exception
						});
					}
					return response;
				}
			};
		});
	});
})();
