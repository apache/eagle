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
	serviceModule.provider('Application', function() {
		// TODO: Mock
		this.list = [
			{name: "DAM", description: "Security check application"},
			{name: "JPA", description: "JPA Test Application"}
		];

		// TODO: Mock
		this.featureList = [
			{name: "Common", description: "Provide the Policy & Alert feature."},
			{name: "Classification", description: "Sensitivity browser of the data classification."},
			{name: "User Profile", description: "Machine learning of the user profile"},
			{name: "Metadata", description: "Stream metadata viewer"},
			{name: "Setup", description: "Stream configuration"},
		];

		this.$get = function() {
			return this;
		};
	});
})();