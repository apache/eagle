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

	serviceModule.service('Application', function($wrapState, Entity) {
		var Application = {};

		Application.list = [];

		// Load applications
		Application.reload = function () {
			Application.list = Entity.query('apps');
		};

		// Load providers
		Application.providers = {};
		Application.providerList = Entity.query('apps/providers');
		Application.providerList._promise.then(function () {
			$.each(Application.providerList, function (i, app) {
				Application.providers[app.type] = app;
			});
		});

		Application.getPromise = function () {
			return Application.list._promise.then(function() {
				return Application;
			});
		};

		// Initialization
		Application.reload();

		return Application;
	});
}());
