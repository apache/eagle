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

	var featureControllers = angular.module('featureControllers');

	// =============================================================
	// =                          Summary                          =
	// =============================================================
	featureControllers.registerNavItem({
		icon: "list",
		title: "Policies",
		url: "summary"
	});
	featureControllers.register('common_summaryCtrl', function(globalContent, Site, damContent, $scope, $q, Entities, $route) {
		globalContent.setConfig(damContent.config);
		globalContent.pageSubTitle = Site.current().name;

		$scope.dataSources = {};
		$scope.dataReady = false;

		var _policyList = Entities.queryGroup("AlertDefinitionService", {dataSource:null, site: Site.current().name}, "@dataSource", "count");

		_policyList._promise.then(function() {
			// List programs
			$.each(_policyList, function(i, unit) {
				var _dataSrc = Site.current().dataSrcList.find(unit.key[0]);
				if(_dataSrc) {
					_dataSrc.count = unit.value[0];
				} else {
					var _siteHref = $("<a href='#/dam/siteList'>").text("Setup");
					var _dlg = $.dialog({
						title: "Data Source Not Found",
						content: $("<div>")
							.append("Data Source [" + unit.key[0] + "] not found. Please check your configuration in ")
							.append(_siteHref)
							.append(" page.")
					});
					_siteHref.click(function() {
						_dlg.modal('hide');
					});
				}
			});

			$scope.dataReady = true;
		});
	});
})();