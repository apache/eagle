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

'use strict';

/* Controllers */
var damControllers = angular.module('damControllers', ['ui.bootstrap', 'eagle.components']);

damControllers.service('damContent', function(Entities) {
	var content = {
		config: {
			pageList: [
				{icon: "list", title: "Policies", url: "#/dam/summary"},
				{icon: "exclamation-triangle", title: "Alerts", url: "#/dam/alertList"},
				{icon: "user-secret", title: "Classification", url: "#/dam/sensitivitySummary"},
				{icon: "graduation-cap", title: "User Profiles", url: "#/dam/userProfileList"},
				{icon: "bullseye", title: "Metadata", url: "#/dam/streamList"},
				{icon: "server", title: "Setup", url: "#/dam/siteList", roles: ["ROLE_ADMIN"]},
			],
			navMapping: {
				"Policy View": "#/dam/summary",
				"Polict List": "#/dam/policyList",
				"Alert List": "#/dam/alertList",
				"User Profile": "#/dam/userProfileList",
			},
		},
		updatePolicyStatus: function(policy, status) {
			$.dialog({
				title: "Confirm",
				content: "Do you want to " + (status ? "enable" : "disable") + " policy[" + policy.tags.policyId + "]?",
				confirm: true,
			}, function(ret) {
				if(ret) {
					policy.enabled = status;
					Entities.updateEntity("AlertDefinitionService", policy);
				}
			});
		},
		deletePolicy: function(policy, callback) {
			$.dialog({
				title: "Confirm",
				content: "Do you want to delete policy[" + policy.tags.policyId + "]?",
				confirm: true,
			}, function(ret) {
				if(ret) {
					policy.enabled = status;
					Entities.deleteEntity("AlertDefinitionService", policy)._promise.finally(function() {
						if(callback) {
							callback(policy);
						}
					});
				}
			});
		},
	};
	return content;
});

// =============================================================
// =                          Summary                          =
// =============================================================
damControllers.controller('summaryCtrl', function(globalContent, Site, damContent, $scope, $q, Entities, $route) {
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
