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

/* Controllers */
var damControllers = angular.module('damControllers', ['ui.bootstrap', 'eagle.components']);

damControllers.service('damContent', function(Entities) {
	'use strict';

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
	'use strict';

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

// =============================================================
// =                         TODO: NVD3                        =
// =============================================================
damControllers.controller('nvd3Ctrl', function(globalContent, Site, damContent, $scope) {
	globalContent.setConfig(damContent.config);
	globalContent.pageTitle = "NVD3 Test";

	var _len = 75;
	function sinAndCos() {
		var sin = [],sin2 = [],
			cos = [];
		var _offset1 = Math.random() * 100;
		var _offset2 = Math.random() * 100;
		var _offset3 = Math.random() * 100;

		//Data is represented as an array of {x,y} pairs.
		var _now = new moment().hour(0).minute(0).second(0).millisecond(0).valueOf();
		var _x;
		_len += 1;
		for (var i = 0; i < _len; i++) {
			_x = _now + i * 1000 * 60 * 60 * 24;
			sin.push({x: _x, y: Math.abs(Math.sin((i+_offset1)/10))});
			sin2.push({x: _x, y: Math.abs(Math.sin((i+_offset2)/10) *0.25 + 0.5)});
			cos.push({x: _x, y: Math.abs(0.5 * Math.cos((i+_offset3)/10))});
		}

		//Line chart data should be sent as an array of series objects.
		return [
			{
				values: sin,      //values - represents the array of {x,y} data points
				key: 'Sine Wave', //key  - the name of the series.
				//color: '#ff7f0e'  //color - optional: choose your own line color.
			},
			{
				values: cos,
				key: 'Cosine Wave',
				//color: '#2ca02c'
			},
			{
				values: sin2,
				key: 'Another sine wave',
				//color: '#7777ff',
				area: true      //area - set to true if you want this line to turn into a filled area chart.
			}
		];
	}

	$scope.chartConfig = {
		chart: "",
		title: "",

		xTitle: "Test X Axis",
		yTitle: "Test Y Axis",
		xType: "time"
	};

	$scope.chart = "area";

	$scope.randomChartData = function() {
		$scope.data = sinAndCos();
	};

	console.log("call!");
});