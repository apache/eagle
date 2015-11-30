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

// =============================================================
// =                        Alert List                         =
// =============================================================
damControllers.controller('alertListCtrl', function(globalContent, Site, damContent, $scope, $routeParams, $interval, $timeout, Entities) {
	globalContent.setConfig(damContent.config);
	globalContent.pageSubTitle = Site.current().name;

	var MAX_PAGESIZE = 10000;

	// Initial load
	$scope.dataSource = $routeParams.dataSource;

	$scope.alertList = [];
	$scope.alertList.ready = false;

	// Load data
	function _loadAlerts() {
		if($scope.alertList._promise) {
			$scope.alertList._promise.abort();
		}

		var _list = Entities.queryEntities("AlertService", {
			site: Site.current().name,
			dataSource: $scope.dataSource,
			hostname: null,
			_pageSize: MAX_PAGESIZE,
			_duration: 1000 * 60 * 60 * 24 * 30,
			__ETD: 1000 * 60 * 60 * 24
		});
		$scope.alertList._promise = _list._promise;
		_list._promise.then(function() {
			var index;

			if($scope.alertList[0]) {
				// List new alerts
				for(index = 0 ; index < _list.length ; index += 1) {
					var _alert = _list[index];
					_alert.__new = true;
					if(_alert.encodedRowkey === $scope.alertList[0].encodedRowkey) {
						break;
					}
				}

				if(index > 0) {
					$scope.alertList.unshift.apply($scope.alertList, _list.slice(0, index));

					// Clean up UI highlight
					$timeout(function() {
						$.each(_list, function(i, alert) {
							delete alert.__new;
						});
					}, 100);
				}
			} else {
				// List all alerts
				$scope.alertList.push.apply($scope.alertList, _list);
			}

			$scope.alertList.ready = true;
		});
	}

	_loadAlerts();
	var _loadInterval = $interval(_loadAlerts, app.time.refreshInterval);
	$scope.$on('$destroy',function(){
		$interval.cancel(_loadInterval);
	});
});

// =============================================================
// =                       Alert Detail                        =
// =============================================================
damControllers.controller('alertDetailCtrl', function(globalContent, Site, damContent, $scope, $routeParams, Entities) {
	globalContent.setConfig(damContent.config);
	globalContent.pageTitle = "Alert Detail";
	globalContent.navPath = ["Alert List", "Alert Detail"];
	globalContent.lockSite = true;

	$scope.common = common;

	// Query policy
	$scope.alertList = Entities.queryEntity("AlertService", $routeParams.encodedRowkey);
	$scope.alertList._promise.then(function() {
		if($scope.alertList.length === 0) {
			$.dialog({
				title: "OPS!",
				content: "Alert not found!",
			}, function() {
				location.href = "#/dam/alertList";
			});
			return;
		} else {
			var alert = $scope.alertList[0];

			$scope.alert = alert;
			Site.current(Site.find($scope.alert.tags.site));
			console.log($scope.alert);
		}
	});

	// UI
	$scope.getMessageTime = function(alert) {
		var _time = common.getValueByPath(alert, "alertContext.properties.timestamp");
		return Number(_time);
	};
});
