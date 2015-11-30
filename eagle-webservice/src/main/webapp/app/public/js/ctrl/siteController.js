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
// =                         Site List                         =
// =============================================================
damControllers.controller('siteListCtrl', function(globalContent, Site, damContent, $scope, $q, Entities) {
	globalContent.setConfig(damContent.config);
	globalContent.hideSite = true;

	$scope._siteEntity;
	$scope._siteEntityLock;

	$scope._dataSrcEntity;
	$scope._dataSrcEntityLock;

	$scope.dataSrcList = Entities.queryGroup("AlertStreamService", '@dataSource=~".*"', "@dataSource", "count");

	// Policy statistic
	$scope.policyStatistic = Entities.queryGroup("AlertDefinitionService", '@dataSource=~".*"', "@site,@dataSource", "count");
	$scope.getPolicyCount = function(site, dataSource) {
		for(var i = 0 ; i < $scope.policyStatistic.length ; i += 1) {
			var _cur = $scope.policyStatistic[i];
			if(_cur.key[0] === site && _cur.key[1] === dataSource) {
				return _cur.value[0];
			}
		}
		return 0;
	};

	// Alert statistic
	$scope.alertStatistic = Entities.queryGroup("AlertService", {_duration: 1000 * 60 * 60 * 24 * 30}, "@site,@dataSource", "count");
	$scope.getAlertCount = function(site, dataSource) {
		for(var i = 0 ; i < $scope.alertStatistic.length ; i += 1) {
			var _cur = $scope.alertStatistic[i];
			if(_cur.key[0] === site && _cur.key[1] === dataSource) {
				return _cur.value[0];
			}
		}
		return 0;
	};

	// =========================================== Site ===========================================
	$scope.showSiteEditor = function(site) {
		$("#siteMDL").modal("show");
		setTimeout(function() {
			$("#siteName").focus();
		}, 500);

		$scope._siteEntity = {
			dataSrcList: {}
		};
		$.each($scope.dataSrcList, function(i, item) {
			$scope._siteEntity.dataSrcList[item.key[0]] = {
				name: item.key[0],
				enabled: false
			};
		});

		if(site) {
			$scope._siteEntity.srcSite = site;
			$scope._siteEntity.name = site.name;

			$.each(site.dataSrcList, function(i, dataSrc) {
				$scope._siteEntity.dataSrcList[dataSrc.tags.dataSource].enabled = dataSrc.enabled === undefined ? true : dataSrc.enabled;
			});
		}
	};
	$scope.checkUpdateSite = function() {
		if(!$scope._siteEntity || !$scope._siteEntity.dataSrcList) return false;

		var _hasDataSrc = !!common.array.find(true, common.map.toArray($scope._siteEntity.dataSrcList), "enabled");
		return $scope._siteEntity.name && _hasDataSrc && !$scope._siteEntityLock;
	};
	$scope.confirmUpateSite = function() {
		var promiseList = [];
		$scope._siteEntityLock = true;

		if($scope._siteEntity.srcSite) {
			var promiseList = [];
			$.each($scope._siteEntity.dataSrcList, function(name, dataSrc) {
				var _entity = {
					enabled: dataSrc.enabled,
					tags: {
						site: $scope._siteEntity.name,
						dataSource: name,
					},
				};

				if(dataSrc.enabled) {
					promiseList.push(Entities.updateEntity("AlertDataSourceService", _entity)._promise);
				} else {
					var _dataSrc = common.array.find(name, $scope._siteEntity.srcSite.dataSrcList, "tags.dataSource");
					if(_dataSrc) {
						_dataSrc.enabled = false;
						promiseList.push(Entities.updateEntity("AlertDataSourceService", _entity)._promise);
					}
				}
			});
		} else {
			$.each($scope._siteEntity.dataSrcList, function(name, dataSrc) {
				if(!dataSrc.enabled) return;

				var _entity = {
					enabled: true,
					tags: {
						site: $scope._siteEntity.name,
						dataSource: name,
					},
				};
				promiseList.push(Entities.updateEntity("AlertDataSourceService", _entity)._promise);
			});
		}
		
		$q.all(promiseList).then(function() {
			$("#siteMDL").modal("hide")
			.on("hidden.bs.modal", function() {
				$("#siteMDL").off("hidden.bs.modal");
				Site.refresh();
			});
		}).finally(function() {
			$scope._siteEntityLock = false;
		});
	};

	$scope.deleteSite = function(site) {
		$.dialog({
			title: "Delete Confirm",
			content: "<span class='text-red fa fa-exclamation-triangle pull-left' style='font-size: 50px;'></span>" +
					"<p>You are <strong class='text-red'>DELETING</strong> the site '<strong>" + site.name + "</strong>'!</p>" +
					"<p>Proceed to delete?</p>",
			buttons: [
				{name: "Delete", class: "btn btn-danger", value: true},
				{name: "Cancel", class: "btn btn-default", value: false},
			]
		}, function(ret) {
			if(!ret) return;

			Entities.deleteEntities("AlertDataSourceService", {
				site: site.name
			})._promise.then(function() {
				Site.refresh();
			});
		});
	};

	// ======================================= Data Source ========================================
	$scope.showDataSourceEditor = function(dataSrc) {
		$("#dataSrcMDL").modal("show");
		setTimeout(function() {
			$("#dataSrcConfig").focus();
		}, 500);

		$scope._dataSrcEntity = dataSrc;
	};

	$scope.confirmUpateDataSource = function() {
		$scope._dataSrcEntityLock = true;
		Entities.updateEntity("AlertDataSourceService", $scope._dataSrcEntity)._promise.then(function() {
			$("#dataSrcMDL").modal("hide");
		}).finally(function() {
			$scope._dataSrcEntityLock = false;
		});
	};

	$scope.confirmDeleteDataSource = function() {
		console.log($scope._dataSrcEntity);
		$("#dataSrcMDL").modal("hide")
		.on('hidden.bs.modal', function (e) {
			$("#dataSrcMDL").off('hidden.bs.modal');

			var _additionalContent = Site.find($scope._dataSrcEntity.tags.site).dataSrcList.length > 1 ? "" : "<p class='text-muted' style='margin-left: 60px;'>(This site has only one source. Delete will remove site either.)</p>";

			$.dialog({
				title: "Delete Confirm",
				content: "<span class='text-red fa fa-exclamation-triangle pull-left' style='font-size: 50px;'></span>" +
						"<p>You are <strong class='text-red'>DELETING</strong> the data source '<strong>" + $scope._dataSrcEntity.tags.dataSource + "</strong>' of '" + $scope._dataSrcEntity.tags.site + "'!</p>" +
						"<p>Proceed to delete?</p>" + _additionalContent,
				buttons: [
					{name: "Delete", class: "btn btn-danger", value: true},
					{name: "Cancel", class: "btn btn-default", value: false},
				]
			}, function(ret) {
				if(!ret) return;

				Entities.deleteEntity("AlertDataSourceService", $scope._dataSrcEntity)._promise.then(function() {
					Site.refresh();
				});
			});
		});
	};
});