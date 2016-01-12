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

	var featureControllers = angular.module("featureControllers");
	var feature = featureControllers.register("common");

	// ==============================================================
	// =                          Function                          =
	// ==============================================================
	feature.service("Policy", function(Entities) {
		var Policy = function () {};

		Policy.updatePolicyStatus = function(policy, status) {
			$.dialog({
				title: "Confirm",
				content: "Do you want to " + (status ? "enable" : "disable") + " policy[" + policy.tags.policyId + "]?",
				confirm: true
			}, function(ret) {
				if(ret) {
					policy.enabled = status;
					Entities.updateEntity("AlertDefinitionService", policy);
				}
			});
		};
		Policy.deletePolicy = function(policy, callback) {
			$.dialog({
				title: "Confirm",
				content: "Do you want to delete policy[" + policy.tags.policyId + "]?",
				confirm: true
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
		};
		return Policy;
	});

	// ==============================================================
	// =                        Configuration                       =
	// ==============================================================
	feature.configNavItem("dataSrcConfig", "Data Source", "database");
	feature.configController('dataSrcConfig', function(PageConfig, Site, $scope, $q, Entities) {
		PageConfig.hideSite = true;

		$scope._siteEntity = null;
		$scope._siteEntityLock = false;

		$scope._dataSrcEntity = null;
		$scope._dataSrcEntityLock = false;

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
				promiseList = [];
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
						Site.reload();
					});
			}).finally(function() {
				$scope._siteEntityLock = false;
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
							Site.reload();
						});
					});
				});
		};
	});

	// ==============================================================
	// =                          Policies                          =
	// ==============================================================

	// ======================= Policy Summary =======================
	feature.navItem("summary", "Policies", "list");
	feature.controller('summary', function(PageConfig, Site, $scope, $q, Entities) {
		PageConfig.pageSubTitle = Site.current().name;

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
					var _siteHref = $("<a>").attr('href', '#/common/siteList').text("Setup");
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

	// ========================= Policy List ========================
	feature.controller('policyList', function(PageConfig, Site, $scope, $wrapState, Entities, Policy) {
		PageConfig.pageTitle = "Policy List";
		PageConfig.pageSubTitle = Site.current().name;
		PageConfig
			.addNavPath("Policy View", "/common/summary")
			.addNavPath("Policy List");

		// Initial load
		$scope.policyList = [];
		if($wrapState.param.filter) {
			$scope.dataSource = Site.current().dataSrcList.find($wrapState.param.filter);
		}

		// List policies
		var _policyList = Entities.queryEntities("AlertDefinitionService", {site: Site.current().name, dataSource: $wrapState.param.filter});
		_policyList._promise.then(function() {
			$.each(_policyList, function(i, policy) {
				if($.inArray(policy.tags.dataSource, app.config.dataSource.uiInvisibleList) === -1) {
					policy.__mailStr = common.getValueByPath(common.parseJSON(policy.notificationDef, {}), "0.recipients", "");
					policy.__mailList = policy.__mailStr.trim() === "" ? [] : policy.__mailStr.split(/[,;]/);
					policy.__expression = common.parseJSON(policy.policyDef, {}).expression;

					$scope.policyList.push(policy);
				}
			});
		});
		$scope.policyList._promise = _policyList._promise;

		// Function
		$scope.searchFunc = function(item) {
			var key = $scope.search;
			if(!key) return true;

			var _key = key.toLowerCase();
			function _hasKey(item, path) {
				var _value = common.getValueByPath(item, path, "").toLowerCase();
				return _value.indexOf(_key) !== -1;
			}
			return _hasKey(item, "tags.policyId") || _hasKey(item, "__expression") || _hasKey(item, "desc") || _hasKey(item, "owner") || _hasKey(item, "__mailStr");
		};

		$scope.updatePolicyStatus = Policy.updatePolicyStatus;
		$scope.deletePolicy = function(policy) {
			Policy.deletePolicy(policy, function(policy) {
				var _index = $scope.policyList.indexOf(policy);
				$scope.policyList.splice(_index, 1);
			});
		};
	});

	// ======================= Policy Detail ========================
	feature.controller('policyDetail', function(PageConfig, Site, $scope, $wrapState, Entities, Policy, nvd3) {
		var MAX_PAGESIZE = 10000;

		PageConfig.pageTitle = "Policy Detail";
		PageConfig.lockSite = true;
		PageConfig
			.addNavPath("Policy View", "/common/summary")
			.addNavPath("Policy List", "/common/policyList")
			.addNavPath("Policy Detail");

		$scope.common = common;
		$scope.chartConfig = {
			chart: "line",
			xType: "time"
		};

		// Query policy
		if($wrapState.param.filter) {
			$scope.policyList = Entities.queryEntity("AlertDefinitionService", $wrapState.param.filter);
		} else {
			// TODO: Must: Check this when alert finished!
			$scope.policyList = Entities.queryEntities("AlertDefinitionService", {
				policyId: $wrapState.param.policy,
				site: $wrapState.param.site,
				alertExecutorId: $wrapState.param.executor
			});
		}
		$scope.policyList._promise.then(function() {
			var policy = null;

			if($scope.policyList.length === 0) {
				$.dialog({
					title: "OPS!",
					content: "Policy not found!"
				}, function() {
					location.href = "#/common/policyList";
				});
				return;
			} else {
				policy = $scope.policyList[0];

				policy.__mailStr = common.getValueByPath(common.parseJSON(policy.notificationDef, {}), "0.recipients", "");
				policy.__mailList = policy.__mailStr.trim() === "" ? [] : policy.__mailStr.split(/[,;]/);
				policy.__expression = common.parseJSON(policy.policyDef, {}).expression;

				$scope.policy = policy;
				Site.current(Site.find($scope.policy.tags.site));
				console.log($scope.policy);
			}

			// Visualization
			var _endTime = app.time.now().hour(23).minute(59).second(59).millisecond(0);
			var _startTime = _endTime.clone().subtract(1, "month").hour(0).minute(0).second(0).millisecond(0);
			var _cond = {
				dataSource: policy.tags.dataSource,
				policyId: policy.tags.policyId,
				_startTime: _startTime,
				_endTime: _endTime
			};

			// > eagle.policy.eval.count
			$scope.policyEvalSeries = nvd3.convert.eagle([Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.policy.eval.count"}, _cond), "@cluster", "sum(value)", 60 * 24)]);

			// > eagle.policy.eval.fail.count
			$scope.policyEvalFailSeries = nvd3.convert.eagle([Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.policy.eval.fail.count"}, _cond), "@cluster", "sum(value)", 60 * 24)]);

			// > eagle.alert.count
			$scope.alertSeries = nvd3.convert.eagle([Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.alert.count"}, _cond), "@cluster", "sum(value)", 60 * 24)]);

			// > eagle.alert.fail.count
			$scope.alertFailSeries = nvd3.convert.eagle([Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.alert.fail.count"}, _cond), "@cluster", "sum(value)", 60 * 24)]);

			// Alert list
			$scope.alertList = Entities.queryEntities("AlertService", {
				site: Site.current().name,
				dataSource: policy.tags.dataSource,
				policyId: policy.tags.policyId,
				_pageSize: MAX_PAGESIZE,
				_duration: 1000 * 60 * 60 * 24 * 30,
				__ETD: 1000 * 60 * 60 * 24
			});
		});

		// Function
		$scope.updatePolicyStatus = Policy.updatePolicyStatus;
		$scope.deletePolicy = function(policy) {
			Policy.deletePolicy(policy, function() {
				location.href = "#/common/policyList";
			});
		};
	});

	// ======================== Policy Edit =========================
	function policyCtrl(create, PageConfig, Site, Policy, $scope, $wrapState, $q, Entities) {
		PageConfig.pageTitle = create ? "Policy Create" : "Policy Edit";
		PageConfig.pageSubTitle = Site.current().name;
		PageConfig
			.addNavPath("Policy View", "/common/summary")
			.addNavPath("Policy List", "/common/policyList")
			.addNavPath("Policy Edit");

		var _winTimeDesc = "Number unit[millisecond, sec, min, hour, day, month, year]. e.g. 23 sec";
		var _winTimeRegex = /^\d+\s+(millisecond|milliseconds|second|seconds|sec|minute|minutes|min|hour|hours|day|days|week|weeks|month|months|year|years)$/;
		var _winTimeDefaultValue = '10 min';
		$scope.config = {
			window: [
				// Display name, window type, required columns[Title, Description || "LONG_FIELD", Regex check, default value]
				{
					title: "Message Time Slide",
					description: "Using timestamp filed from input is used as event's timestamp",
					type: "externalTime",
					fields:[
						{title: "Field", defaultValue: "timestamp", hide: true},
						{title: "Time", description: _winTimeDesc, regex: _winTimeRegex, defaultValue: _winTimeDefaultValue}
					]
				},
				{
					title: "System Time Slide",
					description: "Using System time is used as timestamp for event's timestamp",
					type: "time",
					fields:[
						{title: "Time", description: _winTimeDesc, regex: _winTimeRegex, defaultValue: _winTimeDefaultValue}
					]
				},
				{
					title: "System Time Batch",
					description: "Same as System Time Window except the policy is evaluated at fixed duration",
					type: "timeBatch",
					fields:[
						{title: "Time", description: _winTimeDesc, regex: _winTimeRegex, defaultValue: _winTimeDefaultValue}
					]
				},
				{
					title: "Length Slide",
					description: "The slide window has a fixed length",
					type: "length",
					fields:[
						{title: "Number", description: "Number only. e.g. 1023", regex: /^\d+$/}
					]
				},
				{
					title: "Length Batch",
					description: "Same as Length window except the policy is evaluated in batch mode when fixed event count reached",
					type: "lengthBatch",
					fields:[
						{title: "Number", description: "Number only. e.g. 1023", regex: /^\d+$/}
					]
				}
			]
		};

		$scope.create = create;
		$scope.encodedRowkey = $wrapState.param.filter;

		$scope.step = 0;
		$scope.dataSources = {};
		$scope.policy = {};

		// ==========================================
		// =            Data Preparation            =
		// ==========================================
		// Steam list
		var _streamList = Entities.queryEntities("AlertStreamService", '@streamName=~".*"');
		var _executorList = Entities.queryEntities("AlertExecutorService", '@streamName=~".*"');
		$scope.streamList = _streamList;
		$scope.executorList = _executorList;
		$scope.streamReady = false;

		$q.all([_streamList._promise, _executorList._promise]).then(function() {
			// Map executor with stream
			$.each(_executorList, function(i, executor) {
				$.each(_streamList, function(j, stream) {
					if(stream.tags.dataSource === executor.tags.dataSource && stream.tags.streamName === executor.tags.streamName) {
						stream.alertExecutor = executor;
						return false;
					}
				});
			});

			// Fill stream list
			$.each(_streamList, function(i, unit) {
				var _srcStreamList = $scope.dataSources[unit.tags.dataSource] = $scope.dataSources[unit.tags.dataSource] || [];
				_srcStreamList.push(unit);
			});

			$scope.streamReady = true;

			// ==========================================
			// =                Function                =
			// ==========================================
			function _findStream(dataSource, streamName) {
				var _streamList = $scope.dataSources[dataSource];
				if(!_streamList) return null;

				for(var i = 0 ; i < _streamList.length ; i += 1) {
					if(_streamList[i].tags.streamName === streamName) {
						return _streamList[i];
					}
				}
				return null;
			}

			// ==========================================
			// =              Step control              =
			// ==========================================
			$scope.steps = [
				// >> Select data source
				{
					title: "Select Data Source",
					ready: function() {
						return $scope.streamReady;
					},
					init: function() {
						if(create) $scope.policy.tags.dataSource = $scope.policy.tags.dataSource || Site.current().dataSrcList[0].tags.dataSource;
					},
					nextable: function() {
						return common.getValueByPath($scope.policy, "tags.dataSource");
					}
				},

				// >> Select stream
				{
					title: "Select Stream",
					ready: function() {
						return $scope.streamReady;
					},
					init: function() {
						$scope.policy.__.streamName = $scope.policy.__.streamName ||
							common.array.find($scope.policy.tags.dataSource, _streamList, "tags.dataSource").tags.streamName;
					},
					nextable: function() {
						var _streamName = common.getValueByPath($scope.policy, "__.streamName");
						if(!_streamName) return false;

						// Detect stream in current data source list
						return !!common.array.find(_streamName, $scope.dataSources[$scope.policy.tags.dataSource], "tags.streamName");
					}
				},

				// >> Define Alert Policy
				{
					title: "Define Alert Policy",
					init: function() {
						// Normal mode will fetch meta list
						if(!$scope.policy.__.advanced) {
							var _stream = _findStream($scope.policy.tags.dataSource, $scope.policy.__.streamName);
							$scope._stream = _stream;

							if(!_stream.metas) {
								_stream.metas = Entities.queryEntities("AlertStreamSchemaService", {dataSource: $scope.policy.tags.dataSource, streamName: $scope.policy.__.streamName});
								_stream.metas._promise.then(function() {
									_stream.metas.sort(function(a, b) {
										if(a.tags.attrName < b.tags.attrName) {
											return -1;
										} else if(a.tags.attrName > b.tags.attrName) {
											return 1;
										}
										return 0;
									});
								});
							}
						}
					},
					ready: function() {
						if(!$scope.policy.__.advanced) {
							return $scope._stream.metas._promise.$$state.status === 1;
						}
						return true;
					},
					nextable: function() {
						if($scope.policy.__.advanced) {
							// Check stream source
							$scope._stream = null;
							$.each($scope.dataSources[$scope.policy.tags.dataSource], function(i, stream) {
								if(($scope.policy.__._expression || "").indexOf(stream.tags.streamName) !== -1) {
									$scope._stream = stream;
									return false;
								}
							});
							return $scope._stream;
						} else {
							// Window
							if($scope.policy.__.windowConfig) {
								var _winMatch = true;
								var _winConds = $scope.getWindow().fields;
								$.each(_winConds, function(i, cond) {
									if(!(cond.val || "").match(cond.regex)) {
										_winMatch = false;
										return false;
									}
								});
								if(!_winMatch) return false;

								// Aggregation
								if($scope.policy.__.groupAgg) {
									if(!$scope.policy.__.groupAggPath ||
										!$scope.policy.__.groupCondOp ||
										!$scope.policy.__.groupCondVal) {
										return false;
									}
								}
							}
						}
						return true;
					}
				},

				// >> Configuration & Notification
				{
					title: "Configuration & Notification",
					nextable: function() {
						return !!$scope.policy.tags.policyId;
					}
				}
			];

			// ==========================================
			// =              Policy Logic              =
			// ==========================================
			_streamList._promise.then(function() {
				// Initial policy entity
				if(create) {
					$scope.policy = {
						__: {
							toJSON: jQuery.noop,
							conditions: {},
							notification: [],
							dedupe: {
								alertDedupIntervalMin: 10,
								emailDedupIntervalMin: 10
							},
							policy: {},
							window: "externalTime",
							group: "",
							groupAgg: "count",
							groupAggPath: "timestamp",
							groupCondOp: ">=",
							groupCondVal: "2"
						},
						desc: "",
						enabled: true,
						prefix: "alertdef",
						remediationDef: "",
						tags: {
							policyType: "siddhiCEPEngine"
						}
					};

					// If configured data source
					if($wrapState.param.dataSrc) {
						$scope.policy.tags.dataSource = $wrapState.param.dataSrc;
						if(common.array.find($wrapState.param.dataSrc, Site.current().dataSrcList, "tags.dataSource")) {
							setTimeout(function() {
								$scope.changeStep(0, 2, false);
								$scope.$apply();
							}, 1);
						}
					}
				} else {
					var _policy = Entities.queryEntity("AlertDefinitionService", $scope.encodedRowkey);
					_policy._promise.then(function() {
						if(_policy.length) {
							$scope.policy = _policy[0];
							$scope.policy.__ = {
								toJSON: jQuery.noop
							};

							Site.current(Site.find($scope.policy.tags.site));
						} else {
							$.dialog({
								title: "OPS",
								content: "Policy not found!"
							}, function() {
								$location.path("/common/policyList");
								$scope.$apply();
							});
							return;
						}

						// === Revert inner data ===
						// >> De-dupe
						$scope.policy.__.dedupe = common.parseJSON($scope.policy.dedupeDef, {});

						// >> Notification
						$scope.policy.__.notification = common.parseJSON($scope.policy.notificationDef, []);

						// >> Policy
						var _policyUnit = $scope.policy.__.policy = common.parseJSON($scope.policy.policyDef);

						// >> Parse expression
						$scope.policy.__.conditions = {};
						var _condition = _policyUnit.expression.match(/from\s+(\w+)(\[(.*)])?(#window[^\)]*\))?\s+(select (\w+, )?(\w+)\((\w+)\) as [\w\d_]+ (group by (\w+) )?having ([\w\d_]+) ([<>=]+) ([^\s]+))?/);
						var _cond_stream = _condition[1];
						var _cond_query = _condition[3] || "";
						var _cond_window = _condition[4];
						var _cond_group = _condition[5];
						var _cond_groupUnit = _condition.slice(7,14);

						if(!_condition) {
							$scope.policy.__.advanced = true;
						} else {
							// > StreamName
							var _streamName = _cond_stream;
							var _cond = _cond_query;

							$scope.policy.__.streamName = _streamName;

							//  > Conditions
							// Loop condition groups
							if(_cond.trim() !== "" && /^\(.*\)$/.test(_cond)) {
								var _condGrps = _cond.substring(1, _cond.length - 1).split(/\)\s+and\s+\(/);
								$.each(_condGrps, function(i, line) {
									// Loop condition cells
									var _condCells = line.split(/\s+or\s+/);
									$.each(_condCells, function(i, cell) {
										var _opMatch = cell.match(/(\S*)\s*(==|!=|>|<|>=|<=|contains)\s*('(?:[^'\\]|\\.)*'|[\w\d]+)/);
										if(!common.isEmpty(_opMatch)) {
											var _key = _opMatch[1];
											var _op = _opMatch[2];
											var _val = _opMatch[3];
											var _conds = $scope.policy.__.conditions[_key] = $scope.policy.__.conditions[_key] || [];
											var _type = "";
											if(_val.match(/'.*'/)) {
												_val = _val.slice(1, -1);
												_type = "string";
											} else if(_val === "true" || _val === "false") {
												var _regexMatch = _key.match(/^str:regexp\((\w+),'(.*)'\)/);
												var _containsMatch = _key.match(/^str:contains\((\w+),'(.*)'\)/);
												var _mathes = _regexMatch || _containsMatch;
												if(_mathes) {
													_key = _mathes[1];
													_val = _mathes[2];
													_type = "string";
													_op = _regexMatch ? "regex" : "contains";
													_conds = $scope.policy.__.conditions[_key] = $scope.policy.__.conditions[_key] || [];
												} else {
													_type = "bool";
												}
											} else {
												_type = "number";
											}
											_conds.push($scope._CondUnit(_key, _op, _val, _type));
										}
									});
								});
							} else if(_cond_query !== "") {
								$scope.policy.__.advanced = true;
							}
						}

						if($scope.policy.__.advanced) {
							$scope.policy.__._expression = _policyUnit.expression;
						} else {
							// > window
							if(!_cond_window) {
								$scope.policy.__.window = "externalTime";
								$scope.policy.__.group = "";
								$scope.policy.__.groupAgg = "count";
								$scope.policy.__.groupAggPath = "timestamp";
								$scope.policy.__.groupCondOp = ">=";
								$scope.policy.__.groupCondVal = "2";
							} else {
								try {
									$scope.policy.__.windowConfig = true;

									var _winCells = _cond_window.match(/\.(\w+)\((.*)\)/);
									$scope.policy.__.window = _winCells[1];
									var _winConds = $scope.getWindow().fields;
									$.each(_winCells[2].split(","), function(i, val) {
										_winConds[i].val = val;
									});

									// Group
									if(_cond_group) {
										$scope.policy.__.group = _cond_groupUnit[3];
										$scope.policy.__.groupAgg = _cond_groupUnit[0];
										$scope.policy.__.groupAggPath = _cond_groupUnit[1];
										$scope.policy.__.groupAggAlias = _cond_groupUnit[4] === "aggValue" ? "" : _cond_groupUnit[4];
										$scope.policy.__.groupCondOp = _cond_groupUnit[5];
										$scope.policy.__.groupCondVal = _cond_groupUnit[6];
									} else {
										$scope.policy.__.group = "";
										$scope.policy.__.groupAgg = "count";
										$scope.policy.__.groupAggPath = "timestamp";
										$scope.policy.__.groupCondOp = ">=";
										$scope.policy.__.groupCondVal = "2";
									}
								} catch(err) {
									$scope.policy.__.window = "externalTime";
								}
							}
						}

						$scope.changeStep(0, 3, false);
					});
				}

				// Start step
				$scope.changeStep(0, 1, false);

				console.log($scope.policy, $scope);
			});

			// ==========================================
			// =                Function                =
			// ==========================================
			// UI: Highlight select step
			$scope.stepSelect = function(step) {
				return step === $scope.step ? "active" : "";
			};

			// UI: Collapse all
			$scope.collapse = function(cntr) {
				var _list = $(cntr).find(".collapse").css("height", "auto");
				if(_list.hasClass("in")) {
					_list.removeClass("in");
				} else {
					_list.addClass("in");
				}
			};

			// Step process. Will fetch target step attribute and return boolean
			function _check(key, step) {
				var _step = $scope.steps[step - 1];
				if(!_step) return;

				var _value = _step[key];
				if(typeof _value === "function") {
					return _value();
				} else if(typeof _value === "boolean") {
					return _value;
				}
				return true;
			}
			// Check step is ready. Otherwise will display load animation
			$scope.stepReady = function(step) {
				return _check("ready", step);
			};
			// Check whether process next step. Otherwise will disable next button
			$scope.checkNextable = function(step) {
				return !_check("nextable", step);
			};
			// Switch step
			$scope.changeStep = function(step, targetStep, check) {
				if(check === false || _check("checkStep", step)) {
					$scope.step =  targetStep;

					_check("init", targetStep);
				}
			};

			// Window
			$scope.getWindow = function() {
				if(!$scope.policy || !$scope.policy.__) return null;
				return common.array.find($scope.policy.__.window, $scope.config.window, "type");
			};

			// Aggregation
			$scope.groupAggPathList = function() {
				return $.grep(common.getValueByPath($scope, "_stream.metas", []), function(meta) {
					return $.inArray(meta.attrType, ['long','integer','number', 'double', 'float']) !== -1;
				});
			};

			$scope.updateGroupAgg = function() {
				$scope.policy.__.groupAggPath = $scope.policy.__.groupAggPath || common.getValueByPath($scope.groupAggPathList()[0], "tags.attrName");

				if($scope.policy.__.groupAgg === 'count') {
					$scope.policy.__.groupAggPath = 'timestamp';
				}
			};

			// Resolver
			$scope.resolverTypeahead = function(value, resolver) {
				var _resolverList = Entities.query("stream/attributeresolve", {
					site: Site.current().name,
					resolver: resolver,
					query: value
				});
				return _resolverList._promise.then(function() {
					return _resolverList;
				});
			};

			// Used for input box when pressing enter
			$scope.conditionPress = function(event) {
				if(event.which == 13) {
					setTimeout(function() {
						$(event.currentTarget).closest(".input-group").find("button").click();
					}, 1);
				}
			};
			// Check whether has condition
			$scope.hasCondition = function(key, type) {
				var _list = common.getValueByPath($scope.policy.__.conditions, key, []);
				if(_list.length === 0) return false;

				if(type === "bool") {
					return !_list[0].ignored();
				}
				return true;
			};
			// Condition unit definition
			$scope._CondUnit = function(key, op, value, type) {
				return {
					key: key,
					op: op,
					val: value,
					type: type,
					ignored: function() {
						return this.type === "bool" && this.val === "none";
					},
					getVal: function() {
						return this.type === "string" ? "'" + this.val + "'" : this.val;
					},
					toString: function() {
						return this.op + " " + this.getVal();
					},
					toCondString: function() {
						var _op = this.op === "=" ? "==" : this.op;
						if(_op === "regex") {
							return "str:regexp(" + this.key + "," + this.getVal() + ")==true";
						} else if(_op === "contains") {
							return "str:contains(" + this.key + "," + this.getVal() + ")==true";
						} else {
							return this.key + " " + _op + " " + this.getVal();
						}
					}
				};
			};
			// Add condition for policy
			$scope.addCondition = function(key, op, value, type) {
				if(value === "" || value === undefined) return false;

				var _condList = $scope.policy.__.conditions[key] = $scope.policy.__.conditions[key] || [];
				_condList.push($scope._CondUnit(key, op, value, type));
				return true;
			};
			// Convert condition list to description string
			$scope.parseConditionDesc = function(key) {
				return $.map($scope.policy.__.conditions[key] || [], function(cond) {
					if(!cond.ignored()) return "[" + cond.toString() + "]";
				}).join(" or ");
			};

			// To query
			$scope.toQuery = function() {
				if(!$scope.policy.__) return "";

				if($scope.policy.__.advanced) return $scope.policy.__._expression;

				// > Query
				var _query = $.map(common.getValueByPath($scope.policy, "__.conditions", {}), function(list) {
					var _conds = $.map(list, function(cond) {
						if(!cond.ignored()) return cond.toCondString();
					});
					if(_conds.length) {
						return "(" + _conds.join(" or ") + ")";
					}
				}).join(" and ");
				if(_query) {
					_query = "[" + _query + "]";
				}

				// > Window
				var _window = $scope.getWindow();
				var _windowStr = "";
				if($scope.policy.__.windowConfig) {
					_windowStr = $.map(_window.fields, function(field) {
						return field.val;
					}).join(",");
					_windowStr = "#window." + _window.type + "(" + _windowStr + ")";

					// > Group
					if($scope.policy.__.group) {
						_windowStr += common.template(" select ${group}, ${groupAgg}(${groupAggPath}) as ${groupAggAlias} group by ${group} having ${groupAggAlias} ${groupCondOp} ${groupCondVal}", {
							group: $scope.policy.__.group,
							groupAgg: $scope.policy.__.groupAgg,
							groupAggPath: $scope.policy.__.groupAggPath,
							groupCondOp: $scope.policy.__.groupCondOp,
							groupCondVal: $scope.policy.__.groupCondVal,
							groupAggAlias: $scope.policy.__.groupAggAlias || "aggValue"
						});
					} else {
						_windowStr += common.template(" select ${groupAgg}(${groupAggPath}) as ${groupAggAlias} having ${groupAggAlias} ${groupCondOp} ${groupCondVal}", {
							groupAgg: $scope.policy.__.groupAgg,
							groupAggPath: $scope.policy.__.groupAggPath,
							groupCondOp: $scope.policy.__.groupCondOp,
							groupCondVal: $scope.policy.__.groupCondVal,
							groupAggAlias: $scope.policy.__.groupAggAlias || "aggValue"
						});
					}
				} else {
					_windowStr = " select *";
				}

				return common.template("from ${stream}${query}${window} insert into outputStream;", {
					stream: $scope.policy.__.streamName,
					query: _query,
					window: _windowStr
				});
			};

			// ==========================================
			// =             Update Policy              =
			// ==========================================
			// dedupeDef notificationDef policyDef
			$scope.finishPolicy = function() {
				$scope.lock = true;

				// dedupeDef
				$scope.policy.dedupeDef = JSON.stringify($scope.policy.__.dedupe);

				// notificationDef
				$scope.policy.__.notification = $scope.policy.__.notification || [];
				var _notificationUnit = $scope.policy.__.notification[0];
				if(_notificationUnit) {
					_notificationUnit.flavor = "email";
					_notificationUnit.id = "email_1";
					_notificationUnit.tplFileName = "";
				}
				$scope.policy.notificationDef = JSON.stringify($scope.policy.__.notification);

				// policyDef
				$scope.policy.__._dedupTags = $scope.policy.__._dedupTags || {};
				$scope.policy.__.policy = {
					expression: $scope.toQuery(),
					type: "siddhiCEPEngine"
				};
				$scope.policy.policyDef = JSON.stringify($scope.policy.__.policy);

				// alertExecutorId
				if($scope._stream.alertExecutor) {
					$scope.policy.tags.alertExecutorId = $scope._stream.alertExecutor.tags.alertExecutorId;
				} else {
					$scope.lock = false;
					$.dialog({
						title: "OPS!",
						content: "Alert Executor not defined! Please check 'AlertExecutorService'!"
					});
					return;
				}

				// site
				$scope.policy.tags.site = $scope.policy.tags.site || Site.current().name;

				// Update function
				function _updatePolicy() {
					Entities.updateEntity("AlertDefinitionService", $scope.policy)._promise.success(function(data) {
						$scope.create = create = false;
						$scope.encodedRowkey = data.obj[0];

						$.dialog({
							title: "Success",
							content: (create ? "Create" : "Update") + " success!"
						}, function() {
							if(data.success) {
								location.href = "#/common/policyList";
							} else {
								$.dialog({
									title: "OPS",
									content: (create ? "Create" : "Update") + "failed!" + JSON.stringify(data)
								});
							}
						});
					}).error(function(data) {
						$.dialog({
							title: "OPS",
							content: (create ? "Create" : "Update") + "failed!" + JSON.stringify(data)
						});
					}).then(function() {
						$scope.lock = false;
					});
				}

				// Check if already exist
				if($scope.create) {
					var _checkList = Entities.queryEntities("AlertDefinitionService", {
						alertExecutorId: $scope.policy.tags.alertExecutorId,
						policyId: $scope.policy.tags.policyId,
						policyType: "siddhiCEPEngine",
						dataSource: $scope.policy.tags.dataSource
					});
					_checkList._promise.then(function() {
						if(_checkList.length) {
							$.dialog({
								title: "Override Confirm",
								content: "Already exists PolicyID '" + $scope.policy.tags.policyId + "'. Do you want to override?",
								confirm: true
							}, function(ret) {
								if(ret) {
									_updatePolicy();
								} else {
									$scope.lock = false;
									$scope.$apply();
								}
							});
						} else {
							_updatePolicy();
						}
					});
				} else {
					_updatePolicy();
				}
			};
		});
	}

	feature.controller('policyCreate', function(PageConfig, Site, Policy, $scope, $wrapState, $q, Entities) {
		policyCtrl(true, PageConfig, Site, Policy, $scope, $wrapState, $q, Entities);
	}, "policyEdit");
	feature.controller('policyEdit', function(PageConfig, Site, Policy, $scope, $wrapState, $q, Entities) {
		PageConfig.lockSite = true;
		policyCtrl(false, PageConfig, Site, Policy, $scope, $wrapState, $q, Entities);
	});

	// ==============================================================
	// =                           Alerts                           =
	// ==============================================================

	// ========================= Alert List =========================
	feature.navItem("alertList", "Alerts", "exclamation-triangle");
	feature.controller('alertList', function(PageConfig, Site, $scope, $wrapState, $interval, $timeout, Entities) {
		PageConfig.pageSubTitle = Site.current().name;

		var MAX_PAGESIZE = 10000;

		// Initial load
		$scope.dataSource = $wrapState.param.dataSource;

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

	// ========================= Alert List =========================
	feature.controller('alertDetail', function(PageConfig, Site, $scope, $wrapState, Entities) {
		PageConfig.pageTitle = "Alert Detail";
		PageConfig.lockSite = true;
		PageConfig
			.addNavPath("Alert List", "/common/alertList")
			.addNavPath("Alert Detail");

		$scope.common = common;

		// Query policy
		$scope.alertList = Entities.queryEntity("AlertService", $wrapState.param.filter);
		$scope.alertList._promise.then(function() {
			if($scope.alertList.length === 0) {
				$.dialog({
					title: "OPS!",
					content: "Alert not found!"
				}, function() {
					location.href = "#/common/alertList";
				});
			} else {
				$scope.alert = $scope.alertList[0];
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
})();