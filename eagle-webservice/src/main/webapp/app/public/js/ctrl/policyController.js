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
// =                        Policy List                        =
// =============================================================
damControllers.controller('policyListCtrl', function(globalContent, Site, damContent, $scope, $routeParams, Entities) {
	globalContent.setConfig(damContent.config);
	globalContent.pageTitle = "Policy List";
	globalContent.pageSubTitle = Site.current().name;
	globalContent.navPath = ["Policy View", "Polict List"];

	// Initial load
	$scope.policyList = [];
	if($routeParams.dataSource) {
		$scope.dataSource = Site.current().dataSrcList.find($routeParams.dataSource);
	}

	// List policies
	var _policyList = Entities.queryEntities("AlertDefinitionService", {site: Site.current().name, dataSource: $routeParams.dataSource});
	_policyList._promise.then(function() {
		$.each(_policyList, function(i, policy) {
			if($.inArray(policy.tags.dataSource, app.config.dataSource.uiInvisibleList) === -1) {
				policy.__mailStr = common.getValueByPath(common.parseJSON(policy.notificationDef, {}), "0.recipients", "");
				policy.__mailList = policy.__mailStr.trim() === "" ? [] : policy.__mailStr.split(/[\,\;]/);
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

	$scope.updatePolicyStatus = damContent.updatePolicyStatus;
	$scope.deletePolicy = function(policy) {
		damContent.deletePolicy(policy, function(policy) {
			var _index = $scope.policyList.indexOf(policy);
			$scope.policyList.splice(_index, 1);
		});
	};
});

// =============================================================
// =                       Policy Detail                       =
// =============================================================
damControllers.controller('policyDetailCtrl', function(globalContent, Site, damContent, charts, $scope, $routeParams, Entities) {
	var MAX_PAGESIZE = 10000;

	globalContent.setConfig(damContent.config);
	globalContent.pageTitle = "Policy Detail";
	globalContent.navPath = ["Policy View", "Polict List", "Polict Detail"];
	globalContent.lockSite = true;

	charts = charts($scope);

	$scope.common = common;

	// Query policy
	if($routeParams.encodedRowkey) {
		$scope.policyList = Entities.queryEntity("AlertDefinitionService", $routeParams.encodedRowkey);
	} else {
		$scope.policyList = Entities.queryEntities("AlertDefinitionService", {
			policyId: $routeParams.policy,
			site: $routeParams.site,
			alertExecutorId: $routeParams.executor
		});
	}
	$scope.policyList._promise.then(function() {
		if($scope.policyList.length === 0) {
			$.dialog({
				title: "OPS!",
				content: "Policy not found!",
			}, function() {
				location.href = "#/dam/policyList";
			});
			return;
		} else {
			var policy = $scope.policyList[0];

			policy.__mailStr = common.getValueByPath(common.parseJSON(policy.notificationDef, {}), "0.recipients", "");
			policy.__mailList = policy.__mailStr.trim() === "" ? [] : policy.__mailStr.split(/[\,\;]/);
			policy.__expression = common.parseJSON(policy.policyDef, {}).expression;

			$scope.policy = policy;
			Site.current(Site.find($scope.policy.tags.site));
			console.log($scope.policy);
		}

		// Visualization
		var _cond = {
			dataSource: policy.tags.dataSource,
			policyId: policy.tags.policyId,
			_duration: 1000 * 60 * 60 * 24 * 30,
		};

		// > eagle.policy.eval.count
		$scope.policyEvalSeries = Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.policy.eval.count"}, _cond), "@cluster", "sum(value)", 60 * 24);

		// > eagle.policy.eval.fail.count
		$scope.policyEvalFailSeries = Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.policy.eval.fail.count"}, _cond), "@cluster", "sum(value)", 60 * 24);

		// > eagle.alert.count
		$scope.alertSeries = Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.alert.count"}, _cond), "@cluster", "sum(value)", 60 * 24);

		// > eagle.alert.fail.count
		$scope.alertFailSeries = Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.alert.fail.count"}, _cond), "@cluster", "sum(value)", 60 * 24);

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
	$scope.updatePolicyStatus = damContent.updatePolicyStatus;
	$scope.deletePolicy = function(policy) {
		damContent.deletePolicy(policy, function(policy) {
			location.href = "#/dam/policyList";
		});
	};
});

// =============================================================
// =                        Policy Edit                        =
// =============================================================
(function() {
	function policyCtrl(create, globalContent, Site, damContent, $scope, $routeParams, $location, $q, Entities) {
		globalContent.setConfig(damContent.config);
		globalContent.pageTitle = "Policy Edit";
		globalContent.pageSubTitle = Site.current().name;
		globalContent.navPath = ["Policy View", "Polict List", "Polict Edit"];

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
						{title: "Time", description: _winTimeDesc, regex: _winTimeRegex, defaultValue: _winTimeDefaultValue},
					]
				},
				{
					title: "System Time Slide",
					description: "Using System time is used as timestamp for event's timestamp",
					type: "time",
					fields:[
						{title: "Time", description: _winTimeDesc, regex: _winTimeRegex, defaultValue: _winTimeDefaultValue},
					]
				},
				{
					title: "System Time Batch",
					description: "Same as System Time Window except the policy is evaluated at fixed duration",
					type: "timeBatch",
					fields:[
						{title: "Time", description: _winTimeDesc, regex: _winTimeRegex, defaultValue: _winTimeDefaultValue},
					]
				},
				{
					title: "Length Slide",
					description: "The slide window has a fixed length",
					type: "length",
					fields:[
						{title: "Number", description: "Number only. e.g. 1023", regex: /^\d+$/},
					]
				},
				{
					title: "Length Batch",
					description: "Same as Length window except the policy is evaluated in batch mode when fixed event count reached",
					type: "lengthBatch",
					fields:[
						{title: "Number", description: "Number only. e.g. 1023", regex: /^\d+$/},
					]
				},
			],
		};

		$scope.create = create;
		$scope.encodedRowkey = $routeParams.encodedRowkey;

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
			};

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
					},
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
					},
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
					},
				},

				// >> Configuration & Notification
				{
					title: "Configuration & Notification",
					nextable: function() {
						return !!$scope.policy.tags.policyId;
					},
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
								emailDedupIntervalMin: 10,
							},
							policy: {},
							window: "externalTime",
							group: "",
							groupAgg: "count",
							groupAggPath: "timestamp",
							groupCondOp: ">=",
							groupCondVal: "2",
						},
						desc: "",
						enabled: true,
						prefix: "alertdef",
						remediationDef: "",
						tags: {
							policyType: "siddhiCEPEngine",
						}
					};

					// If configured data source
					if($routeParams.dataSrc) {
						$scope.policy.tags.dataSource = $routeParams.dataSrc;
						if(common.array.find($routeParams.dataSrc, Site.current().dataSrcList, "tags.dataSource")) {
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
								toJSON: jQuery.noop,
							};

							Site.current(Site.find($scope.policy.tags.site));
						} else {
							$.dialog({
								title: "OPS",
								content: "Policy not found!",
							}, function() {
								$location.path("/dam/policyList");
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
						var _condition = _policyUnit.expression.match(/from\s+(\w+)(\[(.*)\])?(#window[^\)]*\))?\s+(select (\w+\, )?(\w+)\((\w+)\) as aggValue (group by (\w+) )?having aggValue ([<>=]+) ([^\s]+))?/);
						var _cond_stream = _condition[1];
						var _cond_query = _condition[3] || "";
						var _cond_window = _condition[4];
						var _cond_group = _condition[5];
						var _cond_groupUnit = _condition.slice(7,13);

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
											if(_val.match(/\'.*\'/)) {
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
										$scope.policy.__.groupCondOp = _cond_groupUnit[4];
										$scope.policy.__.groupCondVal = _cond_groupUnit[5];
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
				var _window = common.array.find($scope.policy.__.window, $scope.config.window, "type");
				return _window;
			};

			// Aggregation
			$scope.groupAggPathList = function() {
				return $.grep(common.getValueByPath($scope, "_stream.metas", []), function(meta) {
					return $.inArray(meta.attrType, ['long','integer','number']) !== -1;
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
			$scope.conditionPress = function(event, key, operation, value, type) {
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
				var _obj = {
					key: key,
					op: op,
					val: value,
					type: type,
					ignored: function() {
						if(this.type === "bool" && this.val === "none") {
							return true;
						}
						return false;
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
					},
				};
				return _obj;
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
				var _query = $.map(common.getValueByPath($scope.policy, "__.conditions", {}), function(list, key) {
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
						_windowStr += common.template(" select ${group}, ${groupAgg}(${groupAggPath}) as aggValue group by ${group} having aggValue ${groupCondOp} ${groupCondVal}", {
							group: $scope.policy.__.group,
							groupAgg: $scope.policy.__.groupAgg,
							groupAggPath: $scope.policy.__.groupAggPath,
							groupCondOp: $scope.policy.__.groupCondOp,
							groupCondVal: $scope.policy.__.groupCondVal,
						});
					} else {
						_windowStr += common.template(" select ${groupAgg}(${groupAggPath}) as aggValue having aggValue ${groupCondOp} ${groupCondVal}", {
							groupAgg: $scope.policy.__.groupAgg,
							groupAggPath: $scope.policy.__.groupAggPath,
							groupCondOp: $scope.policy.__.groupCondOp,
							groupCondVal: $scope.policy.__.groupCondVal,
						});
					}
				} else {
					_windowStr = " select *";
				}

				return common.template("from ${stream}${query}${window} insert into outputStream;", {
					stream: $scope.policy.__.streamName,
					query: _query,
					window: _windowStr,
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
							content: (create ? "Create" : "Update") + " success!",
						}, function() {
							if(data.success) {
								Site.url(Site.find($scope.policy.tags.site), "/dam/policyList");
							} else {
								$.dialog({
									title: "OPS",
									content: (create ? "Create" : "Update") + "failed!" + JSON.stringify(data),
								});
							}
						});
					}).error(function(data) {
						$.dialog({
							title: "OPS",
							content: (create ? "Create" : "Update") + "failed!" + JSON.stringify(data),
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
						dataSource: $scope.policy.tags.dataSource,
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

	damControllers.controller('policyCreateCtrl', function(globalContent, Site, damContent, $scope, $routeParams, $location, $q, Entities) {
		policyCtrl(true, globalContent, Site, damContent, $scope, $routeParams, $location, $q, Entities);
	});
	damControllers.controller('policyEditCtrl', function(globalContent, Site, damContent, $scope, $routeParams, $location, $q, Entities) {
		globalContent.lockSite = true;
		policyCtrl(false, globalContent, Site, damContent, $scope, $routeParams, $location, $q, Entities);
	});
})();