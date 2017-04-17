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

(function () {
	/**
	 * `register` is global function that for application to set up 'controller', 'service', 'directive', 'route' in Eagle
	 */
	var systemMetricApp = register(['ngRoute', 'ngAnimate', 'ui.router', 'eagle.service']);

    systemMetricApp.route("systemMetric", {
        url: "/hadoopService/system/overview?startTime&endTime",
        site: true,
        templateUrl: "partials/overview.html",
        controller: "overviewCtrl",
        resolve: {time: true}
    }).route("serverList", {
        url: "/hadoopService/system/serverList",
        site: true,
        templateUrl: "partials/serverList.html",
        controller: "serverListCtrl"
    }).route("serverDetail", {
        url: "/hadoopService/system/serverDetail/:hostname",
        site: true,
        templateUrl: "partials/serverDetail.html",
        controller: "serverDetailCtrl",
        resolve: {time: true}
    });

    systemMetricApp.portal({
		name: "Services", icon: "heartbeat", list: [
			{name: "System", path: "hadoopService/system/overview"}
		]
	}, true);

    systemMetricApp.service("SYSTEMMETRIC", function ($q, $http, Time, Site, Application) {
        var SYSTEMMETRIC = window._SYSTEMMETRIC = {};
        SYSTEMMETRIC.STATUS_ACTIVE = "active";
        SYSTEMMETRIC.STATUS_WARNING = "warning";
        SYSTEMMETRIC.STATUS_ERROR = "error";
        SYSTEMMETRIC.QUERY_SYSTEM_METRICS = '${baseURL}/rest/entities?query=GenericMetricService[${condition}]{*}&metricName=${metric}&pageSize=${limit}';
        SYSTEMMETRIC.QUERY_SYSTEM_METRICS_WITHTIME = '${baseURL}/rest/entities?query=GenericMetricService[${condition}]{*}&metricName=${metric}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}';
        SYSTEMMETRIC.QUERY_SYSTEM_METRICS_INTERVAL = '${baseURL}/rest/entities?query=GenericMetricService[${condition}]<${groups}>{${field}}${order}${top}&metricName=${metric}&pageSize=${limit}&startTime=${startTime}&endTime=${endTime}&intervalmin=${intervalMin}&timeSeries=true';
        SYSTEMMETRIC.QUERY_SYSTEM_INSTANCE = '${baseURL}/rest/entities?query=SystemServiceInstance[${condition}]{*}&pageSize=${limit}';
        SYSTEMMETRIC.QUERY_SYSTEM_INSTANCE_AGG = "${baseURL}/rest/entities?query=SystemServiceInstance[${condition}]<${groups}>{${field}}&pageSize=${limit}";
        
        var getQuery = SYSTEMMETRIC.getQuery = function (queryName, siteId) {
            var baseURL;
            siteId = siteId || Site.current().siteId;
            var app = Application.find("SYSTEM_METRIC_WEB_APP", siteId)[0];
            var host = app.configuration["service.host"];
            var port = app.configuration["service.port"];

            if (!host && !port) {
                baseURL = "";
            } else {
                if (host === "localhost" || !host) {
                    host = location.hostname;
                }
                if (!port) {
                    port = location.port;
                }
                baseURL = "http://" + host + ":" + port;
            }

            return common.template(SYSTEMMETRIC["QUERY_" + queryName], {baseURL: baseURL});
        };

        function wrapList(promise) {
            var _list = [];
            _list._done = false;
            _list._promise = promise.then(
                /**
                 * @param {{}} res
                 * @param {{}} res.data
                 * @param {{}} res.data.obj
                 */
                function (res) {
                    _list.splice(0);
                    Array.prototype.push.apply(_list, res.data.obj);
                    _list._done = true;
                    return _list;
                });
            return _list;
        }

        function toFields(fields) {
            return (fields || []).length > 0 ? $.map(fields, function (field) {
                return "@" + field;
            }).join(",") : "*";
        }

        SYSTEMMETRIC.metricsToSeries = function (name, metrics, option, transflag, rawData) {
            if (arguments.length === 4 && typeof rawData === "object") {
                option = rawData;
                rawData = false;
            }
            var previous = 0;
            data = $.map(metrics, function (metric, index) {
                var temp = metric.value[0];
                if(transflag){
                    metric.value[0] = metric.value[0] - previous;
                }
                previous = temp;
                if(transflag && index === 0){
                    return ;
                }
                return rawData ? metric.value[0] : {
                    x: metric.timestamp,
                    y: metric.value[0]
                };
            });
           
            return $.extend({
                name: name,
                showSymbol: false,
                type: "line",
                data: data
            }, option || {});
        };

        SYSTEMMETRIC.get = function (url, params) {
            return $http({
                url: url,
                method: "GET",
                params: params
            });
        };

        SYSTEMMETRIC.condition = function (condition) {
            return $.map(condition, function (value, key) {
                return "@" + key + '="' + value + '"';
            }).join(" AND ");
        };

        SYSTEMMETRIC.aggSystemInstance = function (condition, groups, field, limit) {
            var fields = field.split(/\s*,\s*/);
            var fieldStr = $.map(fields, function (field, index) {
                var matches = field.match(/^([^\s]*)(\s+.*)?$/);
                if (matches[2]) {
                    orderId = index;
                }
                return matches[1];
            }).join(", ");
            var config = {
                condition: SYSTEMMETRIC.condition(condition),
                groups: toFields(groups),
                field: fieldStr,
                limit: limit || 10000
            };
            var metrics_url = common.template(getQuery("SYSTEM_INSTANCE_AGG"), config);
            return wrapList(SYSTEMMETRIC.get(metrics_url));
        };

        SYSTEMMETRIC.systemMetricsAggregation = function (condition, metric, groups, field, intervalMin, startTime, endTime, top, limit) {
            var fields = field.split(/\s*,\s*/);
            var orderId = -1;
            var fieldStr = $.map(fields, function (field, index) {
                var matches = field.match(/^([^\s]*)(\s+.*)?$/);
                if (matches[2]) {
                    orderId = index;
                }
                return matches[1];
            }).join(", ");


            var config = {
                condition: SYSTEMMETRIC.condition(condition),
                startTime: Time.format(startTime),
                endTime: Time.format(endTime),
                metric: metric,
                groups: toFields(groups),
                field: fieldStr,
                order: orderId === -1 ? "" : ".{" + fields[orderId] + "}",
                top: top ? "&top=" + top : "",
                intervalMin: intervalMin,
                limit: limit || 10000
            };

            var metrics_url = common.template(getQuery("SYSTEM_METRICS_INTERVAL"), config);
            var _list = wrapList(SYSTEMMETRIC.get(metrics_url));
            _list._aggInfo = {
                groups: groups,
                startTime: Time(startTime).valueOf(),
                interval: intervalMin * 60 * 1000
            };
            _list._promise.then(function () {
                _list.reverse();
            });
            return _list;
        };

        SYSTEMMETRIC.aggMetricsToEntities = function (list, param, flatten) {
            var _list = [];
            _list.done = false;
            _list._promise = list._promise.then(function () {
                var _startTime = list._aggInfo.startTime;
                var _interval = list._aggInfo.interval;
                $.each(list, function (i, obj) {
                    var tags = {};
                    $.each(list._aggInfo.groups, function (j, group) {
                        tags[group] = obj.key[j];
                    });
                    var _subList = [];
                    _subList.group = obj.key.join(",");
                    $.each(obj.value[0], function (index, value) {
                        var node = {
                            timestamp: _startTime + index * _interval,
                            value: [value],
                            tags: tags,
                            flag: param
                        };
                        _subList.push(node);
                    });
                    /*var _subList = $.map(obj.value[0], function (value, index) {
                        return {
                            timestamp: _startTime + index * _interval,
                            value: [value],
                            tags: tags,
                            flag: param
                        };
                    });*/
                    if (flatten) {
                        _list.push.apply(_list, _subList);
                    } else {
                        _list.push(_subList);
                    }
                });
                _list.done = true;
                return _list;
            }, function () {
                return [];
            });
            return _list;
        };

        SYSTEMMETRIC.hostStatus = function (condition, limit) {
            var config = {
                condition: SYSTEMMETRIC.condition(condition),
                limit: limit || 10000
            };

            var metrics_url = common.template(getQuery("SYSTEM_INSTANCE"), config);
            return wrapList(SYSTEMMETRIC.get(metrics_url));
        };


        SYSTEMMETRIC.serverList = function (siteid, status, coresCount) {
            var condition = {
                    site: siteid,
                    role: "system"
                };
            if(typeof status !== 'undefined') {
                condition.status = status;
            }
            if(typeof coresCount !== 'undefined') {
                condition.cores = coresCount;
            }
            return SYSTEMMETRIC.hostStatus(condition);
        };

        SYSTEMMETRIC.getMetricObj = function () {
            var deferred = $q.defer();
            $http.get("apps/system/config.json").success(function (resp) {
                deferred.resolve(resp);
            });
            return deferred.promise;
        };

        return SYSTEMMETRIC;
    });


    systemMetricApp.requireCSS("style/index.css");
    systemMetricApp.require("widget/availabilityChart.js");
    systemMetricApp.require("ctrl/overview.js");
    systemMetricApp.require("ctrl/serverDetailCtrl.js");
    systemMetricApp.require("ctrl/serverListCtrl.js");
})();
