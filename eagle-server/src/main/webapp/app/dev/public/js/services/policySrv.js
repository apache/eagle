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

	serviceModule.service('Policy', function($q, UI, Entity) {
		var Policy = {
			publisherTypes: {},
			publisherTypeList: Entity.queryMetadata('publishmentTypes'),
			/* publisherTypes: {
				'org.apache.eagle.alert.engine.publisher.impl.AlertEmailPublisher': {
					name: "Email",
					displayFields: ["recipients"],
					fields: ["subject", "template", "sender", "recipients"]
				},
				'org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher': {
					name: "Kafka",
					displayFields: ["topic"],
					fields: ["topic", "kafka_broker"]
				},
				'org.apache.eagle.alert.engine.publisher.impl.AlertSlackPublisher': {
					name: "Slack",
					displayFields: ["channels"],
					fields: ["token", "channels", "severitys", "urltemplate"]
				},
				'org.apache.eagle.alert.engine.publisher.impl.AlertEagleStorePlugin': {
					name: "Storage",
					displayFields: [],
					fields: []
				}
			}, */

			delete: function (policy) {
				var deferred = $q.defer();

				UI.deleteConfirm(policy.name)(function (entity, closeFunc) {
					Entity.deleteMetadata("policies/" + policy.name)._promise.finally(function () {
						closeFunc();
						deferred.resolve();
					});
				}, function () {
					deferred.reject();
				});

				return deferred.promise;
			},

			start: function (policy) {
				return Entity.post("metadata/policies/" + encodeURIComponent(policy.name) + "/status/ENABLED", {})._promise;
			},

			stop: function (policy) {
				return Entity.post("metadata/policies/" + encodeURIComponent(policy.name) + "/status/DISABLED", {})._promise;
			}
		};

		Policy.publisherTypeList._then(function () {
			$.each(Policy.publisherTypeList, function (i, type) {
				type.displayFields = type.displayFields || type.fields;
				Policy.publisherTypes[type.type] = type;
			});
		});

		Policy.getSeverityClass = function (severity) {
			switch ((severity || "").toUpperCase()) {
				case "WARNING":
					return "warning";
				case "CRITICAL":
				case "FATAL":
					return "danger";
				case "OK":
					return "success";
			}
			return "default";
		};

		return Policy;
	});
})();
