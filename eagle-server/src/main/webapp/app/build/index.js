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
	'use strict';
	console.log('Generating resource tree...');

	var env = require('jsdom').env;
	var fs = require('fs');

	// Parse tree
	fs.readFile('dev/index.html', 'utf8', function (err, html) {
		if (err) return console.log(err);

		env(html, function (err, window) {
			if (err) console.log(err);

			// Get js / css resource
			var $ = require('jquery')(window);
			function getResList(match, attr) {
				var $eleList = $(match);
				var requireList = [];
				var projectList = [];
				var list = [];

				$.each($eleList, function (i, ele) {
					var path = $(ele).attr(attr);

					if(path.match(/^apps/)) return;

					if(path.match(/node_modules/)) {
						requireList.push(path.replace(/\.\.\//, ""));
						list.push(path.replace(/\.\.\//, ""));
					} else {
						projectList.push("dev/" + path);
						list.push("dev/" + path);
					}
				});

				return {
					list: list,
					requireList: requireList,
					projectList: projectList
				};
			}

			var cssList = getResList('link[href][rel="stylesheet"]', 'href');
			var jsList = getResList('script[src]', 'src');

			// JS Worker process
			var workerFolderPath = 'dev/public/js/worker/';
			var workerList = fs.readdirSync(workerFolderPath);
			var workerRequireList = [];

			workerList = workerList.map(function (path) {
				if(!/\w+Worker\.js/.test(path)) return;

				var workerPath = workerFolderPath + path;
				var content = fs.readFileSync(workerPath, 'utf8');
				var regex = /self\.importScripts\(["']([^"']*)["']\)/g;
				var match;
				while ((match = regex.exec(content)) !== null) {
					var modulePath = match[1];
					workerRequireList.push((workerFolderPath + modulePath).replace(/^dev\//, ""));
				}

				return workerPath.replace(/^dev\//, "");
			}).filter(function (path) {
				return !!path;
			});

			// Parse grunt config
			var resJson = {
				concat: {
					js: {
						require: {
							options: {
								separator: '\n'
							},
							src: jsList.requireList,
							dest: 'tmp/public/js/modules.js'
						},
						project: {
							options: {
								separator: '\n',
								sourceMap :true
							},
							src: jsList.projectList,
							dest: 'tmp/public/js/doc.js'
						}
					},
					css: {
						require: {
							src: cssList.requireList.concat('tmp/public/css/project.min.css'),
							dest: 'tmp/public/css/doc.css'
						},
						project: {
							options: {
								separator: '\n'
							},
							src: cssList.projectList,
							dest: 'tmp/public/js/project.min.css'
						}
					}
				},
				copy: {
					js: {
						worker: workerList.concat(workerRequireList)
					}
				}
			};

			// Save tree & call grunt
			fs.writeFile('grunt.json', JSON.stringify(resJson, null, '\t'), 'utf8', function (err) {
				if(err) return console.log(err);

				console.log("Grunt packaging...");
				var exec = require('child_process').exec;
				var grunt = exec('npm run grunt');

				grunt.stdout.pipe(process.stdout);
				grunt.stderr.pipe(process.stdout);
				grunt.on('exit', function(code) {
					console.log("Grunt Exit Code:", code);
					process.exit(code)
				});
			});
		});
	});
})();
