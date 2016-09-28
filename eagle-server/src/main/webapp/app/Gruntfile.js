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

module.exports = function (grunt) {
	// ==========================================================
	// =                     Parse Resource                     =
	// ==========================================================
	/*console.log('Generating resource tree...');

	var env = require('jsdom').env;
	var fs = require('fs');

	var html = fs.readFileSync('dev/index.html', 'utf8');

	console.log("1111", env);
	env(html, function (err, window) {
		console.log(">>>!!!");
		if (err) console.log(err);

		var $ = require('jquery')(window);
		var $cssList = $('link[href][rel="stylesheet"]');
		var cssList = $.map($cssList, function (ele) {
			return $(ele).attr("href");
		});

		console.log(">>>", cssList);
	});
	console.log(">>>222");*/

	// ==========================================================
	// =                      Grunt Config                      =
	// ==========================================================
	grunt.initConfig({
		config: grunt.file.readJSON('grunt.json'),

		jshint: {
			options: {
				browser: true,
				globals: {
					$: true,
					jQuery: true,
					moment: true
				}
			},
			all: [
				'dev/**/*.js'
			]
		},

		clean: {
			build: ['ui/', 'tmp/'],
			tmp: ['tmp/'],
			ui: ['ui/']
		},

		copy: {
			worker: {
				files: [
					{expand: true, cwd: 'dev/', src: '<%= config.copy.js.worker %>', dest: 'tmp'}
				]
			},
			ui: {
				files: [
					{expand: true, cwd: 'tmp/', src: ['**'], dest: 'ui'},
					{expand: true, cwd: 'dev/', src: ['public/images/**', 'partials/**'], dest: 'ui'},
					{expand: true, cwd: 'node_modules/font-awesome/', src: ['fonts/**'], dest: 'ui/public'},
					{expand: true, cwd: 'node_modules/bootstrap/', src: ['fonts/**'], dest: 'ui/public'}
				]
			}
		},

		concat: {
			js_project: '<%= config.concat.js.project %>',
			js_require: '<%= config.concat.js.require %>',
			css_require: {
				options: {
					separator: '\n',
					process: function(src) {
						return "@import url(https://fonts.googleapis.com/css?family=Source+Sans+Pro:300,400,600,700,300italic,400italic,600italic);" +
							src.replace('@import url(https://fonts.googleapis.com/css?family=Source+Sans+Pro:300,400,600,700,300italic,400italic,600italic);', '');
					}
				},
				src: '<%= config.concat.css.require.src %>',
				dest: '<%= config.concat.css.require.dest %>'
			}
		},

		'regex-replace': {
			strict: {
				src: ['tmp/public/js/project.js'],
				actions: [
					{
						name: 'use strict',
						search: '\\\'use strict\\\';?',
						replace: '',
						flags: 'gmi'
					},
					{
						name: 'build timestamp',
						search: '\\/\\/ GRUNT REPLACEMENT\\: Module\\.buildTimestamp \\= TIMESTAMP',
						replace: 'Module.buildTimestamp = ' + (+new Date()) + ';',
						flags: 'gmi'
					}
				]
			}
		},

		uglify: {
			project: {
				options: {
					mangle: false,
					sourceMap: true
				},
				files: [
					{
						src: 'tmp/public/js/project.js',
						dest: 'tmp/public/js/project.min.js'
					}
				]
			}
		},

		cssmin: {
			project: {
				files: {
					'tmp/public/css/project.min.css': '<%= config.concat.css.project.src %>',
				}
			}
		},

		htmlrefs: {
			project: {
				src: 'dev/index.html',
				dest: "tmp/index.html"
			}
		},
	});

	grunt.loadNpmTasks('grunt-contrib-jshint');
	grunt.loadNpmTasks('grunt-contrib-clean');
	grunt.loadNpmTasks('grunt-contrib-concat');
	grunt.loadNpmTasks('grunt-contrib-uglify');
	grunt.loadNpmTasks('grunt-contrib-cssmin');
	grunt.loadNpmTasks('grunt-htmlrefs');
	grunt.loadNpmTasks('grunt-regex-replace');
	grunt.loadNpmTasks('grunt-contrib-copy');

	grunt.registerTask('default', [
		// jshint
		'jshint:all',

		// Clean Env
		'clean:build',

		// Compress JS
		'copy:worker',
		'concat:js_project',
		'regex-replace:strict',
		'uglify',
		'concat:js_require',

		// Compress CSS
		'cssmin:project',
		'concat:css_require',

		// Pass HTML Resources
		'htmlrefs',
		'copy:ui',

		// Clean Env
		'clean:tmp'
	]);
};
