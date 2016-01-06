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
	// Project configuration.
	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),
		config: grunt.file.readJSON('grunt.json'),

		jshint: {
			options: {
				browser: true,
				globals: {
					$: true,
					jQuery: true,
					moment: true,
				},
			},
			all: [
				'app/**/*.js'
			],
		},

		clean: {
			build: ['ui/', 'tmp/'],
			tmp: ['tmp/'],
			ui: ['ui/'],
		},
		concat: {
			app: {
				src: [
					'app/public/js/app.js',
					'app/public/js/app.*.js',

					'app/public/js/common.js',

					'app/public/js/components/main.js',
					'app/public/js/components/**.js',
					'app/public/js/components/**/**.js',

					'app/public/js/ctrl/damController.js',
					'app/public/js/ctrl/*.js',
				],
				dest: 'tmp/public/js/scripts.js'
			},
			js: '<%= config.concat.js %>',
			css: {
				options: {
					process: function(src, filepath) {
						return "@import url(https://fonts.googleapis.com/css?family=Source+Sans+Pro:300,400,600,700,300italic,400italic,600italic);" +
						src.replace('@import url(https://fonts.googleapis.com/css?family=Source+Sans+Pro:300,400,600,700,300italic,400italic,600italic);', '');
					}
				},
				src: '<%= config.concat.css.src %>',
				dest: '<%= config.concat.css.dest %>',
			}
		},
		'regex-replace': {
			strict: {
				src: ['tmp/public/js/scripts.js'],
				actions: [
					{
						name: 'use strict',
						search: '\\\'use strict\\\';?',
						replace: '',
						flags: 'gmi'
					}
				]
			},
		},
		uglify: {
			ui: {
				options: {
					mangle: false
				},
				src: 'tmp/public/js/scripts.js',
				dest: 'tmp/public/js/scripts.min.js'
			}
		},
		cssmin: {
			ui: {
				files: {
					'tmp/public/css/styles.css': ['app/public/css/main.css']
				}
			}
		},
		htmlrefs: {
			ui: {
				src: 'app/index.html',
				dest: "tmp/index.html",
			}
		},
		copy: {
			ui: {
				files: [
					{expand: true, cwd: 'tmp/', src: ['**'], dest: 'ui'},
					{expand: true, cwd: 'app/', src: ['public/images/**', 'partials/**'], dest: 'ui'},
					{expand: true, cwd: 'node_modules/font-awesome/', src: ['fonts/**'], dest: 'ui/public'},
					{expand: true, cwd: 'node_modules/bootstrap/', src: ['fonts/**'], dest: 'ui/public'},
				]
			}
		}
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
		'concat:app',
		'regex-replace:strict',
		'uglify',
		'concat:js',
		// Compress CSS
		'cssmin',
		'concat:css',
		// Pass HTML Resources
		'htmlrefs',
		'copy',
		// Clean Env
		'clean:tmp',
	]);
};