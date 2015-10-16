#!/usr/bin/env bash

create 'alertStreamSchema', {NAME => 'f', BLOOMFILTER => 'ROW', VERSIONS => '1', COMPRESSION => 'SNAPPY'}
create 'mlmodel', {NAME => 'f', BLOOMFILTER => 'ROW', VERSIONS => '1', COMPRESSION => 'SNAPPY'}
create 'eagle_metric', {NAME => 'f', BLOOMFILTER => 'ROW', VERSIONS => '1', COMPRESSION => 'SNAPPY'}
create 'userprofile', {NAME => 'f', BLOOMFILTER => 'ROW', VERSIONS => '1', COMPRESSION => 'SNAPPY'}
