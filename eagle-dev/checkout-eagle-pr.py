#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Utility for creating well-formed pull request merges and pushing them to Apache.
#   usage: ./apache-pr-merge.py    (see config env vars below)
#
# This utility assumes you already have local a Eagle git folder and that you
# have added remotes corresponding to both (i) the github apache Eagle
# mirror and (ii) the apache git repo.

import json
import os
import re
import subprocess
import sys
import urllib2

# Location of your EAGLE git development area
EAGLE_HOME = os.environ.get("EAGLE_HOME", os.getcwd())
# Remote name which points to the Gihub site
PR_REMOTE_NAME = os.environ.get("PR_REMOTE_NAME", "apache-github")
# Remote name which points to Apache git
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache-git")
# OAuth key used for issuing requests against the GitHub API. If this is not defined, then requests
# will be unauthenticated. You should only need to configure this if you find yourself regularly
# exceeding your IP's unauthenticated request rate limit. You can create an OAuth key at
# https://github.com/settings/tokens. This script only requires the "public_repo" scope.
GITHUB_OAUTH_KEY = os.environ.get("GITHUB_OAUTH_KEY")

GITHUB_BASE = "https://github.com/apache/eagle/pull"
GITHUB_API_BASE = "https://api.github.com/repos/apache/eagle"
JIRA_BASE = "https://issues.apache.org/jira/browse"
JIRA_API_BASE = "https://issues.apache.org/jira"

PR_REPO = "https://github.com/apache/eagle.git"
PUSH_REPO = "https://git-wip-us.apache.org/repos/asf/eagle.git"

# Prefix added to temporary branches
BRANCH_PREFIX = "PR_TOOL"


def get_json(url):
    try:
        request = urllib2.Request(url)
        if GITHUB_OAUTH_KEY:
            request.add_header('Authorization', 'token %s' % GITHUB_OAUTH_KEY)
        return json.load(urllib2.urlopen(request))
    except urllib2.HTTPError as e:
        if "X-RateLimit-Remaining" in e.headers and e.headers["X-RateLimit-Remaining"] == '0':
            print "Exceeded the GitHub API rate limit; see the instructions in " + \
                  "dev/merge_EAGLE_pr.py to configure an OAuth token for making authenticated " + \
                  "GitHub requests."
        else:
            print "Unable to fetch URL, exiting: %s" % url
        sys.exit(-1)


def run_cmd(cmd):
    print cmd
    if isinstance(cmd, list):
        return subprocess.check_output(cmd)
    else:
        return subprocess.check_output(cmd.split(" "))


# merge the requested PR and return the merge hash
def checkout_pr(pr_num, target_ref, title, body, pr_repo_desc):
    pr_branch_name = "%s_CHECKOUT_PR_%s" % (BRANCH_PREFIX, pr_num)
    target_branch_name = "%s_MERGE_PR_%s_%s" % (BRANCH_PREFIX, pr_num, target_ref.upper())
    run_cmd("git fetch %s pull/%s/head:%s" % (PR_REMOTE_NAME, pr_num, pr_branch_name))
    # run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, target_ref, target_branch_name))
    run_cmd("git checkout %s" % pr_branch_name)


def get_current_ref():
    ref = run_cmd("git rev-parse --abbrev-ref HEAD").strip()
    if ref == 'HEAD':
        # The current ref is a detached HEAD, so grab its SHA.
        return run_cmd("git rev-parse HEAD").strip()
    else:
        return ref


def check_init():
    try:
        run_cmd("git config --get remote.%s.url" % PR_REMOTE_NAME)
    except:
        run_cmd("git remote add %s %s" % (PR_REMOTE_NAME, PR_REPO))
    try:
        run_cmd("git config --get remote.%s.url" % PUSH_REMOTE_NAME)
    except:
        run_cmd("git remote add %s %s" % (PUSH_REMOTE_NAME, PUSH_REPO))


def main():
    global original_head

    os.chdir(EAGLE_HOME)
    original_head = get_current_ref()

    check_init()

    branches = get_json("%s/branches" % GITHUB_API_BASE)
    branch_names = filter(lambda x: x.startswith("branch-"), [x['name'] for x in branches])
    # Assumes branch names can be sorted lexicographically
    latest_branch = sorted(branch_names, reverse=True)[0]

    pr_num = raw_input("Which pull request would you like to checkout? (e.g. 34): ")
    pr = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num))
    pr_events = get_json("%s/issues/%s/events" % (GITHUB_API_BASE, pr_num))

    url = pr["url"]
    title = pr["title"]
    body = pr["body"]
    target_ref = pr["base"]["ref"]
    user_login = pr["user"]["login"]
    base_ref = pr["head"]["ref"]
    pr_repo_desc = "%s/%s" % (user_login, base_ref)

    print ("\n=== Pull Request #%s ===" % pr_num)
    print ("title\t%s\nsource\t%s\ntarget\t%s\nurl\t%s" % (
        title, pr_repo_desc, target_ref, url))

    checkout_pr(pr_num, target_ref, title, body, pr_repo_desc)


if __name__ == "__main__":
    import doctest

    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        exit(-1)
    try:
        main()
    except:
        raise
