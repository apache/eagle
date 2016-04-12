#! /usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import urllib2 as url_lib
import getopt
import json
import sys
import subprocess
import re
import time
import platform

##### Constants Definitions - start #####
SCRIPT_ABS_DIR, SCRIPT_NAME = os.path.split(os.path.abspath(sys.argv[0]))
EXPECTED_DIR_IN_REPO = "eagle-dev"

# for log level:
LOG_LEVEL_NAME_DEBUG = "debug"
LOG_LEVEL_VALUE_DEBUG = 0
LOG_LEVEL_NAME_INFO = "info"
LOG_LEVEL_VALUE_INFO = 1
LOG_LEVEL_NAME_WARN = "warn"
LOG_LEVEL_VALUE_WARN = 2
LOG_LEVEL_NAME_ERROR = "error"
LOG_LEVEL_VALUE_ERROR = 3
LOG_LEVEL_NAME_FATAL = "fatal"
LOG_LEVEL_VALUE_FATAL = 4
DEFAULT_LOG_LEVEL_NAME = LOG_LEVEL_NAME_INFO
LOG_LEVEL_REGEXP = re.compile("^(%s|%s|%s|%s|%s)$" % (
    LOG_LEVEL_NAME_DEBUG, LOG_LEVEL_NAME_INFO, LOG_LEVEL_NAME_WARN, LOG_LEVEL_NAME_ERROR, LOG_LEVEL_NAME_FATAL), re.I)
LOG_LEVEL_MAPPING = dict()
LOG_LEVEL_MAPPING[LOG_LEVEL_NAME_DEBUG] = LOG_LEVEL_VALUE_DEBUG
LOG_LEVEL_MAPPING[LOG_LEVEL_NAME_INFO] = LOG_LEVEL_VALUE_INFO
LOG_LEVEL_MAPPING[LOG_LEVEL_NAME_WARN] = LOG_LEVEL_VALUE_WARN
LOG_LEVEL_MAPPING[LOG_LEVEL_NAME_ERROR] = LOG_LEVEL_VALUE_ERROR
LOG_LEVEL_MAPPING[LOG_LEVEL_NAME_FATAL] = LOG_LEVEL_VALUE_FATAL

# textuals:
HELP_MESSAGE = "".join([
    "%s -n <pr_number> [--log-level <log_level_string>] [-p|--patch]\n" % SCRIPT_NAME,
    "\n"
    "args: (arg-type: R --> Required, O --> Optional)\n",
    "    -n", "\t\t\t", "R", "\tpull request number", "\n",
    "    --log_level", "\t\t", "O", "\tlowest log level considered, valid value:\n",
    "               ", "\t\t", " ", ("\t  |--> '%s', '%s', '%s', '%s', and '%s'\n" % (
        LOG_LEVEL_NAME_DEBUG, LOG_LEVEL_NAME_INFO, LOG_LEVEL_NAME_WARN, LOG_LEVEL_NAME_ERROR, LOG_LEVEL_NAME_FATAL)),
    "               ", "\t\t", " ", ("\t  \\--> '%s' by default\n" % DEFAULT_LOG_LEVEL_NAME),
    "    -p, --patch", "\t\t", "O", "\toperate with patch mode\n",
    "\n"
    "samples (replace '0' with real pull request number):\n",
    "    %s -n 0\n" % SCRIPT_NAME,
    "    %s -n 0 --log_level debug -p\n" % SCRIPT_NAME,
    "    %s -n 0 --log_level info --patch\n" % SCRIPT_NAME
])
GITHUB_ORGANIZATION = "apache"
REPO_NAME = "incubator-eagle"
APACHE_GIT_REPO_URL = "https://git-wip-us.apache.org/repos/asf/incubator-eagle.git"
PROJECT_PREFIX = "EAGLE-"
GITHUB_PR_STATE_OPEN = "open"
DEFAULT_REMOTE_NAME = "eagle_temp_upstream"
OPERATING_BRANCH_PREFIX = "operating-branch-"
TEMP_FILE_DIR = "merge_pr_temp_folder"
APPROVED_SIGNS = [":+1:", "LGTM"]
REJECTED_SIGNS = [":-1:"]
ENCODING = "utf-8"

# string templates:
GITHUB_PATH_TUPLE = (GITHUB_ORGANIZATION, REPO_NAME)
GITHUB_PATCH_URL_TEMPLATE = "/".join(
        ["https://patch-diff.githubusercontent.com/raw", GITHUB_ORGANIZATION, REPO_NAME, "pull", "%s.patch"])
GITHUB_USER_INFO_TEMPLATE = "https://api.github.com/users/%s"
PR_INFO_API_TEMPLATE = "/".join(["https://api.github.com/repos", GITHUB_ORGANIZATION, REPO_NAME, "pulls", "%s"])
JIRA_TICKET_LINK_TEMPLATE = "".join(["https://issues.apache.org/jira/browse/", PROJECT_PREFIX, "%s"])
##### Constants Definitions - end #####

##### Global Variable Definitions - start #####
log_level_name = DEFAULT_LOG_LEVEL_NAME
log_level = LOG_LEVEL_MAPPING[log_level_name]
patch_mode = False
pr_number = 0
repo_root_dir = None
script_in_repo_root_dir = False
##### Global Variable Definitions - end #####

##### Define Valid Committers - start #####
committers = dict()


# here is where to add committer, with syntax: committers[author_github_id] = author_email_address
##### Define Valid Committers - end #####

def to_console(msg):
    print msg


def __log_msg_list__(key, msg):
    if isinstance(msg, list):
        for sm in msg:
            to_console("%s: %s" % (key, sm))
    elif isinstance(msg, str):
        to_console("%s: %s" % (key, msg))
    elif isinstance(msg, unicode):
        to_console("%s: %s" % (key, msg.encode(ENCODING)))
    else:
        raise AttributeError("argument 'msg' must be a string, unicode or list, but %s is given" % type(msg))


def __shall_log__(level_number):
    return level_number >= log_level


def __go_with_patch__():
    return patch_mode


def fatal(msg):
    if __shall_log__(LOG_LEVEL_VALUE_FATAL):
        __log_msg_list__("FTL", msg)


def error(msg):
    if __shall_log__(LOG_LEVEL_VALUE_ERROR):
        __log_msg_list__("ERR", msg)


def warn(msg):
    if __shall_log__(LOG_LEVEL_VALUE_WARN):
        __log_msg_list__("WRN", msg)


def info(msg):
    if __shall_log__(LOG_LEVEL_VALUE_INFO):
        __log_msg_list__("INF", msg)


def debug(msg):
    if __shall_log__(LOG_LEVEL_VALUE_DEBUG):
        __log_msg_list__("DBG", msg)


def display_help(*err_msg):
    if err_msg:
        if len(err_msg) == 1 and isinstance(err_msg[0], list):
            error(err_msg[0])
        else:
            error(list(err_msg))
    help_msg_sign = "-------------------- HELPING MESSAGE --------------------\n"
    to_console("\n%sUsage: %s%s" % (help_msg_sign, HELP_MESSAGE, help_msg_sign))


def exit_with_errmsg(err, err_code):
    if isinstance(err, subprocess.CalledProcessError):
        error(err.output.decode().strip() + ", exit with code %s ..." % str(err_code))
    else:
        error(str(err) + ", exit with code %s ..." % str(err_code))
    exit(err_code)


def extract_run_command_err_msg(e):
    return e.output.decode().strip()


def is_windows():
    s = platform.system().lower()
    return s.startswith("win")


def __debug_command__(command_in_list, result):
    debug("running command: %s, result:\n%s" % (command_in_list, result))


def run_command(command):
    c = None
    if isinstance(command, list):
        c = command
    elif isinstance(command, unicode):
        c = command.encode(ENCODING).split(" ")
    else:
        c = command.split(" ")
    result = subprocess.check_output(c, stderr=subprocess.STDOUT).decode()
    __debug_command__(c, result)
    return result


def get_repo_root_dir():
    debug("to get repo root directory")
    return run_command("git rev-parse --show-toplevel").strip().replace("/", os.sep)


def get_temp_file_dir():
    return os.sep.join([repo_root_dir, "..", TEMP_FILE_DIR])


def compose_patch_file_path(temp_dir):
    return os.sep.join([temp_dir, "%s.patch" % pr_number])


def compose_commit_msg_file_path(temp_dir):
    return os.sep.join([temp_dir, "pr_%s_commit_msg" % pr_number])


def is_working_directory_clean():
    commands = ["git", "diff-index", "--quiet", "HEAD"]
    return_code = subprocess.call(commands)
    debug("to ensure working directory is clean")
    __debug_command__(commands, "return_code: %s (0 for True, 1 for False)\n" % return_code)
    if return_code == 0:
        return True
    else:
        return False


def has_untracked_files_excluding(exclusion):
    return len(find_untracked_files_excluding(exclusion)) != 0


def find_untracked_files_excluding(exclusion):
    debug("to find untracked files")
    fs = run_command("git ls-files --exclude-standard --others").strip().split("\n")
    count = len(fs)
    for i in range(count):
        if fs[i] == "" or fs[i] == exclusion:
            fs.pop(i)
    return fs


def is_self_reviewed(author, reviewer):
    # currently, allow self-reviewing, but may disallow in the future
    # return author == reviewer
    return False


def is_valid_reviewer(reviewer):
    # currently, don't validate the authorization of the reviewer, may validate it in the future
    # return reviewer in committers.keys()
    return True


def ask_for_input(prompt_msg):
    return raw_input("%s%s" % ("IPT: ", prompt_msg))


def ask_for_multiline_input(prompt_msg):
    to_console("IPT: %s" % prompt_msg)
    lines = sys.stdin.readlines()
    return lines


def is_valid_email(email):
    if not email:
        return False
    return re.search("^[\w\-\.]+@(\w[\w\-]+\.)+[a-zA-Z0-9]+$", email)


def has_branch(branch):
    debug("to check if branch exists")
    result = run_command("git branch --list %s" % branch).strip()
    return result != ""


def get_current_branch():
    debug("to get current branch")
    return run_command("git rev-parse --abbrev-ref HEAD").strip()


def get_commit_history(branch, required_count):
    debug("to query commit history")
    result = run_command("git log --decorate --graph -n %s" % required_count).strip()
    rows = re.split("\n", result)
    git_logs = []
    i = 0
    for row in rows:
        if row.startswith("*"):
            if i == 0:
                git_logs.append("new -> \t%s\n" % row)
                i += 1
            elif i == 1:
                git_logs.append("old -> \t%s\n" % row)
                i += 1
            else:
                git_logs.append("\t%s\n" % row)
        else:
            git_logs.append("\t%s\n" % row)
    git_logs.append("\t* commit ......")
    return "".join(git_logs)


def compose_commit_msg(commit_msg_file_path, jira_id, pr_title_content, commit_description_lines, author, author_email,
                       reviewer_email_mapping):
    jira_url = JIRA_TICKET_LINK_TEMPLATE % jira_id
    if os.path.exists(commit_msg_file_path):
        os.remove(commit_msg_file_path)
    comp = ["@%s <%s>" % (key, reviewer_email_mapping.get(key)) for key in reviewer_email_mapping.keys()]
    reviewer_row = "Reviewer(s): %s\r\n" % ", ".join(comp)
    with open(commit_msg_file_path, "w") as f:
        f.write("%s%s %s\r\n" % (PROJECT_PREFIX, jira_id, pr_title_content))
        for line in commit_description_lines:
            if line.strip() != "":
                f.write("%s" % line)
        f.write("\r\n")
        f.write("%s\r\n" % jira_url)
        f.write("\r\n")
        f.write("Author: @%s <%s>\r\n" % (author, author_email))
        f.write(reviewer_row)
        f.write("\r\n")
        f.write("Closes #%s.\r\n" % pr_number)


def download_file(download_url, file_path):
    source = url_lib.urlopen(download_url)
    with open(file_path, "wb") as f:
        f.write(source.read())


def ensure_env():
    # check if the git working directory is clean
    if not is_working_directory_clean():
        exit_with_errmsg("working directory is not clean, please commit un-staged changes or stash them and try again",
                         -1)

    # check if any untrack file(s)
    untracked_files = find_untracked_files_excluding(SCRIPT_NAME if script_in_repo_root_dir else "")
    if len(untracked_files) > 0:
        warn("untracked files found:\n    %s" % "\n    ".join(untracked_files))
        exit_with_errmsg("to avoid loss, please commit untracked files or delete them before executing this script", -1)

    # choose a remote name to operate
    debug("to query remotes")
    rows = run_command("git remote -v").strip().split("\n")
    pushable_remote_name = ""
    for row in rows:
        if APACHE_GIT_REPO_URL in row:
            parts = re.split("\s+", row)
            if "push" in parts[2]:
                pushable_remote_name = parts[0]
                break
    if pushable_remote_name != "":  # find a pushable remote name
        info("will use \"%s\" as a remote name to operate" % pushable_remote_name)
    else:  # no pushable remote name, add one
        debug("to add remote for operation")
        run_command("git remote add %s %s" % (DEFAULT_REMOTE_NAME, APACHE_GIT_REPO_URL))
        info("there is no pushable remote name defined, add remote \"%s\" for you" % DEFAULT_REMOTE_NAME)
        pushable_remote_name = DEFAULT_REMOTE_NAME

    # create temporary folder
    temp_dir = get_temp_file_dir()
    patch_file_path = compose_patch_file_path(temp_dir)
    if os.path.exists(temp_dir):
        if patch_mode and os.path.exists(patch_file_path):
            os.remove(patch_file_path)
            info("old patch file deleted: %s" % patch_file_path)
    else:
        debug("to create temporary directory")
        run_command("mkdir %s" % temp_dir)
        info("temporary dir created: %s" % temp_dir)

    return (pushable_remote_name, temp_dir)


def get_info_from_github(url):
    try:
        gh_req = url_lib.Request(url)
        response = url_lib.urlopen(gh_req)
        pr_info = response.read().decode(ENCODING)
        return json.loads(pr_info)
    except url_lib.HTTPError as e:
        exit_with_errmsg("failed to get data from URL: %s" % url, -1)
    except url_lib.URLError as e:
        exit_with_errmsg("server not reachable: %s" % url, -1)


def parse_pr_title(pr_title):
    regex = re.compile("^[[]?%s(\d+)[]]?\W?\s*(.+)$" % PROJECT_PREFIX, re.I)
    match = re.search(regex, pr_title)
    if match:
        return (match.group(1), match.group(2))
    else:
        err_msg = "the title of the pull request should start with \"%s${jira_id}\" (case-insensitive), followed by a white-space and textual content, please revise accordingly" % PROJECT_PREFIX
        exit_with_errmsg(err_msg, -1)


def ensure_quality_metrics(pr_comments_url, author, latest_commit_timestamp):
    # TODO - Px - need to add checking steps for CI auto testing
    info("looking up comments from github...")
    pr_comments = get_info_from_github(pr_comments_url)
    count = len(pr_comments)
    reviewers = set()
    approvers = set()
    rejecters = set()
    never_found_review_comment = True  # True to mean haven't found any review comment
    for i in range(count - 1, -1, -1):  # loop from count-1 to 0, step is -1
        comment_json = pr_comments[i]
        reviewer = comment_json['user']['login'].encode(ENCODING)
        comment_timestamp = comment_json['created_at'].encode(ENCODING)
        is_current_comment_valid = comment_timestamp > latest_commit_timestamp
        # currently, allow self-reviewing, but may disallow in the future, to disallow, modify in function is_self_reviewed()
        if is_self_reviewed(author, reviewer):  # the author of the pr shall not be considered as a reviewer
            continue
        if (is_valid_reviewer(reviewer) and
                not (reviewer in approvers or
                             reviewer in rejecters or
                             reviewer in reviewers)):  # valid review comments of this reviewer have been proceeded, continue to next comment
            comment = comment_json['body'].encode(ENCODING)
            parts = re.split("\n", comment)
            for part in parts:
                part = part.strip()
                # if part in APPROVED_SIGNS or part in REJECTED_SIGNS:
                if part in APPROVED_SIGNS:  # means current comment is a review comment
                    if never_found_review_comment and not is_current_comment_valid:  # means the latest review comment is earlier than the latest commit
                        exit_with_errmsg(
                                "the latest commit(s) of this pull request has not been approved, cannot merge it", -1)
                    if never_found_review_comment:  # means find the latest review comment that is later than the latest commit, shall switch never_found_review_comment off
                        never_found_review_comment = False
                    if is_current_comment_valid:  # this review comment should be considered as part of approval or rejection of the pr
                        if part in APPROVED_SIGNS:
                            if reviewer not in rejecters:  # means, this approval is the latest valid review of this reviewer, considered as pr approved by this reviewer
                                approvers.add(reviewer)
                        else:  # means part in REJECTED_SIGNS, currently this condition is unreachable, but may be reachable in the future when we allow rejections, just keep it
                            if reviewer not in approvers:  # means, this rejection is the latest valid review of this reviewer, considered as pr rejected by this reviewer
                                rejecters.add(reviewer)
                    reviewers.add(reviewer)
                    break  # have found the review part in this comment, break to check next comment
        # currently, don't take rejections into account, may take it in the future by un-comment below 2 lines
        '''
		if (not is_current_comment_valid or i == 0) and len(rejecters) != 0:   # find the pr is rejected by some reviewers, and found all rejecters, should ask for their approval
			exit_with_errmsg("this pr is rejected by reviewers: %s, cannot merge it, please get approved and try again" % list(rejecters), -1)
		'''
    if never_found_review_comment:
        exit_with_errmsg("this pull request has not been approved, cannot merge it", -1)
    info("verified: this pr is reviewed by %s and finally approved by %s, it's good to merge" % (
        list(reviewers), list(approvers)))
    return list(reviewers)


def get_reviewer_email_mapping(reviewers_accounts):
    info("querying email address for reviewers' account from github...")
    reviewer_email_mapping = dict()
    for account in reviewers_accounts:
        reviewer_info = get_info_from_github(GITHUB_USER_INFO_TEMPLATE % account)
        email = reviewer_info['email'].encode(ENCODING)
        if email:
            reviewer_email_mapping[account] = email
        else:
            reviewer_email_mapping[account] = ""
            # because currently don't keep information of committers, if an email is None, just save it as ""
            # reviewer_email_mapping[account] = committers[account]
    return reviewer_email_mapping


def determine_author_email(author, reviewer_email_mapping):
    author_email = None
    if author in reviewer_email_mapping:  # the email of the author in reviewer list has been queried, no need to query again
        author_email = reviewer_email_mapping[author]
    else:
        info("querying email address of author <%s> from github" % author)
        author_info = get_info_from_github(GITHUB_USER_INFO_TEMPLATE % author)
        author_email = author_info['email'].encode(ENCODING)
    if not author_email or author_email.strip() == "":  # means couldn't find public email of author
        log_error = False
        skip_author_email_regexp = re.compile("^no$", re.I)
        # ask operater to input email for the author, skip by "no" or validate the input string
        while not is_valid_email(author_email):
            if not log_error:  # the first loop, shall not log error
                log_error = True
            else:
                error("the input is not a valid email address")
            author_email = ask_for_input(
                    "couldn't find public email of author <%s>, please input one (<input \"no\" and press Enter key> to skip):" % author).strip()
            if re.search(skip_author_email_regexp, author_email):
                author_email = ""
                break
    debug("email address of author <%s> is finally considered: %s" % (author, author_email))
    return author_email


def generate_commit_msg_file(temp_dir, jira_id, pr_title_content, author, author_email, reviewer_email_mapping):
    finish_hint = "<press Ctrl+D>"
    if is_windows():
        finish_hint = "<press Ctrl+Z then press Enter key>"
    commit_msg_file_path = compose_commit_msg_file_path(temp_dir)
    # prompt user to input description of this merge, better to open a vi
    commit_description_lines = ask_for_multiline_input(
            "please input the description of the merge, which will be regarded as a part of the final commit message\n(<press Enter key> to start new lines, %s to finish inputting):" % finish_hint)
    # in the input multi-line message, discard redundant blank lines after the meanful lines
    while len(commit_description_lines) != 0 and commit_description_lines[-1].strip() == "":
        commit_description_lines.pop()
    # compose the template file
    compose_commit_msg(commit_msg_file_path, jira_id, pr_title_content, commit_description_lines, author, author_email,
                       reviewer_email_mapping)
    debug("save commit message file: %s" % commit_msg_file_path)
    return commit_msg_file_path


def merge_pr_and_checkin(operating_branch, pushable_remote_name, base_sha, forked_repo_url, forked_repo_pr_branch,
                         temp_dir, commit_msg_file_path):
    success = False
    user_skip = False
    patch_file_path = None
    try:
        # get latest update from base repo
        debug("to get latest updates from base remote")
        run_command("git fetch %s master" % pushable_remote_name)
        remote_master_sha = run_command("git rev-parse %s/master" % pushable_remote_name).strip()
        if base_sha != remote_master_sha:
            warn(
                    "the master branch of remote <%s> has commit(s) ahead of the base-commit of the ones in this PR" % pushable_remote_name)
        # checkout operating branch
        debug("to checkout base remote master as base")
        run_command("git checkout -b %s %s/master" % (operating_branch, pushable_remote_name))
        if patch_mode:
            download_url = GITHUB_PATCH_URL_TEMPLATE % pr_number
            patch_file_path = compose_patch_file_path(temp_dir)
            # download patch
            # TODO - P1 - try to involve progress bar for downloading
            info("downloading %s.patch from github..." % pr_number)
            download_file(download_url, patch_file_path)
            # apply patch
            debug("to apply the patch")
            run_command("git apply %s" % patch_file_path)
        else:
            info("shall be doing with non-patch mode")
            debug("to get all updates in the pr")
            # merge pr
            run_command("git pull %s %s" % (forked_repo_url, forked_repo_pr_branch))
            debug("to reset all commits in order to make single final commit")
            # reset all updates in the pr into unstaged status
            run_command("git reset %s/master" % pushable_remote_name)

        debug("to add all updates to single final commit")
        run_command("git add -A")
        if script_in_repo_root_dir:
            debug("to exclude the copied script itself")
            run_command("git reset HEAD %s" % SCRIPT_NAME)
        debug("to make the single final commit")
        run_command("git commit -F %s" % commit_msg_file_path)
        info("committed to local repository on branch %s, the commit history now looks like: " % operating_branch)
        history = get_commit_history(operating_branch, 2)
        to_console("%s" % history)
        debug("to query the newest commit sha")
        newest_commit = run_command("git rev-parse HEAD").strip()
        choice = ask_for_input(
                "shall we push commit <%s> to remote <%s/master>?\n(<input \"y\" or \"n\" and press Enter key>):" % (
                    newest_commit, pushable_remote_name)).strip()
        if choice == "Y" or choice == "y":
            # push to remote
            info("pushing to remote repository...")
            debug("to push to remote")
            run_command("git push %s %s:master" % (pushable_remote_name, operating_branch))
            success = True
        else:
            # log the info and depend finally-block to recover environment
            info("recovering working directory to original state...")
            user_skip = True

        return (success, user_skip)
    finally:
        if patch_file_path and os.path.exists(patch_file_path):
            os.remove(patch_file_path)
            debug("recover env - patch file deleted: %s" % patch_file_path)


def main(argv):
    # cope with invoking arguments
    try:
        opts, args = getopt.getopt(argv, "hn:l:p", ["log-level=", "patch"])
    except getopt.GetoptError as e:
        display_help(e.msg)
        sys.exit(-1)
    has_pr = False
    for opt, arg in opts:
        if opt == "-h":
            display_help()
            exit(1)
        elif opt == "-n":
            regexp = re.compile("^[1-9][0-9]*$")
            if not regexp.search(arg):
                display_help("pull request number should be a positive integer")
                exit(-1)
            global pr_number
            pr_number = arg
        elif (opt == "-l" or opt == "--log-level"):
            match = LOG_LEVEL_REGEXP.search(arg)
            if match:
                matched_value = match.group(1).lower()
                global log_level_name
                global log_level
                log_level_name = matched_value
                log_level = LOG_LEVEL_MAPPING[log_level_name]
            else:
                display_help("un-recognized log level: %s" % arg)
                exit(-1)
        elif (opt == "-p" or opt == "--patch"):
            global patch_mode
            patch_mode = True
        else:
            display_help("un-recognized argument: %s" % opt)

    # pr_number is a required argument
    if pr_number == 0:
        display_help("pull request number must be provided")
        exit(-1)

    # log executing basic info
    debug("log level is: %s" % log_level_name)
    debug("attempt to work on pull request: #%s" % pr_number)
    debug("attempt to use %s mode to operate" % ("patch" if patch_mode else "non-patch"))

    # start processing
    success = False
    user_skip = False
    executing_dir = os.getcwd()
    original_branch = None
    operating_branch = None
    commit_msg_file_path = None
    try:
        to_console("-------------------- START PROCESSING --------------------")
        # set global repo root dir, guarantee all git related operations are executed in local git repo
        global repo_root_dir
        repo_root_dir = get_repo_root_dir()
        debug("repo's root dir is: %s" % repo_root_dir)

        # validate the location of the script file, it's important for future "git add" and recovery
        debug("the executed script is locating at: %s" % SCRIPT_ABS_DIR)
        global script_in_repo_root_dir
        if SCRIPT_ABS_DIR.endswith(EXPECTED_DIR_IN_REPO):
            debug("the script is in dir %s" % SCRIPT_ABS_DIR)
            script_in_repo_root_dir = False
        else:
            if repo_root_dir != SCRIPT_ABS_DIR:
                exit_with_errmsg(
                        "you're executing copied script, please place it under dir %s, and try again" % repo_root_dir,
                        -1)
            else:
                debug("the script is in dir %s" % repo_root_dir)
                script_in_repo_root_dir = True

        # guarantee operating in repo_root_dir
        if os.getcwd() != repo_root_dir:
            os.chdir(repo_root_dir)
            debug("change dir to %s" % repo_root_dir)

        # guarantee the environment for operations
        (pushable_remote_name, temp_dir) = ensure_env()

        # record original branch where to start the execution
        original_branch = get_current_branch()

        # validate pr info
        info("query pr information from github...")
        pr_info_url = PR_INFO_API_TEMPLATE % pr_number
        pr_info = get_info_from_github(pr_info_url)

        # validate pr state
        pr_state = pr_info['state'].encode(ENCODING)
        if pr_state != GITHUB_PR_STATE_OPEN:
            exit_with_errmsg("the state of pr %s is not %s, please try another pull request number" % (
                pr_number, GITHUB_PR_STATE_OPEN), -1)

        # validate if pr has conflict(s)
        pr_mergeable = pr_info['mergeable']
        if not pr_mergeable:
            exit_with_errmsg("this pull request contains conflict(s) that must be revolved", -1)

        # validate and extract info from pr title, then generate jira url
        # currently will not check if the jira ticket is valid for reason of authentication complication
        (jira_id, pr_title_content) = parse_pr_title(pr_info['title'].encode(ENCODING))

        # get latest commit timestamp
        pr_commits_url = pr_info['_links']['commits']['href'].encode(ENCODING)
        pr_commits_json = get_info_from_github(pr_commits_url)
        pr_commits_count = pr_info['commits']
        latest_commit_json = pr_commits_json[pr_commits_count - 1]
        latest_commit_timestamp = latest_commit_json['commit']['committer']['date'].encode(ENCODING)

        # check if the pr is well approved and get reviewers' account together with their email
        pr_comments_url = pr_info['_links']['comments']['href'].encode(ENCODING)
        pr_author = pr_info['user']['login'].encode(ENCODING)
        reviewer_accounts = ensure_quality_metrics(pr_comments_url, pr_author, latest_commit_timestamp)
        reviewer_email_mapping = get_reviewer_email_mapping(reviewer_accounts)

        # determine author's email
        author_email = None
        author_email_in_last_commit = latest_commit_json['commit']['author']['email'].encode(ENCODING)
        if author_email_in_last_commit:
            author_email = author_email_in_last_commit
            if pr_author in reviewer_email_mapping:
                reviewer_email_mapping[pr_author] = author_email_in_last_commit
        else:
            author_email = determine_author_email(pr_author, reviewer_email_mapping)

        # generate commit message file
        commit_msg_file_path = generate_commit_msg_file(temp_dir, jira_id, pr_title_content, pr_author, author_email,
                                                        reviewer_email_mapping)

        # choose the right branch and check the sha
        base_sha = pr_info['base']['sha'].encode(ENCODING)
        forked_repo_url = pr_info['head']['repo']['clone_url'].encode(ENCODING)
        forked_repo_pr_branch = pr_info['head']['ref'].encode(ENCODING)

        # merge pr and checkin
        timestamp = str(time.time()).replace(".", "")
        operating_branch = "%s%s" % (OPERATING_BRANCH_PREFIX, timestamp)
        (success, user_skip) = merge_pr_and_checkin(operating_branch, pushable_remote_name, base_sha, forked_repo_url,
                                                    forked_repo_pr_branch, temp_dir, commit_msg_file_path)
    except subprocess.CalledProcessError as e:
        exit_with_errmsg(e, -1)
    finally:
        try:
            if success:
                debug("recover env - to checkout original branch")
                run_command("git checkout %s" % original_branch)
                debug("recover env - to delete operating branch")
                run_command("git branch -D %s" % operating_branch)
            else:
                if original_branch:
                    if not is_working_directory_clean():
                        debug("recover env - to hard reset to original branch")
                        run_command("git reset --hard %s" % original_branch)
                    if get_current_branch() != original_branch:
                        debug("recover env - to checkout original branch")
                        run_command("git checkout %s" % original_branch)
                    if operating_branch and has_branch(operating_branch):
                        debug("recover env - to delete operating branch")
                        run_command("git branch -D %s" % operating_branch)
        except subprocess.CalledProcessError as e:
            exit_with_errmsg(e, -1)
        finally:
            # delete temp files if exist
            if commit_msg_file_path and os.path.exists(commit_msg_file_path):
                debug("recover env - to delete commit message file: %s" % commit_msg_file_path)
                os.remove(commit_msg_file_path)
            # guarantee return to original executing dir
            if os.getcwd() != executing_dir:
                debug("recover env - to change back to original executing dir: %s" % executing_dir)
                os.chdir(executing_dir)
            # notify user
            if success:
                info("pull request <%s> has been applied and pushed to remote <%s/master> of repository <%s>" % (
                    pr_number, pushable_remote_name, APACHE_GIT_REPO_URL))
                info("if you're going to continue with any forked repository, please update it accordingly")
            else:
                info("%sthe process is cancelled, and the environment has been recovered" % (
                    "" if user_skip else "failure occurs, "))
            info("Bye.")
            to_console("-------------------- ENDED PROCESSING --------------------")


if __name__ == "__main__":
    main(sys.argv[1:])
