#! /usr/bin/python2.7
import os
import urllib2 as url_lib
import getopt
import json
import sys
import subprocess
import re
import time

##### Constants Definitions - start #####
# textuals:
HELP_MESSAGE = "auto_merge.py -n <pr_number> [--log-level debug|info|warn|error|fatal]"
GITHUB_ORGANIZATION = "apache"
REPO_NAME = "incubator-eagle"
APACHE_GIT_REPO_URL = "https://git-wip-us.apache.org/repos/asf/incubator-eagle.git"
PROJECT_PREFIX = "EAGLE-"
GITHUB_PR_STATE_OPEN = "open"
DEFAULT_REMOTE_NAME = "eagle_temp_upstream"
OPERATING_BRANCH_PREFIX = "operating_branch_"
TEMP_FILE_DIR = "patch_temp_folder"
APPROVED_SIGNS = [":+1:", "LGTM"]
REJECTED_SIGNS = [":-1:"]
ENCODING = "utf-8"

# for log level:
LOG_LEVEL_NAME_DEBUG = "debug"
LOG_LEVEL_VALUE_DEBUG = 1
LOG_LEVEL_NAME_INFO = "info"
LOG_LEVEL_VALUE_INFO = 2
LOG_LEVEL_NAME_WARN = "warn"
LOG_LEVEL_VALUE_WARN = 3
LOG_LEVEL_NAME_ERROR = "error"
LOG_LEVEL_VALUE_ERROR = 4
LOG_LEVEL_NAME_FATAL = "fatal"
LOG_LEVEL_VALUE_FATAL = 5
LOG_LEVEL_REGEXP = re.compile("^(%s|%s|%s|%s|%s)$" % (LOG_LEVEL_NAME_DEBUG, LOG_LEVEL_NAME_INFO, LOG_LEVEL_NAME_WARN, LOG_LEVEL_NAME_ERROR, LOG_LEVEL_NAME_FATAL), re.I)
LOG_LEVEL_MAPPING = dict()
LOG_LEVEL_MAPPING[LOG_LEVEL_NAME_DEBUG] = LOG_LEVEL_VALUE_DEBUG
LOG_LEVEL_MAPPING[LOG_LEVEL_NAME_INFO] = LOG_LEVEL_VALUE_INFO
LOG_LEVEL_MAPPING[LOG_LEVEL_NAME_WARN] = LOG_LEVEL_VALUE_WARN
LOG_LEVEL_MAPPING[LOG_LEVEL_NAME_ERROR] = LOG_LEVEL_VALUE_ERROR
LOG_LEVEL_MAPPING[LOG_LEVEL_NAME_FATAL] = LOG_LEVEL_VALUE_FATAL

# string templates:
GITHUB_PATH_TUPLE = (GITHUB_ORGANIZATION, REPO_NAME)
GITHUB_PATCH_URL_TEMPLATE = "/".join(["https://patch-diff.githubusercontent.com/raw", GITHUB_ORGANIZATION, REPO_NAME, "pull", "%s.patch"])
GITHUB_USER_INFO_TEMPLATE = "https://api.github.com/users/%s"
PR_INFO_API_TEMPLATE = "/".join(["https://api.github.com/repos", GITHUB_ORGANIZATION, REPO_NAME, "pulls", "%s"])
PR_COMMENTS_API_TEMPLATE = "/".join(["https://api.github.com/repos", GITHUB_ORGANIZATION, REPO_NAME, "issues", "%s/comments"])
JIRA_TICKET_LINK_TEMPLATE = "".join(["https://issues.apache.org/jira/browse/", PROJECT_PREFIX, "%s"])
##### Constants Definitions - end #####

##### Global Variable Definitions - start #####
log_level = LOG_LEVEL_VALUE_INFO
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
	to_console("Usage: %s" % HELP_MESSAGE)

def exit_with_errmsg(err, err_code):
	if isinstance(err, subprocess.CalledProcessError):
		error(err.output.decode().strip() + ", exit with code %s ..." % str(err_code))
	else:
		error(str(err) + ", exit with code %s ..." % str(err_code))
	exit(err_code)

def extract_run_command_err_msg(e):
	return e.output.decode().strip()

def run_command(command):
	c = None
	if isinstance(command, list):
		c = command
	else:
		c = command.split(" ")
	return subprocess.check_output(c, stderr=subprocess.STDOUT).decode()

def get_info_from_github(url):
	try:
		gh_req = url_lib.Request(url)
		response = url_lib.urlopen(gh_req)
		pr_info = response.read().decode()
		return json.loads(pr_info)
	except url_lib.HTTPError as e:
		exit_with_errmsg("failed to get data from URL: %s" % url, -1)
	except url_lib.URLError as e:
		exit_with_errmsg("server not reachable: %s" % url, -1)

def get_repo_root_dir():
	try:
		return run_command("git rev-parse --show-toplevel").strip()
	except subprocess.CalledProcessError as e:
		exit_with_errmsg(e, -1)

def get_temp_file_dir():
	repo_root_dir = get_repo_root_dir()
	return os.sep.join([repo_root_dir, "..", TEMP_FILE_DIR])

def compose_patch_file_path(temp_dir, pr_number):
	return os.sep.join([temp_dir, "%s.patch" % pr_number])

def compose_commit_msg_template(temp_dir, pr_number):
	return os.sep.join([temp_dir, "pr_%s_commit_msg.template" % pr_number])

def is_working_directory_clean():
	return_code = subprocess.call(["git", "diff-index", "--quiet", "HEAD"])
	if return_code == 0:
		return True
	else:
		return False

def ensure_env(pr_number):
	try:
		temp_dir = get_temp_file_dir()

		# choose a remote name to operate
		rows = run_command("git remote -v").strip().split("\n")
		pushable_remote_name = ""
		for row in rows:
			if APACHE_GIT_REPO_URL in row:
				parts = re.split("\s+", row)
				if "push" in parts[2]:
					pushable_remote_name = parts[0]
					break
		if pushable_remote_name != "":   # find a pushable remote name
			info("will use \"%s\" as a remote name to operate" % pushable_remote_name)
		else:   # no pushable remote name, add one
			info("there is no pushable remote name defined, add \"%s\" for you" % DEFAULT_REMOTE_NAME)
			run_command("git remote add %s %s" % (DEFAULT_REMOTE_NAME, APACHE_GIT_REPO_URL))
			info("remote name \"%s\" added" % DEFAULT_REMOTE_NAME)
			pushable_remote_name = DEFAULT_REMOTE_NAME

		# check if the git working directory is clean
		if not is_working_directory_clean():
			exit_with_errmsg("working directory is not clean, please commit un-staged changes or stash them and try again", -1)

		# create temporary folder
		patch_file_path = compose_patch_file_path(temp_dir, pr_number)
		if os.path.exists(temp_dir):
			if os.path.exists(patch_file_path):
				os.remove(patch_file_path)
				info("old %s deleted successfully" % patch_file_path)
		else:
			run_command("mkdir %s" % temp_dir)
			info("%s created successfully" % temp_dir)
		
		return (pushable_remote_name, temp_dir)	
	except subprocess.CalledProcessError as e:
		exit_with_errmsg(e, -1)

def parse_pr_title(pr_title):
	regex = re.compile("^%s(\d+)\s+(.+)$" % PROJECT_PREFIX)
	match = re.search(regex, pr_title)
	if match:
		return (match.group(1), match.group(2))
	else:
		err_msg = "the title of the pull request should start with \"%s${jira_id}\", followed by a white-space and textual content, please revise accordingly" % PROJECT_PREFIX
		exit_with_errmsg(err_msg, -1)

def is_self_reviewed(author, reviewer):
	# currently, allow self-reviewing, but may disallow in the future
	#return author == reviewer
	return False

def is_valid_reviewer(reviewer):
	# currently, don't validate the authorization of the reviewer, may validate it in the future
	#return reviewer in committers.keys()
	return True

def ensure_quality_metrics(pr_number, author, last_push_timestamp):
	# TODO - Px - need to add checking steps for CI auto testing
	pr_comments_url = PR_COMMENTS_API_TEMPLATE % pr_number
	info("looking up comments from github...")
	pr_comments = get_info_from_github(pr_comments_url)
	count = len(pr_comments)
	reviewers = set()
	approvers = set()
	rejecters = set()
	never_found_review_comment = True   # True to mean haven't found any review comment
	for i in range(count-1, -1, -1):   # loop from count-1 to 0, step is -1
		comment_json = pr_comments[i]
		reviewer = comment_json['user']['login'].encode(ENCODING)
		comment_timestamp = comment_json['created_at'].encode(ENCODING)
		is_current_comment_valid = comment_timestamp > last_push_timestamp
		# currently, allow self-reviewing, but may disallow in the future, to disallow, modify in function is_self_reviewed()
		if is_self_reviewed(author, reviewer):   # the author of the pr shall not be considered as a reviewer
			continue
		if (is_valid_reviewer(reviewer) and
			not (reviewer in approvers or 
				reviewer in rejecters or 
				reviewer in reviewers)):   # valid review comments of this reviewer have been proceeded, continue to next comment
			comment = comment_json['body'].encode(ENCODING)
			parts = re.split("\n", comment)
			for part in parts:
				part = part.strip()
				#if part in APPROVED_SIGNS or part in REJECTED_SIGNS:
				if part in APPROVED_SIGNS:   # means current comment is a review comment
					if never_found_review_comment and not is_current_comment_valid:   # means the latest review comment is earlier than the latest commit
						exit_with_errmsg("the latest commit(s) of this pull request has not been approved, cannot merge it", -1)
					if never_found_review_comment:   # means find the latest review comment that is later than the latest commit, shall switch never_found_review_comment off
						never_found_review_comment = False
					if is_current_comment_valid:   # this review comment should be considered as part of approval or rejection of the pr
						if part in APPROVED_SIGNS:
							if reviewer not in rejecters:   # means, this approval is the latest valid review of this reviewer, considered as pr approved by this reviewer
								approvers.add(reviewer)
						else:   # means part in REJECTED_SIGNS, currently this condition is unreachable, but may be reachable in the future when we allow rejections, just keep it
							if reviewer not in approvers:   # means, this rejection is the latest valid review of this reviewer, considered as pr rejected by this reviewer
								rejecters.add(reviewer)
					reviewers.add(reviewer)
					break   # have found the review part in this comment, break to check next comment
		# currently, don't take rejections into account, may take it in the future by un-comment below 2 lines
		'''
		if (not is_current_comment_valid or i == 0) and len(rejecters) != 0:   # find the pr is rejected by some reviewers, and found all rejecters, should ask for their approval
			exit_with_errmsg("this pr is rejected by reviewers: %s, cannot merge it, please get approved and try again" % list(rejecters), -1)
		'''
	if never_found_review_comment:
		exit_with_errmsg("this pull request has not been approved, cannot merge it", -1)
	info("verified: this pr is reviewed by %s and finally approved by %s, it's good to merge" % (list(reviewers), list(approvers)))
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
			#reviewer_email_mapping[account] = committers[account]
	return reviewer_email_mapping

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
	try:
		result = run_command("git branch --list %s" % branch).strip()
		return result != ""
	except subprocess.CalledProcessError as e:
		exit_with_errmsg(e, -1)

def get_commit_history(branch, required_count):
	try:
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
	except subprocess.CalledProcessError as e:
		exit_with_errmsg(e, -1)

def compose_commit_template(commit_msg_template_file_path, pr_number, jira_id, pr_title_content, commit_description_lines, jira_url, author, author_email, reviewer_email_mapping):
	if os.path.exists(commit_msg_template_file_path):
		os.remove(commit_msg_template_file_path)
	comp = ["@%s <%s>" % (key, reviewer_email_mapping.get(key)) for key in reviewer_email_mapping.keys()]
	reviewer_row = "Reviewer(s): %s\r\n" % ", ".join(comp)
	with open(commit_msg_template_file_path, "w") as f:
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

def merge_and_push(pushable_remote_name, base_sha, pr_number, temp_dir, jira_id, pr_title_content, jira_url, author, reviewer_email_mapping):
	original_branch = None
	operating_branch = "%s%s" % (OPERATING_BRANCH_PREFIX, str(time.time()).replace(".", ""))
	commit_msg_template_file_path = compose_commit_msg_template(temp_dir, pr_number)
	download_url = GITHUB_PATCH_URL_TEMPLATE % pr_number
	patch_file_path = compose_patch_file_path(temp_dir, pr_number)

	current_dir = run_command("pwd").strip()

	success = False

	try:
		repo_root_dir = get_repo_root_dir()
		os.chdir(repo_root_dir)   # this step is very important to 'git applly', without it, the command executed from some location may do nothing without notifications

		# download patch
		info("downloading %s.patch from github..." % pr_number)
		download_file(download_url, patch_file_path)

		# choose the right branch and check the sha
		original_branch = run_command("git rev-parse --abbrev-ref HEAD").strip()
		run_command("git fetch %s master" % pushable_remote_name)
		remote_master_sha = run_command("git rev-parse %s/master" % pushable_remote_name).strip()
		if base_sha != remote_master_sha:
			warn("the master branch of remote <%s> has commit(s) ahead of the ones in this %s.patch" % (pushable_remote_name, pr_number))
		run_command("git checkout -b %s %s/master" % (operating_branch, pushable_remote_name))
		run_command("git apply %s" % patch_file_path)

		# query email address of the author
		author_email = ""
		if author in reviewer_email_mapping.keys():   # the email of the author in reviewer list has been queried, no need to query again
			author_email = reviewer_email_mapping[author]
		else:
			info("querying email address of author <%s> from github" % author)
			author_info = get_info_from_github(GITHUB_USER_INFO_TEMPLATE % author)
			author_email = author_info['email'].encode(ENCODING)
		if not author_email or author_email.strip() == "":   # means couldn't find public email of author
			log_error = False
			skip_author_email_regexp = re.compile("^no$", re.I)
			# ask operater to input email for the author, skip by "no" or validate the input string
			while not is_valid_email(author_email):
				if not log_error:   # the first loop, shall not log error
					log_error = True
				else:
					error("the input is not a valid email address")
				author_email = ask_for_input("couldn't find public email of author <%s>, please input one (<input \"no\" and press Enter key> to skip):" % author).strip()
				if re.search(skip_author_email_regexp, author_email):
					author_email = ""
					break

		# prompt user to input description of this merge, better to open a vi
		commit_description_lines = ask_for_multiline_input("please input the description of the merge, which will be regarded as a part of the final commit message\n(<press Enter key> to start new lines, <press Ctrl+D> to finish inputting):")
		# in the input multi-line message, discard redundant blank lines after the meanful lines
		while len(commit_description_lines) != 0 and commit_description_lines[-1].strip() == "":
			commit_description_lines.pop()

		# compose the template file
		compose_commit_template(commit_msg_template_file_path, pr_number, jira_id, pr_title_content, commit_description_lines, jira_url, author, author_email, reviewer_email_mapping)

		# commit
		run_command("git commit -aF %s" % commit_msg_template_file_path)
		info("committed to local repository on branch %s, the commit history now looks like: " % operating_branch)
		history = get_commit_history(operating_branch, 2)
		to_console("%s" % history)
		newest_commit = run_command("git rev-parse HEAD").strip()
		choice = ask_for_input("shall we push commit <%s> to remote <%s/master>?\n(<input \"y\" or \"n\" and press Enter key>):" % (newest_commit, pushable_remote_name)).strip()
		if choice == "Y" or choice == "y":
			# push to remote
			info("pushing to remote repository...")
			run_command("git push %s %s:master" % (pushable_remote_name, operating_branch))
			success = True
		else:
			# log the info and depend finally-block to recover environment
			info("recovering working directory to original state...")
		return success
	except subprocess.CalledProcessError as e:
		exit_with_errmsg(e, -1)
	finally:
		if success:
			try:
				run_command("git checkout %s" % original_branch)
				run_command("git branch -D %s" % operating_branch)
			except subprocess.CalledProcessError as e:
				exit_with_errmsg(e, -1)
			finally:
				os.remove(commit_msg_template_file_path)
				os.remove(patch_file_path)
		else:
			try:
				if original_branch:
					if not is_working_directory_clean():
						run_command("git reset --hard %s" % original_branch)
					run_command("git checkout %s" % original_branch)
					if has_branch(operating_branch):
						run_command("git branch -D %s" % operating_branch)
			except subprocess.CalledProcessError as e:
				exit_with_errmsg(e, -1)
			finally:
				if os.path.exists(commit_msg_template_file_path):
					os.remove(commit_msg_template_file_path)
				if os.path.exists(patch_file_path):
					os.remove(patch_file_path)
		os.chdir(current_dir)

def main(argv):
	# cope with invoking arguments
	if len(argv) == 0:
		display_help("pull request number must be provided")
		exit(-1)
	pr_number = ""
	try:
		opts, args = getopt.getopt(argv, "hn:", ["log-level="])
	except getopt.GetoptError as e:
		display_help(e.msg)
		sys.exit(-1)
	for opt, arg in opts:
		if opt == "-h":
			display_help()
			exit(1)
		elif opt == "-n":
			pr_number = arg
		elif opt == "--log-level":
			log_level_name = arg
			match = LOG_LEVEL_REGEXP.search(log_level_name)
			if match:
				global log_level
				log_level = LOG_LEVEL_MAPPING[match.group(1).lower()]
			else:
				display_help("un-recognized log level: %s" % log_level_name)
				exit(-1)

	# validate pr info
	to_console("-------------------- START PROCESSING --------------------")
	info("query pr information from github...")
	pr_info_url = PR_INFO_API_TEMPLATE % pr_number
	pr_info = get_info_from_github(pr_info_url)

	# validate pr state
	pr_state = pr_info['state'].encode(ENCODING)
	if pr_state != GITHUB_PR_STATE_OPEN:
		exit_with_errmsg("the state of pr %s is not %s, please try another pull request number" % (pr_number, GITHUB_PR_STATE_OPEN), -1)

	# validate if pr has conflict(s)
	pr_mergeable = pr_info['mergeable']
	if not pr_mergeable:
		exit_with_errmsg("this pull request contains conflict(s) that must be revolved", -1)

	# guarantee the environment for operations
	(pushable_remote_name, temp_dir) = ensure_env(pr_number)

	# validate and extract info from pr title, then generate jira url
	# currently will not check if the jira ticket is valid for reason of authentication complication
	(jira_id, pr_title_content) = parse_pr_title(pr_info['title'].encode(ENCODING))
	jira_url = JIRA_TICKET_LINK_TEMPLATE % jira_id

	# check if the pr is well approved and get reviewers' account together with their email
	last_push_timestamp = pr_info['head']['repo']['pushed_at'].encode(ENCODING)
	pr_author = pr_info['user']['login'].encode(ENCODING)
	reviewer_accounts = ensure_quality_metrics(pr_number, pr_author, last_push_timestamp)
	reviewer_email_mapping = get_reviewer_email_mapping(reviewer_accounts)

	# merge and push to github, to finish the whole process
	base_sha = pr_info['base']['sha'].encode(ENCODING)
	success = merge_and_push(pushable_remote_name, base_sha, pr_number, temp_dir, jira_id, pr_title_content, jira_url, pr_author, reviewer_email_mapping)

	# notify user to update the forked repo for original has been updated
	if success:
		info("pull request <%s> has been applied and pushed to remote <%s/master> of repository <%s>" % (pr_number, pushable_remote_name, APACHE_GIT_REPO_URL))
		info("if you're going to continue with any forked repository, please update it accordingly")
	else:
		info("the process is cancelled, and the environment has been recovered")
	info("Bye.")
	to_console("-------------------- ENDED PROCESSING --------------------")

if __name__ == "__main__":
	main(sys.argv[1:])
