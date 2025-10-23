#!/usr/bin/env python3
"""
Script to set cron timers for FaaSr workflows on GitHub Actions
"""
import argparse
import os
import sys
import logging
import json
import yaml
from pathlib import Path
from croniter import croniter
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Set cron timer for FaaSr workflow on GitHub Actions"
    )
    parser.add_argument(
        "--workflow-file",
        required=True,
        help="Path to the workflow JSON file"
    )
    parser.add_argument(
        "--cron",
        required=True,
        help="Cron schedule expression (e.g., '*/5 * * * *')"
    )
    return parser.parse_args()


def validate_cron_expression(cron_expr):
    """Validate cron expression syntax"""
    try:
        iter = croniter(cron_expr, datetime.now())
        next_run = iter.get_next(datetime)
        
        logger.info(f"Cron expression validated: {cron_expr}")
        logger.info(f"Next scheduled run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return True
        
    except (ValueError, KeyError) as e:
        logger.error(f"Invalid cron expression: {cron_expr}")
        logger.error(f"Error: {e}")
        logger.error("")
        logger.error("Cron format: minute hour day month weekday")
        logger.error("Examples:")
        logger.error("  */5 * * * *     - Every 5 minutes")
        logger.error("  0 * * * *       - Every hour")
        logger.error("  0 0 * * *       - Every day at midnight")
        logger.error("  0 0 * * 0       - Every Sunday at midnight")
        logger.error("  30 2 * * 1-5    - Weekdays at 2:30 AM")
        return False


def load_workflow_json(workflow_path):
    """Load and parse the workflow JSON file"""
    if not Path(workflow_path).is_file():
        logger.error(f"Workflow file not found: {workflow_path}")
        sys.exit(1)
    
    try:
        with open(workflow_path, 'r') as f:
            workflow_data = json.load(f)
        return workflow_data
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse workflow JSON: {e}")
        sys.exit(1)


def get_entry_action(workflow_data):
    """Get the entry action (FunctionInvoke) from workflow"""
    entry_action = workflow_data.get("FunctionInvoke")
    if not entry_action:
        logger.error("FunctionInvoke not found in workflow JSON")
        sys.exit(1)
    
    if entry_action not in workflow_data.get("ActionList", {}):
        logger.error(f"Entry action '{entry_action}' not found in ActionList")
        sys.exit(1)
    
    logger.info(f"Entry action: {entry_action}")
    return entry_action


def get_github_actions_config(workflow_data, entry_action):
    """Extract and validate GitHub Actions configuration"""
    try:
        action_config = workflow_data["ActionList"][entry_action]
        server_name = action_config["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        
        faas_type = server_config.get("FaaSType", "")
        
        if faas_type != "GitHubActions":
            logger.error(f"Entry action is configured for {faas_type}, not GitHubActions")
            logger.error("Timer setting is only supported for GitHub Actions")
            sys.exit(1)
        
        config = {
            "branch": server_config.get("Branch", "main")
        }
        
        return config
        
    except KeyError as e:
        logger.error(f"Missing required configuration key: {e}")
        sys.exit(1)


def get_workflow_yaml_path(entry_action):
    """Get the path to the workflow YAML file"""
    if entry_action.endswith('.yml') or entry_action.endswith('.yaml'):
        workflow_file = entry_action
    else:
        workflow_file = f"{entry_action}.yml"
    
    yaml_path = f".github/workflows/{workflow_file}"
    
    return yaml_path, workflow_file


def check_workflow_registered(yaml_path):
    """Check if workflow YAML file exists"""
    if not Path(yaml_path).is_file():
        logger.error(f"Workflow YAML file not found: {yaml_path}")
        logger.error("")
        logger.error("This workflow has not been registered yet.")
        logger.error("Please register the workflow first using the FAASR REGISTER action:")
        logger.error("  1. Go to Actions tab")
        logger.error("  2. Select (FAASR REGISTER) workflow")
        logger.error("  3. Click Run workflow")
        logger.error("  4. Enter your workflow file name")
        logger.error("  5. Wait for registration to complete")
        logger.error("  6. Then run this timer setup action again")
        logger.error("")
        sys.exit(1)
    
    return True


def read_workflow_yaml(yaml_path):
    """Read existing workflow YAML file"""
    try:
        with open(yaml_path, 'r') as f:
            workflow_yaml = yaml.safe_load(f)
        return workflow_yaml
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse workflow YAML: {e}")
        sys.exit(1)


def set_timer_in_yaml(workflow_yaml, cron_schedule):
    """Add or update cron schedule in workflow YAML"""
    if 'on' not in workflow_yaml:
        workflow_yaml['on'] = {}
    
    if not isinstance(workflow_yaml['on'], dict):
        existing_trigger = workflow_yaml['on']
        workflow_yaml['on'] = {}
        if isinstance(existing_trigger, str):
            workflow_yaml['on'][existing_trigger] = None
    
    had_schedule = 'schedule' in workflow_yaml['on']
    
    workflow_yaml['on']['schedule'] = [{'cron': cron_schedule}]
    
    if had_schedule:
        logger.info(f"Updated cron schedule: {cron_schedule}")
    else:
        logger.info(f"Added cron schedule: {cron_schedule}")
    
    return workflow_yaml


def write_workflow_yaml(yaml_path, workflow_yaml):
    """Write updated workflow YAML back to file"""
    try:
        with open(yaml_path, 'w') as f:
            yaml.dump(workflow_yaml, f, default_flow_style=False, sort_keys=False)
    except Exception as e:
        logger.error(f"Failed to write workflow YAML: {e}")
        sys.exit(1)


def commit_and_push_changes(yaml_path, workflow_file, cron_schedule, branch):
    """Commit and push changes to GitHub"""
    import subprocess
    
    try:
        result = subprocess.run(['git', 'diff', '--quiet', yaml_path], 
                              capture_output=True)
        
        if result.returncode == 0:
            logger.info("No changes detected, timer may already be set to this schedule")
            return
        
        subprocess.run(['git', 'add', yaml_path], check=True, capture_output=True)
        
        commit_msg = f"FaaSr: Set workflow timer to '{cron_schedule}' for {workflow_file}"
        
        subprocess.run(['git', 'commit', '-m', commit_msg], 
                      check=True, capture_output=True)
        
        subprocess.run(['git', 'push', 'origin', branch], 
                      check=True, capture_output=True, stderr=subprocess.STDOUT)
        
        logger.info(f"Changes pushed to branch: {branch}")
        
    except subprocess.CalledProcessError as e:
        logger.error("Git operation failed")
        if e.output:
            logger.error(f"Output: {e.output.decode()}")
        if e.stderr:
            logger.error(f"Error: {e.stderr.decode()}")
        sys.exit(1)


def main():
    """Main execution function"""
    args = parse_arguments()
    
    gh_token = os.getenv("GH_PAT")
    if not gh_token:
        logger.error("GH_PAT environment variable not set")
        sys.exit(1)
    
    if not validate_cron_expression(args.cron):
        sys.exit(1)
    
    workflow_data = load_workflow_json(args.workflow_file)
    
    entry_action = get_entry_action(workflow_data)
    
    gh_config = get_github_actions_config(workflow_data, entry_action)
    
    yaml_path, workflow_file = get_workflow_yaml_path(entry_action)
    check_workflow_registered(yaml_path)
    
    workflow_yaml = read_workflow_yaml(yaml_path)
    workflow_yaml = set_timer_in_yaml(workflow_yaml, args.cron)
    write_workflow_yaml(yaml_path, workflow_yaml)
    
    commit_and_push_changes(yaml_path, workflow_file, args.cron, gh_config['branch'])
    
    logger.info("")
    logger.info("Timer configuration complete")
    logger.info(f"Workflow {entry_action} will run on schedule: {args.cron}")


if __name__ == "__main__":
    main()
