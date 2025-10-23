#!/usr/bin/env python3
"""
Script to unset (remove) cron timers from FaaSr workflows on GitHub Actions
"""
import argparse
import os
import sys
import logging
import json
import yaml
from pathlib import Path

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
        description="Unset cron timer from FaaSr workflow on GitHub Actions"
    )
    parser.add_argument(
        "--workflow-file",
        required=True,
        help="Path to the workflow JSON file"
    )
    return parser.parse_args()


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
    """Extract GitHub Actions configuration"""
    try:
        action_config = workflow_data["ActionList"][entry_action]
        server_name = action_config["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        
        faas_type = server_config.get("FaaSType", "")
        
        if faas_type != "GitHubActions":
            logger.error(f"Entry action is configured for {faas_type}, not GitHubActions")
            sys.exit(1)
        
        return {
            "branch": server_config.get("Branch", "main")
        }
    except KeyError as e:
        logger.error(f"Missing configuration key: {e}")
        sys.exit(1)


def get_workflow_yaml_path(entry_action):
    """Get the path to the workflow YAML file"""
    if entry_action.endswith('.yml') or entry_action.endswith('.yaml'):
        workflow_file = entry_action
    else:
        workflow_file = f"{entry_action}.yml"
    
    return f".github/workflows/{workflow_file}", workflow_file


def check_workflow_registered(yaml_path):
    """Check if workflow YAML file exists"""
    if not Path(yaml_path).is_file():
        logger.error(f"Workflow YAML file not found: {yaml_path}")
        logger.error("")
        logger.error("This workflow has not been registered yet.")
        logger.error("Cannot unset timer for unregistered workflow.")
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


def unset_timer_in_yaml(workflow_yaml):
    """Remove cron schedule from workflow YAML"""
    if 'on' in workflow_yaml and isinstance(workflow_yaml['on'], dict):
        if 'schedule' in workflow_yaml['on']:
            old_schedule = workflow_yaml['on']['schedule']
            del workflow_yaml['on']['schedule']
            logger.info(f"Removed cron schedule: {old_schedule}")
            return workflow_yaml, True
        else:
            logger.info("No schedule found in workflow")
            return workflow_yaml, False
    else:
        logger.info("No schedule configuration found")
        return workflow_yaml, False


def write_workflow_yaml(yaml_path, workflow_yaml):
    """Write updated workflow YAML back to file"""
    try:
        with open(yaml_path, 'w') as f:
            yaml.dump(workflow_yaml, f, default_flow_style=False, sort_keys=False)
    except Exception as e:
        logger.error(f"Failed to write workflow YAML: {e}")
        sys.exit(1)


def commit_and_push_changes(yaml_path, workflow_file, branch, had_schedule):
    """Commit and push changes to GitHub"""
    import subprocess
    
    if not had_schedule:
        logger.info("No changes to commit")
        return
    
    try:
        subprocess.run(['git', 'add', yaml_path], check=True, capture_output=True)
        
        commit_msg = f"FaaSr: Unset workflow timer for {workflow_file}"
        
        subprocess.run(['git', 'commit', '-m', commit_msg], 
                      check=True, capture_output=True)
        
        subprocess.run(['git', 'push', 'origin', branch], 
                      check=True, capture_output=True)
        
        logger.info(f"Changes pushed to branch: {branch}")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Git operation failed: {e}")
        sys.exit(1)


def main():
    """Main execution function"""
    args = parse_arguments()
    
    gh_token = os.getenv("GH_PAT")
    if not gh_token:
        logger.error("GH_PAT environment variable not set")
        sys.exit(1)
    
    workflow_data = load_workflow_json(args.workflow_file)
    
    entry_action = get_entry_action(workflow_data)
    
    gh_config = get_github_actions_config(workflow_data, entry_action)
    
    yaml_path, workflow_file = get_workflow_yaml_path(entry_action)
    check_workflow_registered(yaml_path)
    
    workflow_yaml = read_workflow_yaml(yaml_path)
    workflow_yaml, had_schedule = unset_timer_in_yaml(workflow_yaml)
    
    if had_schedule:
        write_workflow_yaml(yaml_path, workflow_yaml)
        commit_and_push_changes(yaml_path, workflow_file, gh_config['branch'], had_schedule)
    
    logger.info("")
    logger.info("Timer removal complete")
    logger.info(f"Workflow {entry_action} will no longer run on a schedule")


if __name__ == "__main__":
    main()
