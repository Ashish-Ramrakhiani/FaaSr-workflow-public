#!/usr/bin/env python3
"""
Script to set cron timers for FaaSr workflows on GitHub Actions
Modifies the registered action workflow to add schedule trigger
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
            "branch": server_config.get("Branch", "main"),
            "repo": server_config.get("ActionRepoName", "")
        }
        
        return config
        
    except KeyError as e:
        logger.error(f"Missing required configuration key: {e}")
        sys.exit(1)


def get_workflow_yaml_path(workflow_name, entry_action):
    """Get the path to the workflow YAML file"""
    workflow_file = f"{workflow_name}-{entry_action}.yml"
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


def get_payload_url(workflow_file_path, repo, branch):
    """
    Construct the GitHub raw URL for the workflow JSON file
    This will be used as the default PAYLOAD_URL for scheduled runs
    """
    # Remove any leading './' or '/' from workflow file path
    clean_path = workflow_file_path.lstrip('./')
    
    # Construct GitHub raw URL
    # Format: https://raw.githubusercontent.com/owner/repo/branch/path/to/file.json
    github_repo = os.getenv("GITHUB_REPOSITORY")
    if not github_repo:
        logger.error("GITHUB_REPOSITORY environment variable not set")
        sys.exit(1)
    
    payload_url = f"https://raw.githubusercontent.com/{github_repo}/{branch}/{clean_path}"
    
    logger.info(f"Default payload URL: {payload_url}")
    return payload_url


def set_timer_in_yaml(workflow_yaml, cron_schedule, payload_url):
    """
    Add or update cron schedule in workflow YAML and set default values
    for OVERWRITTEN and PAYLOAD_URL to support scheduled runs
    """
    
    # Handle different possible trigger key names and fix them
    trigger_section = None
    
    if 'true' in workflow_yaml:
        # Fix the 'true:' key to 'on:'
        logger.info("Fixing 'true:' key to 'on:'")
        trigger_section = workflow_yaml.pop('true')
        workflow_yaml['on'] = trigger_section
    elif 'on' in workflow_yaml:
        trigger_section = workflow_yaml['on']
    else:
        # Create new 'on' section
        trigger_section = {}
        workflow_yaml['on'] = trigger_section
    
    # Ensure trigger section is a dict
    if not isinstance(trigger_section, dict):
        logger.warning("Converting trigger section to dict")
        old_value = trigger_section
        trigger_section = {}
        workflow_yaml['on'] = trigger_section
    
    # Check if schedule already exists
    had_schedule = 'schedule' in trigger_section
    
    # Add schedule to the trigger section
    trigger_section['schedule'] = [{'cron': cron_schedule}]
    
    # Add default values to workflow_dispatch inputs if they exist
    if 'workflow_dispatch' in trigger_section:
        if 'inputs' not in trigger_section['workflow_dispatch']:
            trigger_section['workflow_dispatch']['inputs'] = {}
        
        inputs = trigger_section['workflow_dispatch']['inputs']
        
        # Add defaults to OVERWRITTEN input
        if 'OVERWRITTEN' in inputs:
            inputs['OVERWRITTEN']['required'] = False
            inputs['OVERWRITTEN']['default'] = '{}'
            logger.info("Added default value to OVERWRITTEN input")
        
        # Add defaults to PAYLOAD_URL input
        if 'PAYLOAD_URL' in inputs:
            inputs['PAYLOAD_URL']['required'] = False
            inputs['PAYLOAD_URL']['default'] = payload_url
            logger.info("Added default payload URL to PAYLOAD_URL input")
    
    # Update environment variables to use defaults when inputs are empty
    if 'jobs' in workflow_yaml:
        for job_name, job_config in workflow_yaml['jobs'].items():
            if 'env' in job_config:
                env_vars = job_config['env']
                
                # Update OVERWRITTEN to use default if empty
                if 'OVERWRITTEN' in env_vars:
                    # Check if it already has the || operator
                    current_value = str(env_vars['OVERWRITTEN'])
                    if '||' not in current_value:
                        env_vars['OVERWRITTEN'] = "${{ github.event.inputs.OVERWRITTEN || '{}' }}"
                
                # Update PAYLOAD_URL to use default if empty
                if 'PAYLOAD_URL' in env_vars:
                    # Check if it already has the || operator
                    current_value = str(env_vars['PAYLOAD_URL'])
                    if '||' not in current_value:
                        env_vars['PAYLOAD_URL'] = f"${{{{ github.event.inputs.PAYLOAD_URL || '{payload_url}' }}}}"
    
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
                      check=True, capture_output=True)
        
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
    
    workflow_name = workflow_data.get("WorkflowName", "default")
    logger.info(f"Workflow name: {workflow_name}")
    
    entry_action = get_entry_action(workflow_data)
    
    gh_config = get_github_actions_config(workflow_data, entry_action)
    
    yaml_path, workflow_file = get_workflow_yaml_path(workflow_name, entry_action)
    check_workflow_registered(yaml_path)
    
    # Construct the payload URL for the workflow JSON file
    payload_url = get_payload_url(args.workflow_file, gh_config['repo'], gh_config['branch'])
    
    workflow_yaml = read_workflow_yaml(yaml_path)
    workflow_yaml = set_timer_in_yaml(workflow_yaml, args.cron, payload_url)
    write_workflow_yaml(yaml_path, workflow_yaml)
    
    commit_and_push_changes(yaml_path, workflow_file, args.cron, gh_config['branch'])
    
    logger.info("")
    logger.info("Timer configuration complete")
    logger.info(f"Workflow {entry_action} will run on schedule: {args.cron}")


if __name__ == "__main__":
    main()
