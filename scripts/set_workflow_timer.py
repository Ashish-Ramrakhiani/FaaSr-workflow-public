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
        
        branch = server_config.get("Branch", "main")
        
        # Clean branch name - remove refs/heads/ if present
        if branch.startswith("refs/heads/"):
            branch = branch.replace("refs/heads/", "")
            logger.info(f"Cleaned branch name: {branch}")
        
        config = {
            "branch": branch,
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
        logger.error("Please register the workflow first using the FAASR REGISTER action")
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
    except Exception as e:
        logger.error(f"Failed to read workflow YAML: {e}")
        sys.exit(1)


def get_payload_url(workflow_file_path, github_repo, branch):
    """
    Construct the payload URL in the format that works with FaaSrPayload
    Format: owner/repo/branch/path (WITHOUT https://raw.githubusercontent.com/ prefix)
    The container's FaaSrPayload class adds the prefix internally
    """
    # Remove any leading './' or '/' from workflow file path
    clean_path = workflow_file_path.lstrip('./')
    
    # Construct the SHORT format that FaaSrPayload expects
    # Format: owner/repo/branch/file.json
    payload_url = f"{github_repo}/{branch}/{clean_path}"
    
    logger.info(f"Payload URL: {payload_url}")
    return payload_url


def set_timer_in_yaml(workflow_yaml, cron_schedule, payload_url):
    """
    Add or update cron schedule in workflow YAML
    Match the exact structure of the working YAML
    """
    
    # Handle 'on' key (PyYAML sometimes parses it as True or 'true')
    on_section = None
    on_key = None
    
    for key in ['on', True, 'true']:
        if key in workflow_yaml:
            on_section = workflow_yaml[key]
            on_key = key
            break
    
    if on_section is None:
        logger.error("Could not find 'on' section in workflow YAML")
        sys.exit(1)
    
    # Remove old key if it's not 'on'
    if on_key != 'on':
        del workflow_yaml[on_key]
    
    # Ensure on_section is a dict
    if not isinstance(on_section, dict):
        on_section = {}
    
    # Update workflow_dispatch inputs to support scheduled runs
    if 'workflow_dispatch' in on_section:
        if 'inputs' in on_section['workflow_dispatch']:
            inputs = on_section['workflow_dispatch']['inputs']
            
            # CHANGE 1: Update OVERWRITTEN input
            if 'OVERWRITTEN' in inputs:
                inputs['OVERWRITTEN']['required'] = False  # ← Changed from true to false
                inputs['OVERWRITTEN']['default'] = '{}'    # ← Added default
                logger.info("✓ Updated OVERWRITTEN: required=false, default='{}'")
            
            # CHANGE 2: Update PAYLOAD_URL input  
            if 'PAYLOAD_URL' in inputs:
                inputs['PAYLOAD_URL']['required'] = False   # ← Changed from true to false
                inputs['PAYLOAD_URL']['default'] = payload_url  # ← Added default
                logger.info(f"✓ Updated PAYLOAD_URL: required=false, default='{payload_url}'")
    
    # CHANGE 3: Add schedule section
    had_schedule = 'schedule' in on_section
    on_section['schedule'] = [{'cron': cron_schedule}]
    
    if had_schedule:
        logger.info(f"✓ Updated schedule: {cron_schedule}")
    else:
        logger.info(f"✓ Added schedule: {cron_schedule}")
    
    # Put the updated on_section back
    workflow_yaml['on'] = on_section
    
    # CHANGE 4: Update env vars to use || operator for defaults
    if 'jobs' in workflow_yaml:
        for job_name, job_config in workflow_yaml['jobs'].items():
            if 'env' in job_config:
                env_vars = job_config['env']
                
                # Update OVERWRITTEN env var
                if 'OVERWRITTEN' in env_vars:
                    current = str(env_vars['OVERWRITTEN'])
                    if '||' not in current:
                        env_vars['OVERWRITTEN'] = "${{ github.event.inputs.OVERWRITTEN || '{}' }}"
                        logger.info("✓ Updated OVERWRITTEN env to use default fallback")
                
                # Update PAYLOAD_URL env var
                if 'PAYLOAD_URL' in env_vars:
                    current = str(env_vars['PAYLOAD_URL'])
                    if '||' not in current:
                        env_vars['PAYLOAD_URL'] = f"${{{{ github.event.inputs.PAYLOAD_URL || '{payload_url}' }}}}"
                        logger.info("✓ Updated PAYLOAD_URL env to use default fallback")
    
    return workflow_yaml


def write_workflow_yaml(yaml_path, workflow_yaml):
    """Write updated workflow YAML back to file with proper key ordering"""
    try:
        # Custom representer to handle GitHub Actions expressions
        def str_representer(dumper, data):
            if '${{' in data and '}}' in data:
                # Don't quote strings with GitHub Actions expressions
                return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='')
            return dumper.represent_scalar('tag:yaml.org,2002:str', data)
        
        yaml.add_representer(str, str_representer)
        
        # CRITICAL FIX: Rebuild workflow_yaml in the correct order
        ordered_workflow = {}
        
        # Order 1: name (if exists)
        if 'name' in workflow_yaml:
            ordered_workflow['name'] = workflow_yaml['name']
        
        # Order 2: on
        if 'on' in workflow_yaml:
            ordered_workflow['on'] = workflow_yaml['on']
        
        # Order 3: jobs
        if 'jobs' in workflow_yaml:
            ordered_workflow['jobs'] = workflow_yaml['jobs']
        
        # Order 4: any other keys
        for key in workflow_yaml:
            if key not in ['name', 'on', 'jobs']:
                ordered_workflow[key] = workflow_yaml[key]
        
        # Dump YAML with ordered structure
        yaml_content = yaml.dump(
            ordered_workflow,
            default_flow_style=False,
            sort_keys=False,
            width=1000,
            allow_unicode=True
        )
        
        # Fix 'on:' being converted to something else
        yaml_content = yaml_content.replace('true:', 'on:', 1)
        yaml_content = yaml_content.replace('"on":', 'on:', 1)
        yaml_content = yaml_content.replace("'on':", 'on:', 1)
        
        with open(yaml_path, 'w') as f:
            f.write(yaml_content)
        
        logger.info(f"✓ Wrote updated YAML to: {yaml_path}")
        
    except Exception as e:
        logger.error(f"Failed to write workflow YAML: {e}")
        sys.exit(1)


def commit_and_push_changes(yaml_path, workflow_file, cron_schedule, branch):
    """Commit and push changes to GitHub"""
    import subprocess
    
    try:
        result = subprocess.run(
            ['git', 'diff', '--quiet', yaml_path],
            capture_output=True
        )
        
        if result.returncode == 0:
            logger.info("No changes detected - timer already set")
            return
        
        subprocess.run(['git', 'add', yaml_path], check=True, capture_output=True)
        
        commit_msg = f"FaaSr: Set timer '{cron_schedule}' for {workflow_file}"
        subprocess.run(
            ['git', 'commit', '-m', commit_msg],
            check=True,
            capture_output=True
        )
        
        subprocess.run(
            ['git', 'push', 'origin', branch],
            check=True,
            capture_output=True
        )
        
        logger.info(f"✓ Committed and pushed to branch: {branch}")
        
    except subprocess.CalledProcessError as e:
        logger.error("Git operation failed")
        if e.stdout:
            logger.error(f"Output: {e.stdout.decode()}")
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
    logger.info(f"Workflow: {workflow_name}")
    
    entry_action = get_entry_action(workflow_data)
    
    gh_config = get_github_actions_config(workflow_data, entry_action)
    
    yaml_path, workflow_file = get_workflow_yaml_path(workflow_name, entry_action)
    check_workflow_registered(yaml_path)
    
    # Get GitHub repo from environment
    github_repo = os.getenv("GITHUB_REPOSITORY")
    if not github_repo:
        logger.error("GITHUB_REPOSITORY environment variable not set")
        sys.exit(1)
    
    # Construct payload URL (SHORT format without https:// prefix)
    payload_url = get_payload_url(args.workflow_file, github_repo, gh_config['branch'])
    
    # Read, update, and write YAML
    workflow_yaml = read_workflow_yaml(yaml_path)
    workflow_yaml = set_timer_in_yaml(workflow_yaml, args.cron, payload_url)
    write_workflow_yaml(yaml_path, workflow_yaml)
    
    # Commit and push
    commit_and_push_changes(yaml_path, workflow_file, args.cron, gh_config['branch'])
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("Timer successfully configured!")
    logger.info(f"  Schedule: {args.cron}")
    logger.info(f"  Workflow: {entry_action}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
