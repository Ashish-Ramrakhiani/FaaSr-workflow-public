#!/usr/bin/env python3
"""
Script to unset (remove) cron timers from FaaSr workflows on GitHub Actions
Removes schedule section and restores inputs to required=true
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
        
        branch = server_config.get("Branch", "main")
        
        # Clean branch name - remove refs/heads/ if present
        if branch.startswith("refs/heads/"):
            branch = branch.replace("refs/heads/", "")
            logger.info(f"Cleaned branch name: {branch}")
        
        return {
            "branch": branch
        }
    except KeyError as e:
        logger.error(f"Missing configuration key: {e}")
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
        logger.error("Cannot unset timer for unregistered workflow.")
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
    """
    Remove cron schedule from workflow YAML and restore to manual-only mode
    This reverses the changes made by set_timer
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
    
    had_schedule = False
    
    # CHANGE 1: Remove schedule section
    if isinstance(on_section, dict) and 'schedule' in on_section:
        old_schedule = on_section['schedule']
        del on_section['schedule']
        logger.info(f"✓ Removed schedule: {old_schedule}")
        had_schedule = True
    else:
        logger.info("No schedule found in workflow")
    
    # CHANGE 2: Restore workflow_dispatch inputs to required=true, remove defaults
    if 'workflow_dispatch' in on_section:
        if 'inputs' in on_section['workflow_dispatch']:
            inputs = on_section['workflow_dispatch']['inputs']
            
            # Restore OVERWRITTEN input
            if 'OVERWRITTEN' in inputs:
                inputs['OVERWRITTEN']['required'] = True  # ← Changed back to true
                if 'default' in inputs['OVERWRITTEN']:
                    del inputs['OVERWRITTEN']['default']  # ← Remove default
                logger.info("✓ Restored OVERWRITTEN: required=true, removed default")
            
            # Restore PAYLOAD_URL input
            if 'PAYLOAD_URL' in inputs:
                inputs['PAYLOAD_URL']['required'] = True   # ← Changed back to true
                if 'default' in inputs['PAYLOAD_URL']:
                    del inputs['PAYLOAD_URL']['default']   # ← Remove default
                logger.info("✓ Restored PAYLOAD_URL: required=true, removed default")
    
    # Put the updated on_section back
    workflow_yaml['on'] = on_section
    
    # CHANGE 3: Remove || operator from env vars (restore to direct references)
    if 'jobs' in workflow_yaml:
        for job_name, job_config in workflow_yaml['jobs'].items():
            if 'env' in job_config:
                env_vars = job_config['env']
                
                # Restore OVERWRITTEN env var
                if 'OVERWRITTEN' in env_vars:
                    current = str(env_vars['OVERWRITTEN'])
                    if '||' in current:
                        env_vars['OVERWRITTEN'] = "${{ github.event.inputs.OVERWRITTEN }}"
                        logger.info("✓ Restored OVERWRITTEN env (removed fallback)")
                
                # Restore PAYLOAD_URL env var
                if 'PAYLOAD_URL' in env_vars:
                    current = str(env_vars['PAYLOAD_URL'])
                    if '||' in current:
                        env_vars['PAYLOAD_URL'] = "${{ github.event.inputs.PAYLOAD_URL }}"
                        logger.info("✓ Restored PAYLOAD_URL env (removed fallback)")
    
    return workflow_yaml, had_schedule


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
        
        # CRITICAL: Rebuild workflow_yaml in the correct order
        # GitHub Actions convention: name → on → jobs → everything else
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


def commit_and_push_changes(yaml_path, workflow_file, branch, had_changes):
    """Commit and push changes to GitHub"""
    import subprocess
    
    if not had_changes:
        logger.info("No changes to commit")
        return
    
    try:
        # Check if there are actually changes
        result = subprocess.run(
            ['git', 'diff', '--quiet', yaml_path],
            capture_output=True
        )
        
        if result.returncode == 0:
            logger.info("No changes detected in file")
            return
        
        subprocess.run(['git', 'add', yaml_path], check=True, capture_output=True)
        
        commit_msg = f"FaaSr: Unset workflow timer for {workflow_file}"
        
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
    
    workflow_data = load_workflow_json(args.workflow_file)
    
    workflow_name = workflow_data.get("WorkflowName", "default")
    logger.info(f"Workflow: {workflow_name}")
    
    entry_action = get_entry_action(workflow_data)
    
    gh_config = get_github_actions_config(workflow_data, entry_action)
    
    yaml_path, workflow_file = get_workflow_yaml_path(workflow_name, entry_action)
    check_workflow_registered(yaml_path)
    
    workflow_yaml = read_workflow_yaml(yaml_path)
    workflow_yaml, had_schedule = unset_timer_in_yaml(workflow_yaml)
    
    if had_schedule:
        write_workflow_yaml(yaml_path, workflow_yaml)
        commit_and_push_changes(yaml_path, workflow_file, gh_config['branch'], had_schedule)
    else:
        logger.info("No timer was set - nothing to unset")
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("Timer successfully removed!")
    logger.info(f"  Workflow: {entry_action}")
    logger.info(f"  Mode: Manual trigger only")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
