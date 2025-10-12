#!/usr/bin/env python3
"""
FaaSr VM Injection Tool

Augments workflows with VM orchestration actions based on selected strategy.

Available Strategies:

1. Sequential Strategy (--strategy sequential):
   - Injects vm_start at workflow beginning (blocks until VM ready)
   - Injects vm_stop after all workflow leaves complete
   - Simple approach: workflow waits for VM startup before proceeding

2. Parallel Strategy (--strategy parallel):
   - Injects vm_start at workflow beginning (non-blocking fire-and-forget)
   - Injects vm_poll before each VM-requiring action (polls until ready)
   - Injects vm_stop after all workflow leaves complete
   - Efficient approach: non-VM actions run in parallel during VM startup

Usage:
    python faasr_inject_vm.py --input workflow.json --strategy parallel
    python faasr_inject_vm.py --input workflow.json --strategy sequential --output custom.json
"""

import argparse
import json
import sys
import logging
from pathlib import Path
from copy import deepcopy
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


class VMInjectionTool:
    """Tool to inject VM orchestration actions into FaaSr workflows."""
    
    def __init__(self, workflow_path, output_path=None):
        """
        Initialize tool.
        
        Args:
            workflow_path: Path to input workflow JSON
            output_path: Path for output (defaults to input_augmented.json)
        """
        self.workflow_path = Path(workflow_path)
        
        if output_path:
            self.output_path = Path(output_path)
        else:
            stem = self.workflow_path.stem
            suffix = self.workflow_path.suffix
            self.output_path = self.workflow_path.parent / f"{stem}_augmented{suffix}"
        
        self.workflow = None
        self.original_workflow = None
    
    def load_workflow(self):
        """Load and validate input workflow."""
        logger.info(f"Loading workflow from: {self.workflow_path}")
        
        if not self.workflow_path.exists():
            raise FileNotFoundError(f"Workflow file not found: {self.workflow_path}")
        
        with open(self.workflow_path, 'r') as f:
            self.workflow = json.load(f)
        
        self.original_workflow = deepcopy(self.workflow)
        
        logger.info("Workflow loaded successfully")
    
    def needs_vm(self):
        """Check if workflow has any VM-requiring actions."""
        if "ActionList" not in self.workflow:
            return False
        
        for action_name, action_config in self.workflow["ActionList"].items():
            if action_config.get("RequiresVM", False):
                logger.info(f"Found VM-requiring action: {action_name}")
                return True
        
        return False
    
    def find_entry_action(self):
        """
        Find the entry point action (no predecessors).
        
        Returns:
            str: Name of entry action
        """
        action_list = self.workflow.get("ActionList", {})
        
        # Build predecessor count
        predecessor_count = {name: 0 for name in action_list.keys()}
        
        for action_name, action_config in action_list.items():
            invoke_next = action_config.get("InvokeNext", [])
            
            # Handle different InvokeNext formats
            if isinstance(invoke_next, str):
                invoke_next = [invoke_next]
            elif isinstance(invoke_next, dict):
                # Conditional invocation - collect all possible next actions
                next_actions = []
                for condition, actions in invoke_next.items():
                    if isinstance(actions, list):
                        next_actions.extend(actions)
                    else:
                        next_actions.append(actions)
                invoke_next = next_actions
            
            for next_action in invoke_next:
                if next_action in predecessor_count:
                    predecessor_count[next_action] += 1
        
        # Find action(s) with zero predecessors
        entry_actions = [name for name, count in predecessor_count.items() if count == 0]
        
        if len(entry_actions) == 0:
            raise ValueError("No entry action found (cycle in workflow?)")
        elif len(entry_actions) > 1:
            raise ValueError(f"Multiple entry actions found: {entry_actions}. Workflow must have single entry point.")
        
        return entry_actions[0]
    
    def find_leaf_actions(self):
        """
        Find all leaf actions (empty InvokeNext).
        
        Returns:
            list: Names of leaf actions
        """
        action_list = self.workflow.get("ActionList", {})
        leaves = []
        
        for action_name, action_config in action_list.items():
            invoke_next = action_config.get("InvokeNext", [])
            
            # Check if empty
            if not invoke_next or invoke_next == []:
                leaves.append(action_name)
        
        if not leaves:
            raise ValueError("No leaf actions found - workflow must have terminal nodes")
        
        return leaves
    
    def find_github_server(self):
        """
        Find a GitHub Actions server for injected actions.
        
        Returns:
            str: Server name
        """
        if "ComputeServers" not in self.workflow:
            raise ValueError("No ComputeServers defined in workflow")
        
        for server_name, server_config in self.workflow["ComputeServers"].items():
            if server_config.get("FaaSType") == "GitHubActions":
                return server_name
        
        raise ValueError("No GitHub Actions server found. VM workflows require GitHub Actions.")
    
    def find_container_for_server(self, server_name):
        """
        Find a container image for the given server.
        
        Args:
            server_name: Server name
            
        Returns:
            str: Container image or None
        """
        action_containers = self.workflow.get("ActionContainers", {})
        action_list = self.workflow.get("ActionList", {})
        
        # Find any action using this server
        for action_name, action_config in action_list.items():
            if action_config.get("FaaSServer") == server_name:
                container = action_containers.get(action_name)
                if container:
                    return container
        
        return None
    
    def inject_vm_actions_sequential(self):
        """
        Inject VM actions using sequential strategy.
        
        Sequential Strategy:
        - Injects vm_start at workflow entry point
        - vm_start blocks and waits until VM is fully ready
        - All workflow actions wait for VM startup to complete
        - Injects vm_stop after all leaf actions complete
        
        Workflow Flow:
            [vm_start + wait] → [action1] → [action2] → ... → [vm_stop]
        """
        logger.info("Applying Sequential Strategy: Start and wait, then execute")
        # Validate prerequisites
        if "VMConfig" not in self.workflow:
            raise ValueError("VMConfig required for VM workflows")
        
        # Find graph structure
        entry_action = self.find_entry_action()
        leaf_actions = self.find_leaf_actions()
        github_server = self.find_github_server()
        container = self.find_container_for_server(github_server)
        
        logger.info(f"Entry action: {entry_action}")
        logger.info(f"Leaf actions: {leaf_actions}")
        logger.info(f"GitHub server: {github_server}")
        
        # Define injected action names
        vm_start_name = "faasr-vm-start"
        vm_stop_name = "faasr-vm-stop"
        
        # Check for name conflicts
        if vm_start_name in self.workflow["ActionList"]:
            raise ValueError(f"Action name conflict: {vm_start_name} already exists")
        if vm_stop_name in self.workflow["ActionList"]:
            raise ValueError(f"Action name conflict: {vm_stop_name} already exists")
        
        # Create VM start action
        self.workflow["ActionList"][vm_start_name] = {
            "FunctionName": "vm_start",
            "FaaSServer": github_server,
            "Type": "Python",
            "RequiresVM": False,
            "InvokeNext": [entry_action],
            "_faasr_builtin": True
        }
        
        # Create VM stop action
        self.workflow["ActionList"][vm_stop_name] = {
            "FunctionName": "vm_stop",
            "FaaSServer": github_server,
            "Type": "Python",
            "RequiresVM": False,
            "InvokeNext": [],
            "_faasr_builtin": True
        }
        
        # Modify leaf actions to point to stop
        for leaf_name in leaf_actions:
            self.workflow["ActionList"][leaf_name]["InvokeNext"] = [vm_stop_name]
            logger.info(f"Modified leaf '{leaf_name}' to invoke VM stop")
        
        # Add containers for injected actions
        if "ActionContainers" not in self.workflow:
            self.workflow["ActionContainers"] = {}
        
        if container:
            self.workflow["ActionContainers"][vm_start_name] = container
            self.workflow["ActionContainers"][vm_stop_name] = container
        
        # Update FunctionInvoke to point to vm_start
        self.workflow["FunctionInvoke"] = vm_start_name
        logger.info(f"Updated FunctionInvoke to: {vm_start_name}")
        
        logger.info("VM actions injected successfully")

    def inject_vm_actions_parallel(self):
        """
        Inject VM actions using parallel strategy with polling.
        
        Parallel Strategy:
        - Injects vm_start at workflow entry (fire-and-forget, non-blocking)
        - Injects vm_poll before each VM-requiring action (polls until ready)
        - Non-VM actions can execute in parallel during VM startup
        - Injects vm_stop after all leaf actions complete
        
        Workflow Flow:
            [vm_start (async)] → [non-vm-action] (runs immediately)
                            ↓
                        [vm_poll] → [vm-action] → ...
                        ↑ waits here
        """
        logger.info("Applying Parallel Strategy: Start fires, poll before each VM action")
    
        
        if "VMConfig" not in self.workflow:
            raise ValueError("VMConfig required for VM workflows")
        
        entry_action = self.find_entry_action()
        leaf_actions = self.find_leaf_actions()
        github_server = self.find_github_server()
        container = self.find_container_for_server(github_server)
        
        logger.info(f"Entry action: {entry_action}")
        logger.info(f"Leaf actions: {leaf_actions}")
        
        # Define injected action names
        vm_start_name = "faasr-vm-start"
        vm_stop_name = "faasr-vm-stop"
        
        # Check for name conflicts
        if vm_start_name in self.workflow["ActionList"]:
            raise ValueError(f"Action name conflict: {vm_start_name} already exists")
        if vm_stop_name in self.workflow["ActionList"]:
            raise ValueError(f"Action name conflict: {vm_stop_name} already exists")
        
        # Create VM start action (fire and forget)
        self.workflow["ActionList"][vm_start_name] = {
            "FunctionName": "vm_start",
            "FaaSServer": github_server,
            "Type": "Python",
            "RequiresVM": False,
            "InvokeNext": [entry_action],
            "_faasr_builtin": True
        }
        
        # Create VM stop action (unchanged)
        self.workflow["ActionList"][vm_stop_name] = {
            "FunctionName": "vm_stop",
            "FaaSServer": github_server,
            "Type": "Python",
            "RequiresVM": False,
            "InvokeNext": [],
            "_faasr_builtin": True
        }
        
        # Find all VM-requiring actions and inject poll before each
        vm_actions = []
        for action_name, action_config in self.workflow["ActionList"].items():
            if action_config.get("RequiresVM", False):
                vm_actions.append(action_name)
        
        logger.info(f"Found {len(vm_actions)} VM-requiring actions: {vm_actions}")
        
        # For each VM action, inject a poll action as its predecessor
        for vm_action_name in vm_actions:
            poll_action_name = f"faasr-vm-poll-{vm_action_name}"
            
            if poll_action_name in self.workflow["ActionList"]:
                raise ValueError(f"Action name conflict: {poll_action_name} already exists")
            
            # Create poll action
            self.workflow["ActionList"][poll_action_name] = {
                "FunctionName": "vm_poll",
                "FaaSServer": github_server,
                "Type": "Python",
                "RequiresVM": False,
                "InvokeNext": [vm_action_name],
                "_faasr_builtin": True
            }
            
            if container:
                self.workflow["ActionContainers"][poll_action_name] = container
            
            # Find all actions that invoke this VM action and redirect them to poll
            for action_name, action_config in self.workflow["ActionList"].items():
                if action_name == poll_action_name:
                    continue
                
                invoke_next = action_config.get("InvokeNext", [])
                if isinstance(invoke_next, str):
                    invoke_next = [invoke_next]
                
                if vm_action_name in invoke_next:
                    # Replace vm_action with poll_action
                    new_invoke_next = [poll_action_name if x == vm_action_name else x for x in invoke_next]
                    action_config["InvokeNext"] = new_invoke_next
                    logger.info(f"Redirected '{action_name}' to invoke poll before '{vm_action_name}'")
        
        # Modify leaf actions to point to stop
        for leaf_name in leaf_actions:
            self.workflow["ActionList"][leaf_name]["InvokeNext"] = [vm_stop_name]
            logger.info(f"Modified leaf '{leaf_name}' to invoke VM stop")
        
        # Add containers for injected actions
        if "ActionContainers" not in self.workflow:
            self.workflow["ActionContainers"] = {}
        
        if container:
            self.workflow["ActionContainers"][vm_start_name] = container
            self.workflow["ActionContainers"][vm_stop_name] = container
        
        # Update FunctionInvoke to point to vm_start
        self.workflow["FunctionInvoke"] = vm_start_name
        logger.info(f"Updated FunctionInvoke to: {vm_start_name}")
        
        logger.info("VM actions injected successfully with parallel strategy")
    
    def save_workflow(self):
        """Save augmented workflow to output file."""
        logger.info(f"Saving augmented workflow to: {self.output_path}")
        
        with open(self.output_path, 'w') as f:
            json.dump(self.workflow, f, indent=4)
        
        logger.info("Augmented workflow saved")
    
    def run(self, strategy="parallel"): 
        """Execute full injection process."""
        try:
            self.load_workflow()
            
            if not self.needs_vm():
                logger.info("Workflow does not require VM - no injection needed")
                self.save_workflow()
                return True
            
            # Apply selected strategy
            if strategy == "parallel":
                self.inject_vm_actions_parallel()
            elif strategy == "sequential":
                self.inject_vm_actions_sequential()
            else:
                raise ValueError(f"Unknown strategy: {strategy}")
            
            self.save_workflow()
            
            logger.info("=" * 60)
            logger.info("SUCCESS: Workflow augmented with VM orchestration")
            logger.info(f"Strategy: {strategy}")
            logger.info(f"Input:  {self.workflow_path}")
            logger.info(f"Output: {self.output_path}")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to inject VM actions: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Inject VM orchestration actions into FaaSr workflows"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input workflow JSON file"
    )
    parser.add_argument(
        "--output",
        help="Path to output workflow JSON file (default: input_augmented.json)"
    )
    parser.add_argument(
        "--strategy",
        default="parallel",
        choices=["sequential", "parallel"],
        help="VM orchestration strategy: 'sequential' blocks until VM ready (simple), 'parallel' polls per VM action (efficient, default)"
    )
    
    args = parser.parse_args()

    if os.getenv("GITHUB_ACTIONS") == "true":
        logger.info("Running in GitHub Actions environment")
    
    tool = VMInjectionTool(args.input, args.output)
    success = tool.run(strategy=args.strategy)

    if os.getenv("GITHUB_ACTIONS") == "true":
        output_file = tool.output_path
        print(f"::set-output name=augmented_file::{output_file}")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
