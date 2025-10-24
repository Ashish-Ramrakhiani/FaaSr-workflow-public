#!/usr/bin/env python3

import argparse
import json
import logging
import os
import subprocess
import sys
import textwrap
import time

import boto3
import requests
from FaaSr_py import graph_functions as faasr_gf
from github import Github
from croniter import croniter
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Deploy FaaSr functions to specified platform"
    )
    parser.add_argument(
        "--workflow-file", required=True, help="Path to the workflow JSON file"
    )
    parser.add_argument(
        "--cron",
        required=False,
        help="Cron schedule expression (e.g., '*/5 * * * *') - optional"
    )
    parser.add_argument(
        "--unset-timer",
        action="store_true",
        help="Unset (remove) any existing timer from the workflow"
    )
    
    args = parser.parse_args()
    
    # Validation: cannot have both --cron and --unset-timer
    if args.cron and args.unset_timer:
        logger.error("Error: Cannot use --cron and --unset-timer together")
        logger.error("Please choose one:")
        logger.error("  Use --cron to set/update a timer")
        logger.error("  Use --unset-timer to remove a timer")
        logger.error("  Use neither for regular registration")
        sys.exit(1)
    
    return args


def validate_cron_expression(cron_expr):
    """Validate cron expression syntax"""
    if not cron_expr:
        return True  # Optional parameter
    
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


def read_workflow_file(file_path):
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Error: Workflow file {file_path} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error(f"Error: Invalid JSON in workflow file {file_path}")
        sys.exit(1)


def verify_containers(workflow_data):
    """Check if custom containers are specified via environment variable"""
    custom_container = os.getenv("CUSTOM_CONTAINER", "false").lower() == "true"

    if custom_container:
        logger.info("Using custom containers")
        return

    # Get set of native containers
    with open("scripts/native_containers.txt", "r") as f:
        native_containers = {line.strip() for line in f.readlines()}

    for container in workflow_data.get("ActionContainers", {}).values():
        if container not in native_containers:
            logger.error(
                f"Custom container {container} not in native_containers.txt -- to use it, you must enable custom containers"
            )
            sys.exit(1)


def generate_github_secret_imports(faasr_payload):
    """Generate GitHub Actions secret import commands from FaaSr payload."""
    import_statements = []

    # Add secrets for compute servers
    for faas_name, compute_server in faasr_payload.get("ComputeServers", {}).items():
        faas_type = compute_server.get("FaaSType", "")
        match (faas_type):
            case "GitHubActions":
                pat_secret = f"{faas_name}_PAT"
                import_statements.append(
                    f"{pat_secret}: ${{{{ secrets.{pat_secret}}}}}"
                )
            case "Lambda":
                access_key = f"{faas_name}_AccessKey"
                secret_key = f"{faas_name}_SecretKey"
                import_statements.extend(
                    [
                        f"{access_key}: ${{{{ secrets.{access_key}}}}}",
                        f"{secret_key}: ${{{{ secrets.{secret_key}}}}}",
                    ]
                )
            case "OpenWhisk":
                api_key = f"{faas_name}_APIkey"
                import_statements.append(f"{api_key}: ${{{{ secrets.{api_key}}}}}")
            case "GoogleCloud":
                secret_key = f"{faas_name}_SecretKey"
                import_statements.append(
                    f"{secret_key}: ${{{{ secrets.{secret_key}}}}}"
                )
            case "SLURM":
                token = f"{faas_name}_Token"
                import_statements.append(f"{token}: ${{{{ secrets.{token}}}}}")
            case _:
                logger.error(
                    f"Unknown FaaSType ({faas_type}) for compute server: {faas_name} - cannot generate secrets"
                )
                sys.exit(1)

    # Add secrets for data stores
    for s3_name in faasr_payload.get("DataStores", {}).keys():
        secret_key = f"{s3_name}_SecretKey"
        access_key = f"{s3_name}_AccessKey"
        import_statements.extend(
            [
                f"{access_key}: ${{{{ secrets.{access_key}}}}}",
                f"{secret_key}: ${{{{ secrets.{secret_key}}}}}",
            ]
        )

    if "VMConfig" in faasr_payload:
        vm_config = faasr_payload["VMConfig"]
        vm_name = vm_config.get("Name")

        if vm_name:
            provider = vm_config.get("Provider", "AWS")

            if provider == "AWS":
                access_key = f"{vm_name}_AccessKey"
                secret_key = f"{vm_name}_SecretKey"
                import_statements.extend(
                    [
                        f"{access_key}: ${{{{ secrets.{access_key}}}}}",
                        f"{secret_key}: ${{{{ secrets.{secret_key}}}}}",
                    ]
                )

    # Indent each line for YAML formatting
    indent = " " * 20
    import_statements = "\n".join(f"{indent}{s}" for s in import_statements)

    return import_statements


def generate_serverless_yaml(action_name, container_image, secret_imports, cron_schedule=None, payload_url=None):
    """
    Generate YAML for serverless (GitHub-hosted runner)
    
    Args:
        action_name: Name of the action
        container_image: Docker container image
        secret_imports: Secret import statements
        cron_schedule: Optional cron schedule string (None to not include schedule)
        payload_url: Optional default payload URL for scheduled runs
    """
    
    # Build 'on' section based on whether schedule is requested
    if cron_schedule:
        # WITH SCHEDULE - Include schedule section and default values
        on_section = f"""on:
  schedule:
    - cron: '{cron_schedule}'
  workflow_dispatch:
    inputs:
      OVERWRITTEN:
        description: "Overwritten fields"
        required: false
        default: '{{}}'
      PAYLOAD_URL:
        description: "URL to payload"
        required: false
        default: {payload_url}"""
        
        # Add default values to env vars for scheduled runs
        overwritten_env = '                    OVERWRITTEN: ${{ github.event.inputs.OVERWRITTEN || \'{}\' }}'
        payload_url_env = f'                    PAYLOAD_URL: ${{{{ github.event.inputs.PAYLOAD_URL || \'{payload_url}\' }}}}'
    else:
        # WITHOUT SCHEDULE - Regular registration, inputs are required
        on_section = """on:
  workflow_dispatch:
    inputs:
      OVERWRITTEN:
        description: "Overwritten fields"
        required: true
      PAYLOAD_URL:
        description: "URL to payload"
        required: true"""
        
        # No default values needed for manual-only workflows
        overwritten_env = '                    OVERWRITTEN: ${{ github.event.inputs.OVERWRITTEN }}'
        payload_url_env = '                    PAYLOAD_URL: ${{ github.event.inputs.PAYLOAD_URL }}'
    
    return textwrap.dedent(
        f"""\
        name: {action_name}

        {on_section}

        jobs:
            run_docker_image:
                runs-on: ubuntu-latest
                container: {container_image}

                env:
{secret_imports}
{overwritten_env}
{payload_url_env}

                steps:
                  - name: Run Python entrypoint
                    run: |
                        cd /action
                        python3 faasr_entry.py
    """
    )


def generate_vm_yaml(action_name, container_image, secret_imports, cron_schedule=None, payload_url=None):
    """
    Generate YAML for VM (self-hosted runner)
    
    Args:
        action_name: Name of the action
        container_image: Docker container image
        secret_imports: Secret import statements
        cron_schedule: Optional cron schedule string (None to not include schedule)
        payload_url: Optional default payload URL for scheduled runs
    """
    
    # Build 'on' section based on whether schedule is requested
    if cron_schedule:
        # WITH SCHEDULE - Include schedule section and default values
        on_section = f"""on:
  schedule:
    - cron: '{cron_schedule}'
  workflow_dispatch:
    inputs:
      OVERWRITTEN:
        description: "Overwritten fields"
        required: false
        default: '{{}}'
      PAYLOAD_URL:
        description: "URL to payload"
        required: false
        default: {payload_url}"""
        
        # Add default values to env vars for scheduled runs
        overwritten_env = '                    OVERWRITTEN: ${{ github.event.inputs.OVERWRITTEN || \'{}\' }}'
        payload_url_env = f'                    PAYLOAD_URL: ${{{{ github.event.inputs.PAYLOAD_URL || \'{payload_url}\' }}}}'
    else:
        # WITHOUT SCHEDULE - Regular registration, inputs are required
        on_section = """on:
  workflow_dispatch:
    inputs:
      OVERWRITTEN:
        description: "Overwritten fields"
        required: true
      PAYLOAD_URL:
        description: "URL to payload"
        required: true"""
        
        # No default values needed for manual-only workflows
        overwritten_env = '                    OVERWRITTEN: ${{ github.event.inputs.OVERWRITTEN }}'
        payload_url_env = '                    PAYLOAD_URL: ${{ github.event.inputs.PAYLOAD_URL }}'
    
    return textwrap.dedent(
        f"""\
        name: {action_name}

        {on_section}

        jobs:
            run_on_vm:
                runs-on: self-hosted
                container: {container_image}

                env:
{secret_imports}
{overwritten_env}
{payload_url_env}

                steps:
                  - name: Run Python entrypoint
                    run: |
                        cd /action
                        python3 faasr_entry.py
    """
    )


def deploy_to_github(workflow_data, cron_schedule=None, payload_url=None, entry_action=None):
    """
    Deploys GH functions to GitHub Actions
    
    Args:
        workflow_data: Workflow JSON data
        cron_schedule: Optional cron schedule for entry action
        payload_url: Optional payload URL for scheduled runs
        entry_action: Name of the entry action (only this gets the schedule)
    """
    github_token = os.getenv("GH_PAT")

    if not github_token:
        logger.error("GH_PAT environment variable not set")
        sys.exit(1)

    g = Github(github_token)

    # Get the workflow name for prefixing
    workflow_name = workflow_data.get("WorkflowName")

    # Ensure workflow name is specified
    if not workflow_name:
        logger.error("WorkflowName not specified in workflow file")
        sys.exit(1)

    json_prefix = workflow_name

    # Get the current repository
    repo_name = os.getenv("GITHUB_REPOSITORY")

    # Filter actions to be deployed to GitHub Actions
    github_actions = {}
    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config["FaaSType"].lower()
        if faas_type == "githubactions":
            github_actions[action_name] = action_data

    if not github_actions:
        logger.info("No actions found for GitHub Actions deployment")
        return

    try:
        repo = g.get_repo(repo_name)

        # Get the default branch name
        default_branch = repo.default_branch
        logger.info(f"Using branch: {default_branch}")

        # Deploy each action
        for action_name, action_data in github_actions.items():
            # Create prefixed action name using workflow_name-action_name format
            prefixed_action_name = f"{json_prefix}-{action_name}"

            requires_vm = action_data.get("RequiresVM", False)

            # Create workflow file
            # Get container image, with fallback to default
            container_image = workflow_data.get("ActionContainers", {}).get(action_name)

            # Ensure container image is specified
            if not container_image:
                logger.error(f"No container specified for action: {action_name}")
                sys.exit(1)

            # Dynamically set required secrets and variables
            secret_imports = generate_github_secret_imports(workflow_data)

            # Determine if this action should get the timer
            # Only the entry action gets the schedule
            is_entry_action = (action_name == entry_action)
            action_cron = cron_schedule if is_entry_action else None
            action_payload_url = payload_url if is_entry_action else None

            if requires_vm:
                workflow_content = generate_vm_yaml(
                    prefixed_action_name,
                    container_image,
                    secret_imports,
                    cron_schedule=action_cron,
                    payload_url=action_payload_url
                )
            else:
                workflow_content = generate_serverless_yaml(
                    prefixed_action_name,
                    container_image,
                    secret_imports,
                    cron_schedule=action_cron,
                    payload_url=action_payload_url
                )

            # Create or update the workflow file
            workflow_file = f".github/workflows/{prefixed_action_name}.yml"
            try:
                # Try to get the existing file
                existing_file = repo.get_contents(workflow_file, ref=default_branch)
                repo.update_file(
                    workflow_file,
                    f"Update workflow: {prefixed_action_name}",
                    workflow_content,
                    existing_file.sha,
                    branch=default_branch,
                )
                logger.info(f"Updated workflow: {prefixed_action_name}")
            except Exception:
                # File does not exist, create it
                repo.create_file(
                    workflow_file,
                    f"Create workflow: {prefixed_action_name}",
                    workflow_content,
                    branch=default_branch,
                )
                logger.info(f"Created workflow: {prefixed_action_name}")

        logger.info("GitHub Actions deployment completed successfully")

    except Exception as e:
        logger.error(f"GitHub Actions deployment failed: {e}")
        sys.exit(1)


def deploy_to_aws(workflow_data):
    """Deploys functions to AWS Lambda"""
    logger.info("Deploying to AWS Lambda...")

    # Get AWS credentials from environment
    aws_access_key = os.getenv("AWS_AccessKey")
    aws_secret_key = os.getenv("AWS_SecretKey")
    aws_arn = os.getenv("AWS_ARN")

    if not aws_access_key or not aws_secret_key:
        logger.error(
            "AWS_AccessKey and AWS_SecretKey environment variables must be set for Lambda deployment"
        )
        sys.exit(1)

    # Filter actions to be deployed to Lambda
    lambda_actions = {}
    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config["FaaSType"].lower()
        if faas_type == "lambda":
            lambda_actions[action_name] = action_data

    if not lambda_actions:
        logger.info("No actions found for Lambda deployment")
        return

    # Create Lambda client
    region = workflow_data["ComputeServers"][
        lambda_actions[list(lambda_actions.keys())[0]]["FaaSServer"]
    ].get("Region", "us-east-1")

    lambda_client = boto3.client(
        "lambda",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )

    # Deploy each action
    for action_name, action_data in lambda_actions.items():
        logger.info(f"Deploying Lambda function: {action_name}")

        # Get container image
        container_image = workflow_data.get("ActionContainers", {}).get(action_name)

        if not container_image:
            logger.error(f"No container specified for action: {action_name}")
            sys.exit(1)

        # Parse ECR URI for account ID
        if container_image.startswith("public.ecr.aws"):
            # Public ECR format: public.ecr.aws/registry_alias/repository:tag
            account_id = None  # Public repos don't need account ID in role ARN
        elif ".dkr.ecr." in container_image:
            # Private ECR format: account-id.dkr.ecr.region.amazonaws.com/repo:tag
            account_id = container_image.split(".")[0]
        else:
            logger.error(
                f"Unsupported container URI format for {action_name}: {container_image}"
            )
            sys.exit(1)

        function_name = f"{workflow_data.get('WorkflowName', 'faasr')}-{action_name}"

        role_arn = aws_arn

        # Check if function exists
        function_exists = False
        try:
            lambda_client.get_function(FunctionName=function_name)
            function_exists = True
        except lambda_client.exceptions.ResourceNotFoundException:
            pass

        if function_exists:
            # Update existing function
            try:
                lambda_client.update_function_code(
                    FunctionName=function_name, ImageUri=container_image
                )
                logger.info(f"Updated Lambda function: {function_name}")
            except Exception as e:
                logger.error(f"Failed to update Lambda function {function_name}: {e}")
                sys.exit(1)
        else:
            # Create new function
            try:
                lambda_client.create_function(
                    FunctionName=function_name,
                    Role=role_arn,
                    Code={"ImageUri": container_image},
                    PackageType="Image",
                    Timeout=900,  # 15 minutes
                    MemorySize=3008,
                )
                logger.info(f"Created Lambda function: {function_name}")
            except Exception as e:
                logger.error(f"Failed to create Lambda function {function_name}: {e}")
                sys.exit(1)

    logger.info("AWS Lambda deployment completed successfully")


def deploy_to_ow(workflow_data):
    """Deploys functions to OpenWhisk"""
    logger.info("Deploying to OpenWhisk...")

    ow_api_key = os.getenv("OW_APIkey")

    if not ow_api_key:
        logger.error(
            "OW_APIkey environment variable must be set for OpenWhisk deployment"
        )
        sys.exit(1)

    # Filter actions to be deployed to OpenWhisk
    ow_actions = {}
    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config["FaaSType"].lower()
        if faas_type == "openwhisk":
            ow_actions[action_name] = action_data

    if not ow_actions:
        logger.info("No actions found for OpenWhisk deployment")
        return

    # Get OpenWhisk configuration from first action's server
    first_action = list(ow_actions.values())[0]
    server_name = first_action["FaaSServer"]
    server_config = workflow_data["ComputeServers"][server_name]

    api_host = server_config.get("API.host", "")
    namespace = server_config.get("Namespace", "_")
    region = server_config.get("Region", "")

    if not api_host:
        logger.error("OpenWhisk API.host not specified in workflow configuration")
        sys.exit(1)

    # Configure wsk CLI
    insecure_flag = "--insecure" if server_config.get("Insecure", False) else ""

    # Set up wsk CLI with credentials
    wsk_config_cmd = f"wsk property set --apihost {api_host} --auth {ow_api_key} --namespace {namespace} {insecure_flag}"
    result = subprocess.run(wsk_config_cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"Failed to configure OpenWhisk CLI: {result.stderr}")
        sys.exit(1)

    logger.info(f"Configured OpenWhisk CLI for {api_host}")

    # Deploy each action
    for action_name, action_data in ow_actions.items():
        logger.info(f"Deploying OpenWhisk action: {action_name}")

        # Get container image
        container_image = workflow_data.get("ActionContainers", {}).get(action_name)

        if not container_image:
            logger.error(f"No container specified for action: {action_name}")
            sys.exit(1)

        # Create or update the action
        action_cmd = f"wsk action update {action_name} --docker {container_image} --web true {insecure_flag}"
        result = subprocess.run(action_cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(
                f"Failed to deploy OpenWhisk action {action_name}: {result.stderr}"
            )
            sys.exit(1)

        logger.info(f"Deployed OpenWhisk action: {action_name}")

    logger.info("OpenWhisk deployment completed successfully")


def deploy_to_gcp(workflow_data):
    """Deploys functions to Google Cloud Functions"""
    logger.info("Deploying to Google Cloud Functions...")

    gcp_secret_key = os.getenv("GCP_SecretKey")

    if not gcp_secret_key:
        logger.error(
            "GCP_SecretKey environment variable must be set for Google Cloud deployment"
        )
        sys.exit(1)

    # Filter actions to be deployed to GCP
    gcp_actions = {}
    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config["FaaSType"].lower()
        if faas_type == "googlecloud":
            gcp_actions[action_name] = action_data

    if not gcp_actions:
        logger.info("No actions found for Google Cloud deployment")
        return

    # Get GCP configuration
    first_action = list(gcp_actions.values())[0]
    server_name = first_action["FaaSServer"]
    server_config = workflow_data["ComputeServers"][server_name]

    project_id = server_config.get("ProjectID", "")
    region = server_config.get("Region", "us-central1")

    if not project_id:
        logger.error("ProjectID not specified in Google Cloud server configuration")
        sys.exit(1)

    # Set up authentication
    with open("/tmp/gcp_key.json", "w") as f:
        f.write(gcp_secret_key)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp_key.json"

    # Deploy each action
    for action_name, action_data in gcp_actions.items():
        logger.info(f"Deploying GCP Cloud Function: {action_name}")

        # Get container image
        container_image = workflow_data.get("ActionContainers", {}).get(action_name)

        if not container_image:
            logger.error(f"No container specified for action: {action_name}")
            sys.exit(1)

        function_name = f"{workflow_data.get('WorkflowName', 'faasr')}-{action_name}"

        # Deploy using gcloud CLI
        deploy_cmd = f"""
        gcloud functions deploy {function_name} \
            --gen2 \
            --runtime=python311 \
            --region={region} \
            --source=. \
            --entry-point=main \
            --trigger-http \
            --allow-unauthenticated \
            --docker-repository={container_image}
        """

        result = subprocess.run(deploy_cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(
                f"Failed to deploy GCP function {function_name}: {result.stderr}"
            )
            sys.exit(1)

        logger.info(f"Deployed GCP Cloud Function: {function_name}")

    logger.info("Google Cloud deployment completed successfully")


def deploy_to_slurm(workflow_data):
    """
    Validates SLURM configuration for workflow actions.
    Does not create persistent resources - jobs are submitted at invocation time.
    """
    logger.info("Validating SLURM configuration...")

    # Group actions by SLURM server
    slurm_actions = {}
    slurm_servers = {}

    for action_name, action_data in workflow_data["ActionList"].items():
        server_name = action_data["FaaSServer"]
        server_config = workflow_data["ComputeServers"][server_name]
        faas_type = server_config["FaaSType"].lower()

        if faas_type == "slurm":
            if server_name not in slurm_actions:
                slurm_actions[server_name] = []
                slurm_servers[server_name] = server_config.copy()
            slurm_actions[server_name].append(action_name)

    if not slurm_actions:
        logger.info("No actions found for SLURM deployment")
        return

    # Process each SLURM server
    for server_name, actions in slurm_actions.items():
        logger.info(f"Registering workflow for SLURM: {server_name}")
        server_config = slurm_servers[server_name]

        # Validate server configuration
        validate_slurm_server_config(server_name, server_config)

        # Test connectivity
        if not test_slurm_connectivity(server_name, server_config):
            logger.error(f"Failed to connect to SLURM server: {server_name}")
            sys.exit(1)

        # Validate each action
        for action_name in actions:
            validate_slurm_action(action_name, workflow_data, server_config)

        logger.info(
            f"Successfully validated {len(actions)} action(s) for SLURM server '{server_name}'"
        )

    logger.info(
        f"SLURM configuration validated successfully. "
        f"No persistent resources created - jobs will be submitted at invocation time."
    )


def validate_slurm_server_config(server_name, server_config):
    """
    Validate SLURM server configuration has required fields.

    Args:
        server_name: Name of the SLURM server
        server_config: Server configuration dict
    """
    required_fields = ["Endpoint", "APIVersion", "Partition", "UserName"]
    missing_fields = [f for f in required_fields if not server_config.get(f)]

    if missing_fields:
        logger.error(
            f"SLURM server '{server_name}' configuration missing required fields: "
            f"{', '.join(missing_fields)}"
        )
        sys.exit(1)

    logger.info(
        f"SLURM server configuration validated: "
        f"{server_config['Endpoint']} (API: {server_config['APIVersion']})"
    )


def test_slurm_connectivity(server_name, server_config):
    """
    Test connectivity to SLURM REST API endpoint (mirrors R implementation).

    Args:
        server_name: Name of the SLURM server
        server_config: Server configuration dict

    Returns:
        bool: True if connectivity test passes
    """
    endpoint = server_config["Endpoint"]
    api_version = server_config.get("APIVersion", "v0.0.37")

    # Ensure endpoint has protocol
    if not endpoint.startswith("http"):
        endpoint = f"http://{endpoint}"

    # Test ping endpoint
    ping_url = f"{endpoint}/slurm/{api_version}/ping"

    # Prepare headers
    headers = {"Accept": "application/json"}

    # Add JWT token and username if available (for auth testing)
    slurm_token = os.getenv("SLURM_Token")
    if slurm_token:
        headers["X-SLURM-USER-TOKEN"] = slurm_token
        username = server_config.get("UserName", "ubuntu")
        headers["X-SLURM-USER-NAME"] = username

        # Validate token format
        if not slurm_token.startswith("eyJ"):
            logger.warning(
                f"SLURM_Token for '{server_name}' doesn't appear to be a valid JWT token"
            )

    try:
        response = requests.get(ping_url, headers=headers, timeout=10)

        if response.status_code == 200:
            logger.info(f"✓ SLURM connectivity test passed for: {server_name}")
            return True
        elif response.status_code in [401, 403]:
            # Authentication required but endpoint is reachable
            logger.info(f"SLURM endpoint reachable at: {server_name} ")
            return True
        else:
            logger.error(
                f"SLURM connectivity test failed: HTTP {response.status_code} - "
                f"{response.text[:200]}"
            )
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"SLURM connectivity error for '{server_name}': {e}")
        return False


def validate_slurm_action(action_name, workflow_data, server_config):
    """
    Validate a single SLURM action configuration.

    Args:
        action_name: Name of the action
        workflow_data: Full workflow JSON
        server_config: Server configuration dict
    """
    action_config = workflow_data["ActionList"][action_name]

    # Validate container image
    container_image = workflow_data.get("ActionContainers", {}).get(action_name)
    if not container_image:
        logger.error(f"No container specified for SLURM action: {action_name}")
        sys.exit(1)

    # Get resource requirements using fallback hierarchy
    resources = get_slurm_resource_requirements(
        action_name, action_config, server_config
    )

    logger.info(
        f"Validated action '{action_name}': "
        f"container={container_image}, "
        f"resources=[CPU:{resources['cpus_per_task']}, "
        f"Memory:{resources['memory_mb']}MB, "
        f"Time:{resources['time_limit']}s]"
    )


def get_slurm_resource_requirements(action_name, action_config, server_config):
    """
    Extract SLURM resource requirements with fallback hierarchy.
    Function-level → Server-level → Default values

    Args:
        action_name: Name of the action
        action_config: Action configuration dict
        server_config: Server configuration dict

    Returns:
        dict: Resource configuration
    """
    # Function-level resources (highest priority)
    function_resources = action_config.get("Resources", {})

    # Extract with fallback hierarchy
    config = {
        "partition": (
            function_resources.get("Partition")
            or server_config.get("Partition")
            or "faasr"
        ),
        "nodes": (function_resources.get("Nodes") or server_config.get("Nodes") or 1),
        "tasks": (function_resources.get("Tasks") or server_config.get("Tasks") or 1),
        "cpus_per_task": (
            function_resources.get("CPUsPerTask")
            or server_config.get("CPUsPerTask")
            or 1
        ),
        "memory_mb": (
            function_resources.get("Memory") or server_config.get("Memory") or 1024
        ),
        "time_limit": (
            function_resources.get("TimeLimit") or server_config.get("TimeLimit") or 60
        ),
        "working_dir": (
            function_resources.get("WorkingDirectory")
            or server_config.get("WorkingDirectory")
            or "/tmp"
        ),
    }

    return config


def main():
    args = parse_arguments()
    
    # Validate cron if provided
    if args.cron:
        if not validate_cron_expression(args.cron):
            logger.error("Invalid cron expression provided")
            sys.exit(1)
    
    workflow_data = read_workflow_file(args.workflow_file)

    # Store the workflow file path in the workflow data
    workflow_data["_workflow_file"] = args.workflow_file

    # Validate workflow for cycles and unreachable states
    logger.info("Validating workflow for cycles and unreachable states...")
    try:
        faasr_gf.check_dag(workflow_data)
        logger.info("Workflow validation passed")
    except SystemExit:
        logger.info("Workflow validation failed - check logs for details")

    # Verify if custom containers are specified correctly
    verify_containers(workflow_data)

    # Get workflow name and entry action
    workflow_name = workflow_data.get("WorkflowName")
    entry_action = workflow_data.get("FunctionInvoke")
    
    # Determine what to do based on flags
    cron_to_use = None
    payload_url = None
    
    if args.unset_timer:
        logger.info("Unset timer mode: Will register workflows WITHOUT schedule")
    elif args.cron:
        logger.info(f"Set timer mode: Will register workflows WITH schedule: {args.cron}")
        # Build payload URL for scheduled runs
        github_repo = os.getenv("GITHUB_REPOSITORY")
        if not github_repo:
            logger.error("GITHUB_REPOSITORY environment variable not set")
            sys.exit(1)
        
        # Get branch from workflow data (from entry action's server)
        if entry_action:
            entry_server_name = workflow_data["ActionList"][entry_action]["FaaSServer"]
            branch = workflow_data["ComputeServers"][entry_server_name].get("Branch", "main")
        else:
            branch = "main"
        
        clean_path = args.workflow_file.lstrip('./')
        payload_url = f"https://raw.githubusercontent.com/{github_repo}/{branch}/{clean_path}"
        logger.info(f"Default payload URL: {payload_url}")
        
        cron_to_use = args.cron
    else:
        logger.info("Regular registration mode: Will register workflows without schedule")

    # Get all unique FaaSTypes from workflow data
    faas_types = set()
    for server in workflow_data.get("ComputeServers", {}).values():
        if "FaaSType" in server:
            faas_types.add(server["FaaSType"].lower())

    if not faas_types:
        logger.error("Error: No FaaSType found in workflow file")
        sys.exit(1)

    logger.info(f"Found FaaS platforms: {', '.join(faas_types)}")

    # Deploy to each platform found
    for faas_type in faas_types:
        logger.info(f"\nDeploying to {faas_type}...")
        if faas_type == "lambda":
            deploy_to_aws(workflow_data)
        elif faas_type == "githubactions":
            # Pass timer parameters to GitHub Actions deployment
            deploy_to_github(workflow_data, cron_schedule=cron_to_use, payload_url=payload_url, entry_action=entry_action)
        elif faas_type == "openwhisk":
            deploy_to_ow(workflow_data)
        elif faas_type == "googlecloud":
            deploy_to_gcp(workflow_data)
        elif faas_type == "slurm":
            deploy_to_slurm(workflow_data)
        else:
            logger.error(f"Unsupported FaaSType: {faas_type}")
            sys.exit(1)
    
    # Final message
    logger.info("")
    if args.unset_timer:
        logger.info("✓ Registration complete - Timer removed from workflow")
    elif args.cron:
        logger.info(f"✓ Registration complete - Timer set to: {args.cron}")
        logger.info(f"  Entry action '{entry_action}' will run on schedule")
    else:
        logger.info("✓ Registration complete")


if __name__ == "__main__":
    main()
