#!/usr/bin/env python3
"""
Sync GitHub secrets to AWS Secrets Manager and/or Google Secret Manager.
"""

import argparse
import os
import sys
from typing import List, Optional


# Mapping of servers to their associated secrets
SERVER_SECRETS_MAP = {
    'AWS': ['AWS_AccessKey', 'AWS_SecretKey', 'AWS_ARN'],
    'GCP': ['GCP_SecretKey'],
    'OW': ['OW_APIkey'],
    'SLURM': ['SLURM_Token'],
    'GH_PAT': ['GH_PAT']
}


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync GitHub secrets to cloud secret managers"
    )

    # Server selection
    parser.add_argument(
        "--server",
        action="append",
        choices=['AWS', 'GCP', 'OW', 'SLURM', 'GH_PAT'],
        help="Server to sync secrets for (can be specified multiple times)"
    )

    # Data server name
    parser.add_argument(
        "--data-server-name",
        type=str,
        default="",
        help="Data server name (e.g., MinIO, AWS, GCP)"
    )

    # Cloud provider flags
    parser.add_argument("--sync-to-aws", action="store_true", help="Sync to AWS Secrets Manager")
    parser.add_argument("--sync-to-gcp", action="store_true", help="Sync to Google Secret Manager")

    return parser.parse_args()


def get_secrets_to_sync(args) -> List[str]:
    """Build list of secrets to sync based on arguments."""
    secrets = []

    # Add secrets based on selected servers
    if args.server:
        for server_name in args.server:
            server_secrets = SERVER_SECRETS_MAP.get(server_name, [])
            secrets.extend(server_secrets)

    # Add data server secrets
    if args.data_server_name:
        data_server_name = args.data_server_name.strip().upper()
        # Automatically construct AccessKey and SecretKey secret names
        access_key_secret = f"{data_server_name}_ACCESSKEY"
        secret_key_secret = f"{data_server_name}_SECRETKEY"
        secrets.extend([access_key_secret, secret_key_secret])

    return secrets


def sync_to_aws(secrets: List[str], aws_region: str, aws_prefix: str) -> bool:
    """Sync secrets to AWS Secrets Manager."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        print("ERROR: boto3 is not installed. Install it with: pip install boto3")
        return False

    print(f"Syncing {len(secrets)} secret(s) to AWS Secrets Manager in region {aws_region}")

    client = boto3.client('secretsmanager', region_name=aws_region)
    success = True

    for secret_name in secrets:
        # Get secret value from environment
        secret_value = os.environ.get(secret_name)

        if not secret_value or secret_value == "***":
            print(f"WARNING: Secret '{secret_name}' not found or not accessible. Skipping.")
            continue

        aws_secret_id = f"{aws_prefix}{secret_name}"

        try:
            # Check if secret exists
            try:
                client.describe_secret(SecretId=aws_secret_id)
                # Secret exists, update it
                print(f"Updating secret '{secret_name}' in AWS as '{aws_secret_id}'...")
                client.put_secret_value(
                    SecretId=aws_secret_id,
                    SecretString=secret_value
                )
                print(f"✓ Successfully updated '{aws_secret_id}'")
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    # Secret doesn't exist, create it
                    print(f"Creating secret '{secret_name}' in AWS as '{aws_secret_id}'...")
                    client.create_secret(
                        Name=aws_secret_id,
                        SecretString=secret_value
                    )
                    print(f"✓ Successfully created '{aws_secret_id}'")
                else:
                    raise
        except Exception as e:
            print(f"ERROR: Failed to sync '{secret_name}' to AWS: {e}")
            success = False

    return success


def sync_to_gcp(secrets: List[str], project_id: str, gcp_prefix: str) -> bool:
    """Sync secrets to Google Secret Manager."""
    try:
        from google.cloud import secretmanager
        from google.api_core import exceptions as gcp_exceptions
    except ImportError:
        print("ERROR: google-cloud-secret-manager is not installed. Install it with: pip install google-cloud-secret-manager")
        return False

    print(f"Syncing {len(secrets)} secret(s) to Google Secret Manager in project {project_id}")

    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_id}"
    success = True

    for secret_name in secrets:
        # Get secret value from environment
        secret_value = os.environ.get(secret_name)

        if not secret_value or secret_value == "***":
            print(f"WARNING: Secret '{secret_name}' not found or not accessible. Skipping.")
            continue

        gcp_secret_id = f"{gcp_prefix}{secret_name}"
        secret_path = f"{parent}/secrets/{gcp_secret_id}"

        try:
            # Check if secret exists
            try:
                client.get_secret(name=secret_path)
                # Secret exists, add new version
                print(f"Adding new version to existing secret '{secret_name}' in GCP as '{gcp_secret_id}'...")
                client.add_secret_version(
                    parent=secret_path,
                    payload={"data": secret_value.encode("utf-8")}
                )
                print(f"✓ Successfully added new version to '{gcp_secret_id}'")
            except gcp_exceptions.NotFound:
                # Secret doesn't exist, create it
                print(f"Creating secret '{secret_name}' in GCP as '{gcp_secret_id}'...")
                secret = client.create_secret(
                    parent=parent,
                    secret_id=gcp_secret_id,
                    secret={
                        "replication": {"automatic": {}}
                    }
                )
                # Add the initial version
                client.add_secret_version(
                    parent=secret.name,
                    payload={"data": secret_value.encode("utf-8")}
                )
                print(f"✓ Successfully created '{gcp_secret_id}'")
        except Exception as e:
            print(f"ERROR: Failed to sync '{secret_name}' to GCP: {e}")
            success = False

    return success


def main():
    """Main function."""
    args = parse_arguments()

    # Validate that at least one cloud provider is selected
    if not args.sync_to_aws and not args.sync_to_gcp:
        print("ERROR: Must specify at least one cloud provider (--sync-to-aws or --sync-to-gcp)")
        sys.exit(1)

    # Get list of secrets to sync
    secrets = get_secrets_to_sync(args)

    if not secrets:
        print("No secrets selected to sync.")
        sys.exit(0)

    print(f"Secrets to sync: {', '.join(secrets)}")

    overall_success = True

    # Sync to AWS if requested
    if args.sync_to_aws:
        aws_region = os.environ.get("AWS_REGION")
        aws_prefix = os.environ.get("AWS_SECRET_PREFIX", "")

        if not aws_region:
            print("ERROR: AWS_REGION environment variable not set")
            sys.exit(1)

        print("\n" + "=" * 60)
        print("AWS Secrets Manager Sync")
        print("=" * 60)
        if not sync_to_aws(secrets, aws_region, aws_prefix):
            overall_success = False

    # Sync to GCP if requested
    if args.sync_to_gcp:
        gcp_project_id = os.environ.get("GCP_PROJECT_ID")
        gcp_prefix = os.environ.get("GCP_SECRET_PREFIX", "")

        if not gcp_project_id:
            print("ERROR: GCP_PROJECT_ID environment variable not set")
            sys.exit(1)

        print("\n" + "=" * 60)
        print("Google Secret Manager Sync")
        print("=" * 60)
        if not sync_to_gcp(secrets, gcp_project_id, gcp_prefix):
            overall_success = False

    print("\n" + "=" * 60)
    if overall_success:
        print("✓ All secrets synced successfully")
        sys.exit(0)
    else:
        print("✗ Some secrets failed to sync")
        sys.exit(1)


if __name__ == "__main__":
    main()
