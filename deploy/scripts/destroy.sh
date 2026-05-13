#!/usr/bin/env bash
# Tear down the entire Phase 6 deployment.
#
# What gets destroyed:
#   - VPC, NAT GW, subnets, route tables, SGs
#   - RDS instance (no final snapshot — data is gone)
#   - AWS MQ broker
#   - ECR repos (and ALL images in them)
#   - ECS cluster + services + task definitions
#   - ALB + target groups + listener rules
#   - Secrets Manager secrets (recovery_window_in_days=0, gone immediately)
#   - IAM roles + policies
#   - CloudWatch log groups
#
# What is NOT destroyed:
#   - terraform.tfstate / .terraform/ (local files; remove manually if needed)
#   - your AWS account, IAM user, Budget alert (created in console)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TF_DIR="$REPO_ROOT/deploy/terraform"

cd "$TF_DIR"

if [[ ! -f terraform.tfstate ]] && [[ ! -d .terraform ]]; then
  echo "✗ No terraform state found in $TF_DIR. Nothing to destroy."
  exit 1
fi

cat <<'EOF'
⚠ This will permanently delete all Phase 6 AWS resources, including:
   - The RDS MySQL instance and ALL data in it
   - Both Secrets Manager secrets (DB + MQ passwords)
   - All ECR images
   - The ALB (its DNS name will be released)

There is no undo. AWS Budget alerts and your IAM user are unaffected.

EOF

read -r -p "Type 'destroy' to confirm: " CONFIRM
if [[ "$CONFIRM" != "destroy" ]]; then
  echo "Aborted."
  exit 1
fi

echo
echo "▶ terraform destroy (this takes 5-10 min, MQ broker dominates)"
terraform destroy -input=false -auto-approve

echo
echo "✓ All Phase 6 resources destroyed."
echo
echo "Verify nothing is left billing you:"
echo "  aws ec2 describe-vpcs --filters Name=tag:Project,Values=dop"
echo "  aws rds describe-db-instances --query 'DBInstances[?DBInstanceIdentifier==\`dop-dev-mysql\`]'"
echo "  aws mq list-brokers"
