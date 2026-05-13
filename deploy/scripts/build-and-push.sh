#!/usr/bin/env bash
# Build all three service images and push to ECR.
#
# Prereqs:
#   - aws CLI configured (`aws sts get-caller-identity` succeeds)
#   - Docker daemon running
#   - ECR repos already exist — i.e. `bash deploy/scripts/deploy.sh` has
#     run terraform apply at least once. If repos don't exist, this script
#     will fail at `docker push`; fix by running deploy.sh first (it can
#     run before images exist — ECS services will crashloop until images
#     arrive, which is fine).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

REGION="${AWS_REGION:-us-west-2}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "▶ Logging in to ECR ${REGISTRY}"
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

# service name → ECR repo name (matches local.services in main.tf)
declare -a SERVICES=(
  "order-service"
  "inventory-service"
  "payment-service"
)

# Detect host platform so we can warn on Apple Silicon (ECS Fargate
# defaults to linux/amd64; building on arm64 without --platform produces
# an unrunnable image).
HOST_ARCH=$(uname -m)
PLATFORM_ARG=""
if [[ "$HOST_ARCH" == "arm64" || "$HOST_ARCH" == "aarch64" ]]; then
  echo "▶ Detected $HOST_ARCH host — building linux/amd64 for Fargate"
  PLATFORM_ARG="--platform=linux/amd64"
fi

for svc in "${SERVICES[@]}"; do
  echo
  echo "▶ Building $svc"
  docker build $PLATFORM_ARG \
    -f "services/${svc}/Dockerfile" \
    -t "${svc}:${IMAGE_TAG}" \
    .

  echo "▶ Tagging $svc → ${REGISTRY}/${svc}:${IMAGE_TAG}"
  docker tag "${svc}:${IMAGE_TAG}" "${REGISTRY}/${svc}:${IMAGE_TAG}"

  echo "▶ Pushing $svc"
  docker push "${REGISTRY}/${svc}:${IMAGE_TAG}"
done

echo
echo "✓ All images pushed to ${REGISTRY}"
echo
echo "Next: bash deploy/scripts/deploy.sh"
