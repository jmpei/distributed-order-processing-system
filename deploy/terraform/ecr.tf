resource "aws_ecr_repository" "this" {
  for_each = local.services

  name                 = each.value.ecr_repo_name
  image_tag_mutability = "MUTABLE" # dev: allow re-pushing :latest

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = { Service = each.key }
}

# Retain the most recent 5 images per repo to keep storage cost trivial.
resource "aws_ecr_lifecycle_policy" "this" {
  for_each = aws_ecr_repository.this

  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}
