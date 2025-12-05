# Team Development Guide

## 🚀 Getting Started
1. Clone: \`git clone https://github.com/mazenchou/final-prj-dbt.git\`
2. Install: \`pip install dbt-bigquery\`
3. Copy: \`cp profiles.example.yml profiles.local.yml\`
4. Configure your local credentials in profiles.local.yml

## 🔀 Git Workflow
We use three main branches:
- \`main\` - Production code
- \`staging\` - Pre-production testing
- \`dev\` - Development integration

### Feature Development:
\`\`\`bash
git checkout dev
git pull origin dev
git checkout -b feature/your-feature-name
# ... make changes ...
git add .
git commit -m 'Description'
git push origin feature/your-feature-name
# Create Pull Request to 'dev'
\`\`\`

## ✅ Pull Request Requirements
- All SQL compiles without errors
- Tests pass locally
- Code reviewed by at least one team member
- Documentation updated if needed

## 🏗️ Project Structure
\`\`\`
models/
├── staging/     # Raw data transformations
├── core/        # Business dimension tables
└── marts/       # Analytics views
seeds/           # CSV data files
tests/           # Data quality tests
\`\`\`