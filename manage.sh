#!/bin/bash

# MongoDB Extra - Management Script
# Handles setup, examples, development, and publishing

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ️${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠️${NC} $1"
}

print_error() {
    echo -e "${RED}❌${NC} $1"
}

print_header() {
    echo -e "\n${BLUE}$1${NC}"
    echo "============================================="
}

# Function to check Node.js requirements
check_nodejs() {
    if ! command -v node &> /dev/null; then
        print_error "Node.js is not installed. Please install Node.js 14+ first."
        echo "   Visit: https://nodejs.org/"
        exit 1
    fi

    NODE_VERSION=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
    if [ "$NODE_VERSION" -lt 14 ]; then
        print_error "Node.js version 14+ is required. Current version: $(node -v)"
        exit 1
    fi

    print_status "Node.js $(node -v) detected"

    if ! command -v npm &> /dev/null; then
        print_error "npm is not installed. Please install npm first."
        exit 1
    fi

    print_status "npm $(npm -v) detected"
}

# Setup function
setup() {
    print_header "🚀 Setting up MongoDB Extra..."
    
    check_nodejs
    
    print_info "📦 Installing dependencies..."
    npm install
    
    # Make the CLI executable
    chmod +x bin/cli.js
    
    print_status "Setup completed successfully!"
    echo ""
    print_info "📖 Usage examples:"
    echo "  npx @rightson/mongo-backup -d myDatabase -c myCollection"
    echo "  npx @rightson/mongo-backup --help"
    echo ""
    print_info "🔗 For detailed documentation, see README.md"
    
    # Test the installation
    print_info "🧪 Testing installation..."
    if node bin/cli.js --help > /dev/null 2>&1; then
        print_status "Installation test passed!"
    else
        print_error "Installation test failed. Please check for errors above."
        exit 1
    fi
    
    print_status "✨ Ready to use! Run 'npx @rightson/mongo-backup --help' for options."
}

# Development setup function
dev_setup() {
    print_header "🔧 Setting up development environment..."
    
    check_nodejs
    
    print_info "📦 Installing dependencies..."
    npm install
    
    print_info "🔗 Linking package locally..."
    npm link
    
    print_status "Development setup completed!"
    print_info "You can now run 'mongo-backup' locally for testing"
}

# Examples function
show_examples() {
    print_header "🗂️ MongoDB Extra - Example Commands"
    echo ""

    echo "📋 Basic Examples:"
    echo ""

    echo "1️⃣  Local MongoDB (no auth):"
    echo "   npx @rightson/mongo-backup -d myapp -c users"
    echo ""

    echo "2️⃣  With authentication (prompts for password):"
    echo "   npx @rightson/mongo-backup --uri mongodb://admin@db.example.com/production -c orders"
    echo ""

    echo "3️⃣  MongoDB Atlas:"
    echo "   npx @rightson/mongo-backup --uri 'mongodb+srv://user:pass@cluster.mongodb.net/production' -c orders"
    echo ""

    echo "4️⃣  Custom date field and compression:"
    echo "   npx @rightson/mongo-backup -d analytics -c events -f timestamp -z"
    echo ""

    echo "5️⃣  Large collection with smaller batches:"
    echo "   npx @rightson/mongo-backup -d bigdata -c transactions -b 1000"
    echo ""

    echo "⚡ Performance-Optimized Examples:"
    echo ""

    echo "6️⃣  High-performance dump (large batches):"
    echo "   npx @rightson/mongo-backup -d warehouse -c products -b 50000 -z"
    echo ""

    echo "7️⃣  Memory-conscious dump (small batches):"
    echo "   npx @rightson/mongo-backup -d logs -c detailed_logs -b 500"
    echo ""

    echo "🔄 Resume Example:"
    echo ""
    echo "If interrupted, just run the same command again - it will resume automatically!"
    echo ""

    echo "🛠️  Utility Commands:"
    echo ""
    echo "📖 Show help:"
    echo "   npx @rightson/mongo-backup --help"
    echo ""
    echo "🧹 Force restart (delete state file):"
    echo "   rm ./dump/.dump-state.json"
    echo ""

    echo "🎯 Pro Tips:"
    echo "   • Add index on your date field: db.collection.createIndex({dateField: 1})"
    echo "   • Use compression (-z) for large datasets"
    echo "   • Adjust batch size based on document size"
    echo "   • Run close to your MongoDB server for speed"
    echo "   • For security, omit password to be prompted rather than using -p"
    echo ""

    # Ask if user wants to create a sample script
    echo ""
    read -p "📝 Create a sample run script (run-example.sh)? (y/n): " -r
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        create_sample_script
    fi
}

# Create sample script function
create_sample_script() {
    cat > run-example.sh << 'EOF'
#!/bin/bash

# Sample MongoDB Extra Script
# Edit the variables below to match your setup

# MongoDB Configuration
MONGODB_URI="mongodb://localhost:27017"  # Or use MongoDB Atlas URI
# MONGODB_URI="mongodb+srv://user:pass@cluster.mongodb.net/"

# Database Configuration
DATABASE="myapp"
COLLECTION="users"
DATE_FIELD="createdAt"

# Output Configuration
OUTPUT_DIR="./monthly-dumps"
BATCH_SIZE="10000"
COMPRESS="--compress"

# Build and execute the command
echo "🚀 Running MongoDB Extra..."
npx @rightson/mongo-backup \
  --uri "$MONGODB_URI" \
  --database "$DATABASE" \
  --collection "$COLLECTION" \
  --date-field "$DATE_FIELD" \
  --output-dir "$OUTPUT_DIR" \
  --batch-size "$BATCH_SIZE" \
  $COMPRESS

echo "✅ Dump completed! Check $OUTPUT_DIR for your files."
EOF

    chmod +x run-example.sh
    print_status "Created 'run-example.sh' - edit and run it for your setup!"
}

# Test function
test_package() {
    print_header "🧪 Testing Package"
    
    print_info "Testing help command..."
    if node bin/cli.js --help > /dev/null 2>&1; then
        print_status "CLI test passed!"
    else
        print_error "CLI test failed!"
        exit 1
    fi
    
    if command -v mongo-backup &> /dev/null; then
        print_info "Testing linked command..."
        if mongo-backup --help > /dev/null 2>&1; then
            print_status "Linked command test passed!"
        else
            print_warning "Linked command test failed (run 'npm link' first)"
        fi
    else
        print_warning "Package not linked locally (run './manage.sh dev' to link)"
    fi
    
    print_status "Package tests completed!"
}

# Build/lint function
build() {
    print_header "🔨 Building Package"
    
    print_info "Running package validation..."
    npm pack --dry-run
    
    print_status "Build completed!"
}

# Publish functions
check_npm_auth() {
    if ! npm whoami &> /dev/null; then
        print_error "Not logged in to npm. Please run 'npm login' first."
        return 1
    fi
    
    NPM_USER=$(npm whoami)
    print_status "Logged in as: $NPM_USER"
    return 0
}

publish_patch() {
    print_header "📦 Publishing Patch Version"
    
    if ! check_npm_auth; then
        exit 1
    fi
    
    print_info "Bumping patch version..."
    npm version patch
    
    print_info "Publishing to npm..."
    npm publish --access public
    
    print_status "Patch version published successfully!"
    print_info "Users can now run: npx @rightson/mongo-backup"
}

publish_minor() {
    print_header "📦 Publishing Minor Version"
    
    if ! check_npm_auth; then
        exit 1
    fi
    
    print_info "Bumping minor version..."
    npm version minor
    
    print_info "Publishing to npm..."
    npm publish --access public
    
    print_status "Minor version published successfully!"
    print_info "Users can now run: npx @rightson/mongo-backup"
}

publish_major() {
    print_header "📦 Publishing Major Version"
    
    if ! check_npm_auth; then
        exit 1
    fi
    
    print_warning "This will create a major version bump!"
    read -p "Are you sure? (y/N): " -r
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi
    
    print_info "Bumping major version..."
    npm version major
    
    print_info "Publishing to npm..."
    npm publish --access public
    
    print_status "Major version published successfully!"
    print_info "Users can now run: npx @rightson/mongo-backup"
}

# Git helpers
git_status() {
    print_header "📊 Git Status"
    git status
}

git_commit() {
    print_header "💾 Git Commit"
    
    if [ -z "$1" ]; then
        print_error "Commit message required. Usage: ./manage.sh commit 'message'"
        exit 1
    fi
    
    git add .
    git commit -m "$1"
    print_status "Changes committed!"
}

git_push() {
    print_header "🚀 Git Push"
    git push origin main
    print_status "Changes pushed to GitHub!"
}

# Help function
show_help() {
    echo "MongoDB Extra - Management Script"
    echo ""
    echo "Usage: ./manage.sh <command>"
    echo ""
    echo "Setup Commands:"
    echo "  setup          - Install dependencies and prepare for use"
    echo "  dev            - Setup development environment with npm link"
    echo ""
    echo "Information Commands:"
    echo "  examples       - Show usage examples"
    echo "  help           - Show this help message"
    echo ""
    echo "Development Commands:"
    echo "  test           - Run package tests"
    echo "  build          - Validate and build package"
    echo ""
    echo "Publishing Commands:"
    echo "  publish-patch  - Publish patch version (1.0.0 -> 1.0.1)"
    echo "  publish-minor  - Publish minor version (1.0.0 -> 1.1.0)"
    echo "  publish-major  - Publish major version (1.0.0 -> 2.0.0)"
    echo ""
    echo "Git Commands:"
    echo "  status         - Show git status"
    echo "  commit <msg>   - Add and commit changes"
    echo "  push           - Push changes to GitHub"
    echo ""
    echo "Example Usage:"
    echo "  ./manage.sh setup"
    echo "  ./manage.sh examples"
    echo "  ./manage.sh publish-patch"
    echo "  ./manage.sh commit 'Add new feature'"
    echo ""
}

# Main script logic
case "${1:-help}" in
    "setup")
        setup
        ;;
    "dev")
        dev_setup
        ;;
    "examples")
        show_examples
        ;;
    "test")
        test_package
        ;;
    "build")
        build
        ;;
    "publish-patch")
        publish_patch
        ;;
    "publish-minor")
        publish_minor
        ;;
    "publish-major")
        publish_major
        ;;
    "status")
        git_status
        ;;
    "commit")
        git_commit "$2"
        ;;
    "push")
        git_push
        ;;
    "help"|*)
        show_help
        ;;
esac