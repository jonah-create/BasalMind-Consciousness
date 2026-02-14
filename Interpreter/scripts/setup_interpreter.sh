#!/bin/bash
# Interpreter Setup Script

set -e  # Exit on error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
INTERPRETER_DIR="$(dirname "$SCRIPT_DIR")"

echo "🔧 Setting up BasalMind Interpreter..."
echo "   Directory: $INTERPRETER_DIR"

# Check Python 3.10+
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "   Python version: $PYTHON_VERSION"

# Create virtual environment
if [ ! -d "$INTERPRETER_DIR/venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv "$INTERPRETER_DIR/venv"
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

# Activate venv
source "$INTERPRETER_DIR/venv/bin/activate"

# Upgrade pip
echo "📦 Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1

# Install requirements
echo "📦 Installing dependencies..."
pip install -r "$INTERPRETER_DIR/requirements.txt"

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Configure .env file with your credentials:"
echo "     - OPENAI_API_KEY (required for embeddings)"
echo "     - Database passwords (if not using defaults)"
echo ""
echo "  2. Run the Interpreter:"
echo "     cd $INTERPRETER_DIR"
echo "     ./scripts/run_interpreter.sh"
echo ""

deactivate
