# Installing Python 3.12 for Better Compatibility

If you're encountering issues with `psycopg2-binary` on Python 3.13, using Python 3.12 is recommended.

## Option 1: Using Homebrew (macOS)

```bash
# Install Python 3.12
brew install python@3.12

# Verify installation
python3.12 --version

# Use Python 3.12 for the ETL service
python3.12 run.sh
```

## Option 2: Using pyenv (Recommended for managing multiple Python versions)

```bash
# Install pyenv if not already installed
brew install pyenv

# Add to your shell profile (~/.zshrc or ~/.bash_profile)
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.zshrc

# Reload shell
source ~/.zshrc

# Install Python 3.12
pyenv install 3.12.7

# Set as local version for this project
cd apps/etl
pyenv local 3.12.7

# Verify
python --version  # Should show 3.12.7

# Now run.sh will use Python 3.12
./run.sh
```

## Option 3: Create a Virtual Environment with Python 3.12

```bash
# If you have Python 3.12 installed
python3.12 -m venv venv312
source venv312/bin/activate

# Install dependencies
uv pip install -r requirements.txt

# Install Singer taps
uv pip install -e connectors/tap-postgres
uv pip install -e connectors/tap-mysql
uv pip install -e connectors/tap-mongodb

# Run the service
python main.py
```

## After Installing Python 3.12

Update the `run.sh` script to use Python 3.12 by default, or create an alias:

```bash
# Add to ~/.zshrc or ~/.bash_profile
alias python-etl='cd /path/to/apps/etl && python3.12 run.sh'
```
