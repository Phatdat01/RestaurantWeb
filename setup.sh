#!/bin/bash

function install_dependencies {
if dpkg -s build-essential autoconf libtool pkg-config libsasl2-dev python-dev-is-python3 > /dev/null 2>&1; then
    echo "All necessary packages are installed"
else
    echo "One or more necessary packages are not installed"
    sudo apt-get update
    sudo apt-get install -y build-essential autoconf libtool pkg-config libsasl2-dev python-dev-is-python3
fi
}

function install_python3_venv {
  if dpkg -l | grep -q "python3-venv"; then
    echo "python3-venv package is installed"
  else
    echo "python3-venv package is not installed"
    sudo apt-get update
    sudo apt-get install -y python3-venv
  fi
}

function create_virtual_env {
  python3 -m venv .env
  source .env/bin/activate
}

function install_requirements {
  pip install -r requirements.txt
}

# function update_database {
#   python manage.py migrate
# }

# Main
install_dependencies
install_python3_venv
create_virtual_env
install_requirements

