#!/bin/bash

set -x

source "$PROJECT_ROOT/sonarqube/import.sh"

function generate_env_props() {
  local build_dir="$PROJECT_ROOT/build"
  # Make the build dir if not already
  mkdir -p "$build_dir" || true

  local python_env_properties="$build_dir/.env_python_properties"
  local python_env_path="$build_dir/python_env"

  echo "PATH=$PYTHON_BINARY_HOME/bin:$PATH" > "$python_env_properties"

  export PATH=$PYTHON_BINARY_HOME/bin:$PATH
  echo "export PATH=$PYTHON_BINARY_HOME/bin:\$PATH" > "$python_env_path"
}

function download_and_extract_python() {
  local python_binary
  python_binary="$(import "$PYTHON_BINARY_VERSION")"
  unzip -q "$python_binary" -d "$HOME"
}

OS="buster"
export PYTHON_SDK_NUMBER="3.9.2"
PYTHON_BINARY_VERSION="mvn:com.uber.devxp:python-binary-${OS}:zip:${PYTHON_SDK_NUMBER}"
PYTHON_MAJOR_VERSION=$(echo "$PYTHON_SDK_NUMBER" | cut -d'.' -f 1)
PYTHON_BINARY_HOME="$HOME/python_home/$PYTHON_SDK_NUMBER"

# Only download if we already don't have the python binary installed
if [[ ! -f "$PYTHON_BINARY_HOME/bin/python${PYTHON_MAJOR_VERSION}" ]]; then
  download_and_extract_python
else
  echo "Python-$PYTHON_SDK_NUMBER already installed in $PYTHON_BINARY_HOME. Using this version instead."
fi

generate_env_props
