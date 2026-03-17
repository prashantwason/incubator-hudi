#! /bin/bash

importSonarW() {
  PROJECT_ROOT=${PROJECT_ROOT:-$(git rev-parse --show-toplevel)}

  source "${PROJECT_ROOT}/sonarqube/import.sh"

  # Export sonar scanner maven artifact name for both linux and macos
  SONAR_SCANNER_VERSION="7.0.1.4817"
  export SONAR_SCANNER_LINUX="mvn:com.uber.sonarqube:sonar-scanner-linux:zip:${SONAR_SCANNER_VERSION}"
  export SONAR_SCANNER_MACOS="mvn:com.uber.sonarqube:sonar-scanner-macos:zip:${SONAR_SCANNER_VERSION}"

  # Download sonar-wrapper-pex for given version and export the executable sonar-wrapper-pex
  SONARW_PEX_VERSION="3.0.0-test16"
  SONARW_ZIP_URL="mvn:com.uber.sonarqube:sonar-wrapper:zip:${SONARW_PEX_VERSION}"
  SONAR_WRAPPER_ZIP="$(import "$SONARW_ZIP_URL")"
  SONAR_WRAPPER_ZIP_DIR=$(dirname "$SONAR_WRAPPER_ZIP")

  unzip -o -d "$SONAR_WRAPPER_ZIP_DIR" "$SONAR_WRAPPER_ZIP"

  SONAR_WRAPPER_PEX="$(find "$SONAR_WRAPPER_ZIP_DIR/" -type f \( -name 'sonar-wrapper' -o -name 'sonar-wrapper.pex' \))"
  export SONAR_WRAPPER_PEX
  if [[ ! -x "$SONAR_WRAPPER_PEX" ]]; then
    chmod +x "$SONAR_WRAPPER_PEX"
  fi
}
