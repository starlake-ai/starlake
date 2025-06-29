#!/bin/bash
set -e

if [ "$EUID" -eq 0 ]
  then echo "Please do not run this script as root or with sudo."
  exit
fi

print_starlake_ascii_art() {
    cat <<EOF
   _____ _______       _____  _               _  ________
  / ____|__   __|/\   |  __ \| |        /\   | |/ /  ____|
 | (___    | |  /  \  | |__) | |       /  \  | ' /| |__
  \___ \   | | / /\ \ |  _  /| |      / /\ \ |  < |  __|
  ____) |  | |/ ____ \| | \ \| |____ / ____ \| . \| |____
 |_____/   |_/_/    \_\_|  \_\______/_/    \_\_|\_\______|


EOF
}

get_installation_directory() {
    # Extract the version number from command-line arguments
    for arg in "$@"; do
        if [[ $arg == "--target="* ]]; then
            INSTALL_DIR="${arg#*=}"
        fi
    done
    if [[ -z "$INSTALL_DIR" ]]
    then
      read -p "Where do you want to install Starlake? [$HOME/starlake]: " INSTALL_DIR
      INSTALL_DIR=${INSTALL_DIR:-$HOME/starlake}
    fi
    INSTALL_DIR=$(eval "echo $INSTALL_DIR")
    mkdir -p "$INSTALL_DIR"
}

if [[ -n "${https_proxy}" ]] || [[ -n "${http_proxy}" ]]; then
  PROXY=${https_proxy:-$http_proxy}
fi

get_from_url() {
    local url=$1
    if [ -n "$PROXY" ] && [ -n "$SL_INSECURE" ]; then
        echo "Downloading data from $url using proxy $PROXY"
        local response=$(curl --insecure --proxy "$PROXY" -s -w "%{http_code}" "$url")
    else
        local response=$(curl -s -w "%{http_code}" "$url")
    fi
    local status_code=${response: -3}

    if [[ ! $status_code =~ ^(2|3)[0-9][0-9]$ ]]; then
        echo "Error: Failed to retrieve data from $url. HTTP status code: $status_code"
        exit 1
    fi

    # Print the content excluding the status code
    local content_length=${#response}
    local content="${response:0:content_length-3}"
    echo "$content"
}


get_version_to_install() {
    # Extract the version number from command-line arguments
    for arg in "$@"; do
        if [[ $arg == "--version="* ]]; then
            VERSION="${arg#*=}"
        fi
    done

    ALL_SNAPSHOT_VERSIONS=$(get_from_url https://s01.oss.sonatype.org/service/local/repositories/snapshots/content/ai/starlake/starlake-core_2.13/ | awk -F'<|>' '/<text>/{print $3}' | grep -oE '^[0-9]+\.[0-9]+\.[0-9]+-SNAPSHOT$' | sort -rV)
    ALL_RELEASE_NEW_PATTERN_VERSIONS=$(get_from_url https://s01.oss.sonatype.org/service/local/repositories/releases/content/ai/starlake/starlake-core_2.13/ | awk -F'<|>' '/<text>/{print $3}' | grep -oE '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -rV)
    ALL_RELEASE_OLD_PATTERN_VERSIONS=$(get_from_url https://s01.oss.sonatype.org/service/local/repositories/releases/content/ai/starlake/starlake-spark3_2.13/ | awk -F'<|>' '/<text>/{print $3}' | grep -oE '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -rV)
    ALL_RELEASE_VERSIONS=$(echo "$ALL_RELEASE_NEW_PATTERN_VERSIONS $ALL_RELEASE_OLD_PATTERN_VERSIONS")

    SNAPSHOT_VERSION=$(echo "$ALL_SNAPSHOT_VERSIONS" | head -n 1)
    LATEST_RELEASE_VERSIONS=$(echo "$ALL_RELEASE_VERSIONS" | head -n 5)

    VERSIONS=("$SNAPSHOT_VERSION" $LATEST_RELEASE_VERSIONS)
    VERSIONS=$VERSIONS

    while [[ ! "${VERSIONS[*]}" =~ (^|[[:space:]])"$VERSION"($|[[:space:]]) ]]; do
      if [[ -n "$VERSION" ]]
      then
        echo "Invalid version $VERSION. Please choose from the available versions."
      fi
      echo "Last 5 available versions:"
      for version in "${VERSIONS[@]}"; do
          echo "$version"
      done
      read -p "Which version do you want to install? [$(echo "$VERSIONS" | head -n 1)]: " VERSION
      VERSION=${VERSION:-$(echo "$VERSIONS" | head -n 1)}
    done

}

install_starlake() {
    echo "installing $VERSION"
    if [[ $VERSION == *"SNAPSHOT"* ]]; then
        local url=https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/starlake.sh
    else
        local url=https://raw.githubusercontent.com/starlake-ai/starlake/v$VERSION/distrib/starlake.sh
    fi
    get_from_url $url > "$INSTALL_DIR/starlake"
    chmod +x "$INSTALL_DIR/starlake"
}


add_starlake_to_path() {
    if [[ "$SHELL" == *zsh* ]] || [[ "$SHELL" == *bash* ]]; then
        if [[ "$SHELL" == *zsh* ]]; then
            if ! grep -q "$INSTALL_DIR" ~/.zshrc; then
                echo  >> ~/.zshrc
                if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
                    echo "export PATH=$INSTALL_DIR:\$PATH" >> ~/.zshrc
                fi
            fi
            zsh ~/.zshrc
        fi
        if [[ "$SHELL" == *bash* ]]; then
            if ! grep -q "$INSTALL_DIR" ~/.bashrc; then
                 echo  >> ~/.bashrc
                if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
                    echo "export PATH=$INSTALL_DIR:\$PATH" >> ~/.bashrc
                fi
            fi
            source ~/.bashrc
        fi
        echo "Starlake has been added to your PATH."
    else
        echo "Could not detect what shell you're using. Please add the following line to your shell configuration file manually:"
        echo "export PATH=$INSTALL_DIR:\$PATH"
    fi
}

run_installation_command() {
    SL_VERSION=$VERSION "$INSTALL_DIR/starlake" install
    #rm "$INSTALL_DIR/setup.jar"
}

print_success_message() {
    echo "Starlake has been successfully installed!"
}

check_java_version() {
# Find the java binary
    if [ -n "${JAVA_HOME}" ]; then
      RUNNER="${JAVA_HOME}/bin/java"
    else
      if [ "$(command -v java)" ]; then
        RUNNER="java"
      else
        echo "JAVA_HOME is not set" >&2
        exit 1
      fi
    fi
    local version=$($RUNNER -version 2>&1 | awk -F '"' '/version/ {print $2}')
    local major=$(echo "$version" | awk -F '.' '{print $1}')
    local minor=$(echo "$version" | awk -F '.' '{print $2}')

    if [[ "$major" -lt 11 ]]; then
        echo "Error: Java 11 or later is required."
        exit 1
    fi
    echo "Java version $version is detected."
}

main() {
    check_java_version
    print_starlake_ascii_art
    get_installation_directory "$@"
    get_version_to_install "$@"
    install_starlake
    add_starlake_to_path
    run_installation_command
    print_success_message
}

# Run the main function
main "$@"
