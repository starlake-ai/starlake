#!/bin/bash
set -e

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
    read -p "Where do you want to install Starlake? [$HOME/starlake]: " INSTALL_DIR
    INSTALL_DIR=${INSTALL_DIR:-$HOME/starlake}
    mkdir -p "$INSTALL_DIR"
}

get_from_url() {
    local url=$1
    local response=$(curl -s -w "%{http_code}" "$url")
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

    ALL_SNAPSHOT_VERSIONS=$(get_from_url https://s01.oss.sonatype.org/service/local/repositories/snapshots/content/ai/starlake/starlake-spark3_2.12/ | awk -F'<|>' '/<text>/{print $3}' | grep -oE '^[0-9]+\.[0-9]+\.[0-9]+-SNAPSHOT$' | sort -rV)
    ALL_RELEASE_VERSIONS=$(get_from_url https://s01.oss.sonatype.org/service/local/repositories/releases/content/ai/starlake/starlake-spark3_2.12/ | awk -F'<|>' '/<text>/{print $3}' | grep -oE '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -rV)

    SNAPSHOT_VERSION=$(echo "$ALL_SNAPSHOT_VERSIONS" | head -n 1)
    LATEST_RELEASE_VERSIONS=$(echo "$ALL_RELEASE_VERSIONS" | head -n 5)

    VERSIONS=("$SNAPSHOT_VERSION" $LATEST_RELEASE_VERSIONS)
    VERSIONS=$VERSIONS

     while [[ ! "$VERSIONS" =~ (^|[[:space:]])"$VERSION"($|[[:space:]]) ]]; do
        echo "Invalid version $VERSION. Please choose from the available versions."
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
                echo -e "\nexport PATH=$INSTALL_DIR:\$PATH" >> ~/.zshrc
            fi
            zsh ~/.zshrc
        fi
        if [[ "$SHELL" == *bash* ]]; then
            if ! grep -q "$INSTALL_DIR" ~/.bashrc; then
                echo -e "\nexport PATH=$INSTALL_DIR:\$PATH" >> ~/.bashrc
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
}

print_success_message() {
    echo "Starlake has been successfully installed!"
}

main() {
    print_starlake_ascii_art
    get_installation_directory
    get_version_to_install "$@"
    install_starlake
    add_starlake_to_path
    run_installation_command
    print_success_message
}

# Run the main function
main "$@"
