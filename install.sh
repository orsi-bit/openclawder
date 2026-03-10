#!/bin/sh
set -e

# openclawder installer script
# Usage: curl -sSL https://raw.githubusercontent.com/orsi-bit/openclawderr/main/install.sh | sh

REPO="orsi-bit/openclawderr"
INSTALL_DIR="${OPENCLAWDER_INSTALL_DIR:-$HOME/.local/bin}"

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
        *) echo "unsupported" ;;
    esac
}

# Detect architecture
detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64) echo "amd64" ;;
        arm64|aarch64) echo "arm64" ;;
        *) echo "unsupported" ;;
    esac
}

# Get latest release tag via redirect (avoids JSON parsing issues under set -e)
get_latest_version() {
    # Follow the /releases/latest redirect and extract the tag from the final URL
    LOCATION=$(curl -sSLI "https://github.com/${REPO}/releases/latest" 2>/dev/null | grep -i "^location:" | tail -1) || true
    if [ -n "$LOCATION" ]; then
        echo "$LOCATION" | sed -E 's|.*releases/tag/([^ \r\n]+).*|\1|' | tr -d '[:space:]'
        return
    fi
    # Fallback: JSON API
    curl -sSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null \
        | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | tr -d '[:space:]' || true
}

# Add install dir to shell profile if not already in PATH
add_to_path() {
    PROFILE=""
    if [ -n "$ZSH_VERSION" ] || [ "$(basename "$SHELL")" = "zsh" ]; then
        PROFILE="$HOME/.zshrc"
    elif [ -n "$BASH_VERSION" ] || [ "$(basename "$SHELL")" = "bash" ]; then
        if [ -f "$HOME/.bash_profile" ]; then
            PROFILE="$HOME/.bash_profile"
        else
            PROFILE="$HOME/.bashrc"
        fi
    else
        PROFILE="$HOME/.profile"
    fi

    LINE="export PATH=\"\$PATH:$INSTALL_DIR\""
    if [ -f "$PROFILE" ] && grep -qF "$INSTALL_DIR" "$PROFILE" 2>/dev/null; then
        return  # already there
    fi

    printf '\n# openclawder\n%s\n' "$LINE" >> "$PROFILE"
    echo "Added openclawder to PATH in $PROFILE"
    echo "Restart your shell or run: source $PROFILE"
}

main() {
    echo "Installing openclawder..."

    OS=$(detect_os)
    ARCH=$(detect_arch)

    if [ "$OS" = "unsupported" ] || [ "$ARCH" = "unsupported" ]; then
        echo "Error: Unsupported OS or architecture: $(uname -s) $(uname -m)"
        echo "Please install manually from https://github.com/${REPO}/releases"
        exit 1
    fi

    VERSION=$(get_latest_version)
    if [ -z "$VERSION" ]; then
        echo "Error: Could not determine latest version"
        echo "Check https://github.com/${REPO}/releases and try:"
        echo "  OPENCLAWDER_VERSION=v0.1.0 sh install.sh"
        exit 1
    fi

    # Allow version override
    VERSION="${OPENCLAWDER_VERSION:-$VERSION}"

    echo "  OS: $OS"
    echo "  Arch: $ARCH"
    echo "  Version: $VERSION"

    # Build download URL
    BINARY="openclawder-${OS}-${ARCH}"
    if [ "$OS" = "windows" ]; then
        BINARY="${BINARY}.exe"
    fi
    URL="https://github.com/${REPO}/releases/download/${VERSION}/${BINARY}"

    echo "  Downloading from: $URL"

    # Create install directory if it doesn't exist
    mkdir -p "$INSTALL_DIR"

    # Download binary
    DEST="${INSTALL_DIR}/openclawder"
    if [ "$OS" = "windows" ]; then
        DEST="${DEST}.exe"
    fi

    if command -v curl >/dev/null 2>&1; then
        curl -fSL "$URL" -o "$DEST" || { echo "Error: Download failed. Check https://github.com/${REPO}/releases"; exit 1; }
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$URL" -O "$DEST" || { echo "Error: Download failed. Check https://github.com/${REPO}/releases"; exit 1; }
    else
        echo "Error: curl or wget required"
        exit 1
    fi

    # Make executable
    chmod +x "$DEST"

    echo ""
    echo "Installed openclawder to $DEST"

    # Auto-add to PATH if not already there
    case ":$PATH:" in
        *":$INSTALL_DIR:"*)
            echo "openclawder is already in your PATH."
            ;;
        *)
            add_to_path
            ;;
    esac

    echo ""
    echo "Run 'openclawder setup' to configure your AI coding tool."
}

main
