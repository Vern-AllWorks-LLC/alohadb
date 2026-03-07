#!/usr/bin/env bash
# ============================================================================
# AlohaDB 20.0 Installer
#
# Interactive installer that:
#   - Checks CPU, RAM, disk space prerequisites
#   - Installs missing build/runtime dependencies
#   - Builds from source with meson/ninja
#   - Creates the alohaadm system user
#   - Initializes the data directory
#   - Configures local or remote-managed mode
#   - Starts the server
#
# Usage: sudo bash install.sh
# ============================================================================

set -euo pipefail

# ---- Constants -------------------------------------------------------------
ALOHADB_VERSION="20.0"
MIN_CPUS=2
MIN_RAM_MB=1024
MIN_DISK_MB=2048
DEFAULT_PREFIX="/var/lib/alohadb"
DEFAULT_PORT=5433
DB_USER="alohaadm"
SHARED_PRELOAD="alohadb_audit,alohadb_cache,alohadb_cron,alohadb_pool,alohadb_query_store,alohadb_ratelimit,alohadb_scale"

# ---- Colors ----------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ---- Helpers ---------------------------------------------------------------
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }

prompt() {
    local var="$1" msg="$2" default="$3"
    local input
    if [[ -n "$default" ]]; then
        read -rp "$(echo -e "${BOLD}$msg${NC} [$default]: ")" input
        eval "$var=\"${input:-$default}\""
    else
        read -rp "$(echo -e "${BOLD}$msg${NC}: ")" input
        eval "$var=\"$input\""
    fi
}

prompt_yn() {
    local var="$1" msg="$2" default="$3"
    local input
    read -rp "$(echo -e "${BOLD}$msg${NC} [$default]: ")" input
    input="${input:-$default}"
    case "${input,,}" in
        y|yes) eval "$var=yes" ;;
        *)     eval "$var=no"  ;;
    esac
}

prompt_password() {
    local var="$1" msg="$2"
    local pw1 pw2
    while true; do
        read -srp "$(echo -e "${BOLD}$msg${NC}: ")" pw1; echo
        read -srp "$(echo -e "${BOLD}Confirm${NC}: ")" pw2; echo
        if [[ "$pw1" == "$pw2" ]]; then
            if [[ ${#pw1} -lt 8 ]]; then
                warn "Password must be at least 8 characters."
                continue
            fi
            if ! echo "$pw1" | grep -qP '[A-Z]'; then
                warn "Password must contain at least 1 uppercase letter."
                continue
            fi
            if ! echo "$pw1" | grep -qP '[a-z]'; then
                warn "Password must contain at least 1 lowercase letter."
                continue
            fi
            if ! echo "$pw1" | grep -qP '[0-9]'; then
                warn "Password must contain at least 1 digit."
                continue
            fi
            if ! echo "$pw1" | grep -qP '[!@%$#_\-]'; then
                warn "Password must contain at least 1 special character from: ! @ % \$ # _ -"
                continue
            fi
            eval "$var=\"$pw1\""
            return
        fi
        warn "Passwords do not match. Try again."
    done
}

# ---- Root check ------------------------------------------------------------
if [[ $EUID -ne 0 ]]; then
    fail "This installer must be run as root (use sudo)."
fi

# ---- Detect source directory -----------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ ! -f "$SCRIPT_DIR/meson.build" ]]; then
    fail "Cannot find meson.build in $SCRIPT_DIR. Run this script from the AlohaDB source directory."
fi

# ---- Banner ----------------------------------------------------------------
echo ""
echo -e "${BOLD}============================================${NC}"
echo -e "${BOLD}  AlohaDB ${ALOHADB_VERSION} Installer${NC}"
echo -e "${BOLD}============================================${NC}"
echo ""

# ============================================================================
# Phase 1: System Requirements
# ============================================================================
info "Checking system requirements..."

# -- OS check ----------------------------------------------------------------
if [[ "$(uname -s)" != "Linux" ]]; then
    fail "This installer supports Linux only. Detected: $(uname -s)"
fi

# -- Architecture check ------------------------------------------------------
ARCH="$(uname -m)"
if [[ "$ARCH" != "x86_64" && "$ARCH" != "aarch64" ]]; then
    fail "Unsupported architecture: $ARCH. AlohaDB requires x86_64 or aarch64."
fi
ok "Architecture: $ARCH"

# -- CPU check ---------------------------------------------------------------
CPU_COUNT=$(nproc 2>/dev/null || echo 1)
if [[ $CPU_COUNT -lt $MIN_CPUS ]]; then
    fail "Minimum $MIN_CPUS CPU cores required. Detected: $CPU_COUNT"
fi
CPU_MODEL=$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2 | xargs || echo "unknown")
ok "CPUs: $CPU_COUNT cores ($CPU_MODEL)"

# -- RAM check ---------------------------------------------------------------
TOTAL_RAM_KB=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo 0)
TOTAL_RAM_MB=$((TOTAL_RAM_KB / 1024))
if [[ $TOTAL_RAM_MB -lt $MIN_RAM_MB ]]; then
    fail "Minimum ${MIN_RAM_MB} MB RAM required. Detected: ${TOTAL_RAM_MB} MB"
fi
ok "RAM: ${TOTAL_RAM_MB} MB"

# ============================================================================
# Phase 2: Installation Directory
# ============================================================================
echo ""
prompt INSTALL_PREFIX "Installation directory" "$DEFAULT_PREFIX"

# -- Resolve the filesystem for the install directory ------------------------
INSTALL_PARENT="$(dirname "$INSTALL_PREFIX")"
mkdir -p "$INSTALL_PARENT" 2>/dev/null || true

# -- Disk space check --------------------------------------------------------
AVAIL_KB=$(df -k "$INSTALL_PARENT" 2>/dev/null | awk 'NR==2{print $4}')
AVAIL_MB=$((AVAIL_KB / 1024))
if [[ $AVAIL_MB -lt $MIN_DISK_MB ]]; then
    fail "Minimum ${MIN_DISK_MB} MB free disk space required on $(df "$INSTALL_PARENT" | awk 'NR==2{print $6}'). Available: ${AVAIL_MB} MB"
fi
ok "Disk: ${AVAIL_MB} MB available on $(df "$INSTALL_PARENT" | awk 'NR==2{print $6}')"

# ============================================================================
# Phase 3: Dependencies
# ============================================================================
echo ""
info "Checking build dependencies..."

# Detect package manager
if command -v apt-get &>/dev/null; then
    PKG_MGR="apt"
elif command -v dnf &>/dev/null; then
    PKG_MGR="dnf"
elif command -v yum &>/dev/null; then
    PKG_MGR="yum"
else
    fail "No supported package manager found (apt, dnf, yum)."
fi
ok "Package manager: $PKG_MGR"

# Define required packages per package manager
if [[ "$PKG_MGR" == "apt" ]]; then
    REQUIRED_PKGS=(
        build-essential
        meson
        ninja-build
        pkg-config
        libreadline-dev
        libssl-dev
        libzstd-dev
        liblz4-dev
        zlib1g-dev
        libicu-dev
        libcurl4-openssl-dev
        python3
        bison
        flex
    )
else
    # RHEL/Fedora/CentOS
    REQUIRED_PKGS=(
        gcc
        gcc-c++
        make
        meson
        ninja-build
        pkgconfig
        readline-devel
        openssl-devel
        libzstd-devel
        lz4-devel
        zlib-devel
        libicu-devel
        libcurl-devel
        python3
        bison
        flex
    )
fi

MISSING_PKGS=()

check_pkg_installed() {
    local pkg="$1"
    if [[ "$PKG_MGR" == "apt" ]]; then
        dpkg -l "$pkg" 2>/dev/null | grep -q "^ii" && return 0 || return 1
    else
        rpm -q "$pkg" &>/dev/null && return 0 || return 1
    fi
}

for pkg in "${REQUIRED_PKGS[@]}"; do
    if check_pkg_installed "$pkg"; then
        ok "  $pkg"
    else
        MISSING_PKGS+=("$pkg")
        warn "  $pkg -- MISSING"
    fi
done

if [[ ${#MISSING_PKGS[@]} -gt 0 ]]; then
    echo ""
    info "Installing ${#MISSING_PKGS[@]} missing package(s)..."
    if [[ "$PKG_MGR" == "apt" ]]; then
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${MISSING_PKGS[@]}"
    else
        "$PKG_MGR" install -y -q "${MISSING_PKGS[@]}"
    fi

    # Verify installation
    STILL_MISSING=()
    for pkg in "${MISSING_PKGS[@]}"; do
        if ! check_pkg_installed "$pkg"; then
            STILL_MISSING+=("$pkg")
        fi
    done
    if [[ ${#STILL_MISSING[@]} -gt 0 ]]; then
        fail "Failed to install: ${STILL_MISSING[*]}"
    fi
    ok "All dependencies installed."
else
    ok "All dependencies present."
fi

# ============================================================================
# Phase 4: Configuration Mode
# ============================================================================
echo ""
echo -e "${BOLD}Configuration Mode${NC}"
echo "  1) Local   - Standalone instance with local alohadb.conf"
echo "  2) Remote  - Managed by a central AlohaDB management server"
echo ""
prompt CONFIG_MODE "Select mode (1 or 2)" "1"

REMOTE_MODE="off"
REMOTE_HOST=""
REMOTE_PORT=""
REMOTE_PASSWORD_ENC=""

if [[ "$CONFIG_MODE" == "2" ]]; then
    REMOTE_MODE="on"
    echo ""
    info "Remote management server configuration:"
    prompt REMOTE_HOST "Management server hostname or IP" ""
    if [[ -z "$REMOTE_HOST" ]]; then
        fail "Management server hostname is required for remote mode."
    fi

    prompt REMOTE_PORT "Management server port" "5480"

    prompt_password REMOTE_PASSWORD_RAW "Management server password"

    # Encrypt the password using OpenSSL AES-256-GCM (same format as crypto.py)
    # Generate a 32-byte key and 12-byte nonce, encrypt, output as base64
    info "Encrypting management password..."
    REMOTE_PASSWORD_ENC=$(python3 -c "
import os, base64, hashlib, sys
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError:
    # Fallback: use openssl CLI
    sys.exit(99)
password = '''${REMOTE_PASSWORD_RAW}'''
# Derive a key from the password itself for transport encoding
# The management server will re-encrypt with its master key on registration
nonce = os.urandom(12)
# Use a well-known transport key (first registration handshake)
transport_key = hashlib.sha256(b'alohadb-transport-v1').digest()
aesgcm = AESGCM(transport_key)
ct = aesgcm.encrypt(nonce, password.encode('utf-8'), None)
print('AES256:' + base64.urlsafe_b64encode(nonce + ct).decode())
" 2>/dev/null) || true

    if [[ -z "$REMOTE_PASSWORD_ENC" ]]; then
        # Fallback: openssl-based encryption
        REMOTE_PASSWORD_ENC=$(echo -n "$REMOTE_PASSWORD_RAW" | openssl enc -aes-256-cbc -a -salt -pbkdf2 -pass pass:alohadb-transport-v1 2>/dev/null | tr -d '\n')
        REMOTE_PASSWORD_ENC="AES256:${REMOTE_PASSWORD_ENC}"
    fi

    ok "Remote mode configured: $REMOTE_HOST:$REMOTE_PORT"
fi

# ============================================================================
# Phase 5: Port Selection
# ============================================================================
echo ""
prompt LISTEN_PORT "AlohaDB listen port" "$DEFAULT_PORT"

# Check if port is already in use
if ss -tlnp 2>/dev/null | grep -q ":${LISTEN_PORT} "; then
    warn "Port $LISTEN_PORT is currently in use."
    prompt_yn PORT_CONTINUE "Continue anyway?" "n"
    if [[ "$PORT_CONTINUE" != "yes" ]]; then
        fail "Aborted. Choose a different port."
    fi
fi

# ============================================================================
# Phase 6: Build
# ============================================================================
echo ""
info "Building AlohaDB ${ALOHADB_VERSION} from source..."
info "Source directory: $SCRIPT_DIR"
info "Install prefix:  $INSTALL_PREFIX"
echo ""

BUILD_DIR="$SCRIPT_DIR/builddir"

# Clean previous build if exists
if [[ -d "$BUILD_DIR" ]]; then
    info "Removing previous build directory..."
    rm -rf "$BUILD_DIR"
fi

info "Configuring with meson..."
meson setup "$BUILD_DIR" "$SCRIPT_DIR" \
    --prefix="$INSTALL_PREFIX" \
    -Drpath=true \
    -Dssl=openssl \
    2>&1 | tail -5
echo ""

info "Compiling (using $CPU_COUNT cores)..."
ninja -C "$BUILD_DIR" -j"$CPU_COUNT" 2>&1 | tail -3
echo ""

info "Installing to $INSTALL_PREFIX..."
ninja -C "$BUILD_DIR" install 2>&1 | tail -3
ok "Build and install complete."

# ============================================================================
# Phase 7: System User
# ============================================================================
echo ""
if id "$DB_USER" &>/dev/null; then
    ok "System user '$DB_USER' already exists."
else
    info "Creating system user '$DB_USER'..."
    useradd -r -m -d "$INSTALL_PREFIX" -s /bin/bash "$DB_USER"
    ok "Created system user '$DB_USER'."
fi

chown -R "$DB_USER":"$DB_USER" "$INSTALL_PREFIX"

# ============================================================================
# Phase 8: Initialize Data Directory
# ============================================================================
DATA_DIR="$INSTALL_PREFIX/data"

if [[ -f "$DATA_DIR/PG_VERSION" ]] || [[ -f "$DATA_DIR/alohadb.pid" ]]; then
    warn "Data directory already initialized at $DATA_DIR. Skipping initdb."
else
    echo ""
    info "Initializing data directory..."
    sudo -u "$DB_USER" "$INSTALL_PREFIX/bin/alinitdb" -D "$DATA_DIR" 2>&1 | tail -5
    ok "Data directory initialized at $DATA_DIR"
fi

# ============================================================================
# Phase 9: Configure
# ============================================================================
echo ""
info "Writing configuration..."

CONF_FILE="$DATA_DIR/alohadb.conf"
HBA_FILE="$DATA_DIR/al_hba.conf"

# -- alohadb.conf ------------------------------------------------------------
# Append AlohaDB-specific settings (don't overwrite existing PG defaults)
cat >> "$CONF_FILE" <<ALOHACONF

# ---- AlohaDB ${ALOHADB_VERSION} Configuration (added by installer) ----
port = ${LISTEN_PORT}
listen_addresses = 'localhost'
shared_preload_libraries = '${SHARED_PRELOAD}'

# Audit logging
alohadb.audit_enabled = on
alohadb.audit_log_directory = '/var/log/alohadb/audit'
alohadb.audit_log_format = 'csv'
alohadb.audit_databases = '*'
alohadb.audit_operations = 'insert,update,delete,create_role,alter_role,drop_role,grant,alter_system'

# Cron
alohadb.cron_database_name = 'alohadb'
ALOHACONF

if [[ "$REMOTE_MODE" == "on" ]]; then
    cat >> "$CONF_FILE" <<REMOTECONF

# Remote management
alohadb.remote_mode = on
alohadb.remote_host = '${REMOTE_HOST}'
alohadb.remote_port = ${REMOTE_PORT}
alohadb.remote_password = '${REMOTE_PASSWORD_ENC}'
REMOTECONF
    ok "Remote management configured: ${REMOTE_HOST}:${REMOTE_PORT}"
else
    ok "Local configuration written."
fi

# -- al_hba.conf: secure defaults -------------------------------------------
# Only allow local connections by default (not trust from 0.0.0.0/0)
if ! grep -q "scram-sha-256" "$HBA_FILE" 2>/dev/null; then
    cat > "$HBA_FILE" <<HBACONF
# AlohaDB Host-Based Authentication (al_hba.conf)
# TYPE  DATABASE  USER  ADDRESS        METHOD

# Local connections (Unix socket)
local   all       all                  peer

# IPv4 local connections
host    all       all   127.0.0.1/32   scram-sha-256

# IPv6 local connections
host    all       all   ::1/128        scram-sha-256

# To allow remote connections, add lines like:
# host  all       all   10.0.0.0/8     scram-sha-256
HBACONF
    ok "Secure al_hba.conf written (scram-sha-256, localhost only)."
fi

# -- Create audit log directory ----------------------------------------------
mkdir -p /var/log/alohadb/audit
chown -R "$DB_USER":"$DB_USER" /var/log/alohadb
ok "Audit log directory created: /var/log/alohadb/audit"

# -- Fix final ownership -----------------------------------------------------
chown -R "$DB_USER":"$DB_USER" "$INSTALL_PREFIX"

# ============================================================================
# Phase 10: Systemd Service (optional)
# ============================================================================
SYSTEMD_FILE="/etc/systemd/system/alohadb.service"
if [[ -d /etc/systemd/system ]] && [[ ! -f "$SYSTEMD_FILE" ]]; then
    cat > "$SYSTEMD_FILE" <<SYSTEMD
[Unit]
Description=AlohaDB ${ALOHADB_VERSION} Database Server
Documentation=https://opencan.ai/
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
User=${DB_USER}
Group=${DB_USER}
ExecStart=${INSTALL_PREFIX}/bin/alohadb -D ${DATA_DIR}
ExecReload=/bin/kill -HUP \$MAINPID
KillMode=mixed
KillSignal=SIGINT
TimeoutSec=infinity
Restart=on-failure
RestartSec=10

# Resource limits
LimitNOFILE=65536
LimitNPROC=65536

[Install]
WantedBy=multi-user.target
SYSTEMD
    systemctl daemon-reload
    ok "Systemd service created: alohadb.service"
fi

# ============================================================================
# Phase 11: Start Server
# ============================================================================
echo ""
prompt_yn START_NOW "Start AlohaDB now?" "y"

if [[ "$START_NOW" == "yes" ]]; then
    info "Starting AlohaDB..."
    if [[ -f "$SYSTEMD_FILE" ]]; then
        systemctl start alohadb
        systemctl enable alohadb 2>/dev/null || true
    else
        sudo -u "$DB_USER" "$INSTALL_PREFIX/bin/al_ctl" \
            -D "$DATA_DIR" \
            -l "$INSTALL_PREFIX/logfile" \
            start
    fi

    # Wait for startup
    sleep 2

    if sudo -u "$DB_USER" "$INSTALL_PREFIX/bin/al_ctl" -D "$DATA_DIR" status &>/dev/null; then
        ok "AlohaDB is running on port $LISTEN_PORT."

        # Create default database
        info "Creating default database 'alohadb'..."
        sudo -u "$DB_USER" "$INSTALL_PREFIX/bin/alsql" -p "$LISTEN_PORT" -d postgres \
            -c "CREATE DATABASE alohadb;" 2>/dev/null || true

        # Install core extensions in the default database
        info "Installing core extensions..."
        sudo -u "$DB_USER" "$INSTALL_PREFIX/bin/alsql" -p "$LISTEN_PORT" -d alohadb -c "
            CREATE EXTENSION IF NOT EXISTS alohadb_audit;
            CREATE EXTENSION IF NOT EXISTS alohadb_cache;
            CREATE EXTENSION IF NOT EXISTS alohadb_cron;
            CREATE EXTENSION IF NOT EXISTS alohadb_query_store;
            CREATE EXTENSION IF NOT EXISTS alohadb_ratelimit;
        " 2>&1 | grep -v "^$"

        if [[ "$REMOTE_MODE" == "on" ]]; then
            sudo -u "$DB_USER" "$INSTALL_PREFIX/bin/alsql" -p "$LISTEN_PORT" -d alohadb -c "
                CREATE EXTENSION IF NOT EXISTS alohadb_remote_config;
            " 2>&1 | grep -v "^$"
        fi

        ok "Core extensions installed."
    else
        warn "Server may not have started. Check $INSTALL_PREFIX/logfile for details."
    fi
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo -e "${BOLD}============================================${NC}"
echo -e "${BOLD}  AlohaDB ${ALOHADB_VERSION} Installation Complete${NC}"
echo -e "${BOLD}============================================${NC}"
echo ""
echo -e "  Install directory:  ${GREEN}${INSTALL_PREFIX}${NC}"
echo -e "  Data directory:     ${GREEN}${DATA_DIR}${NC}"
echo -e "  Config file:        ${GREEN}${CONF_FILE}${NC}"
echo -e "  HBA file:           ${GREEN}${HBA_FILE}${NC}"
echo -e "  Audit logs:         ${GREEN}/var/log/alohadb/audit/${NC}"
echo -e "  Port:               ${GREEN}${LISTEN_PORT}${NC}"
echo -e "  System user:        ${GREEN}${DB_USER}${NC}"
echo -e "  Config mode:        ${GREEN}$([ "$REMOTE_MODE" == "on" ] && echo "Remote ($REMOTE_HOST:$REMOTE_PORT)" || echo "Local")${NC}"
if [[ -f "$SYSTEMD_FILE" ]]; then
echo -e "  Systemd service:    ${GREEN}alohadb.service${NC}"
fi
echo ""
echo -e "  ${BOLD}Connect:${NC}"
echo -e "    sudo -u $DB_USER $INSTALL_PREFIX/bin/alsql -p $LISTEN_PORT -d alohadb"
echo ""
echo -e "  ${BOLD}Service control:${NC}"
if [[ -f "$SYSTEMD_FILE" ]]; then
echo -e "    systemctl start|stop|restart|status alohadb"
else
echo -e "    sudo -u $DB_USER $INSTALL_PREFIX/bin/al_ctl -D $DATA_DIR start|stop|restart|status"
fi
echo ""
echo -e "  ${BOLD}User guide:${NC} https://opencan.ai/alohadb-guide.html"
echo ""
