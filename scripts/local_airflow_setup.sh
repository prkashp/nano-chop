#!/bin/bash
# Setup script for local Airflow deployment with nano-chop
# Usage: bash scripts/local_airflow_setup.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=========================================="
echo "nano-chop Local Airflow Setup"
echo "=========================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker is not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker is installed${NC}"

if ! docker ps &> /dev/null; then
    echo -e "${RED}✗ Docker daemon is not running${NC}"
    echo "Start it with: colima start"
    exit 1
fi
echo -e "${GREEN}✓ Docker daemon is running${NC}"

# Warn about colima cpuType on Apple Silicon
if [[ "$(uname -m)" == "arm64" ]]; then
    COLIMA_CFG="$HOME/.colima/default/colima.yaml"
    if [ -f "$COLIMA_CFG" ] && grep -q 'arch: x86_64' "$COLIMA_CFG"; then
        if ! grep -q 'cpuType: "max"' "$COLIMA_CFG" && ! grep -q "cpuType: max" "$COLIMA_CFG"; then
            echo ""
            echo -e "${YELLOW}WARNING: Apple Silicon + colima x86_64 detected${NC}"
            echo "You must set cpuType: max in $COLIMA_CFG"
            echo "Otherwise Airflow will crash with 'Illegal instruction'."
            echo ""
            echo "Fix:"
            echo "  1. Edit $COLIMA_CFG and set: cpuType: \"max\""
            echo "  2. Run: colima stop && colima start"
            echo "  3. Re-run this script"
            echo ""
            read -p "Continue anyway? [y/N] " confirm
            [[ "$confirm" =~ ^[Yy]$ ]] || exit 1
        fi
    fi
fi

echo ""

# Set up .env file
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"
    echo -e "${GREEN}✓ .env created — fill in your Snowflake credentials${NC}"
else
    echo -e "${YELLOW}⊘ .env already exists${NC}"
fi

# Create required directories
mkdir -p "$PROJECT_ROOT/logs"
echo -e "${GREEN}✓ Directories ready${NC}"

echo ""
echo "Starting Airflow services..."

cd "$PROJECT_ROOT"
docker-compose up -d

echo ""
echo "Waiting for webserver to become healthy..."
attempts=0
max=40
until curl -sf http://localhost:8080/health | grep -q '"metadatabase": {"status": "healthy"}' 2>/dev/null; do
    attempts=$((attempts + 1))
    if [ $attempts -ge $max ]; then
        echo -e "${YELLOW}⊘ Webserver took longer than expected${NC}"
        echo "Check logs: docker-compose logs webserver"
        break
    fi
    printf "."
    sleep 5
done

echo ""
echo "=========================================="
echo -e "${GREEN}Setup Complete!${NC}"
echo "=========================================="
echo ""
echo "  UI:        http://localhost:8080"
echo "  Username:  airflow"
echo "  Password:  airflow"
echo ""
echo "Next steps:"
echo "  1. Fill in Snowflake credentials in .env"
echo "  2. Edit config/tables.yaml with your table definitions"
echo "  3. Trigger a DAG in the Airflow UI"
echo ""
echo "Useful commands:"
echo "  docker-compose logs -f webserver    # Tail webserver logs"
echo "  docker-compose logs -f scheduler    # Tail scheduler logs"
echo "  docker-compose down                 # Stop all services"
echo "  docker-compose ps                   # Show container status"
