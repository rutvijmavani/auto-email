#!/bin/bash
# scripts/setup_postgres_server.sh
#
# Run this ONCE on the Oracle VM to:
#   1. Install PostgreSQL 15
#   2. Create the database and user
#   3. Update .env with DATABASE_URL
#   4. Migrate existing SQLite data → PostgreSQL
#   5. Verify the migration
#
# Usage (as opc):
#   chmod +x scripts/setup_postgres_server.sh
#   ./scripts/setup_postgres_server.sh
#
# Requirements: Oracle Linux 8 / Ubuntu 22.04 / Debian 11
#               Python venv at ~/mail/venv
#               SQLite DB at ~/mail/data/recruiter_pipeline.db

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$PROJECT_DIR/.env"
SQLITE_DB="$PROJECT_DIR/data/recruiter_pipeline.db"
VENV="$PROJECT_DIR/venv"

PG_DB="recruiter_pipeline"
PG_USER="pipeline_user"
PG_PASS="pipeline123"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " PostgreSQL Setup for Recruiter Pipeline"
echo " Project: $PROJECT_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ─────────────────────────────────────────
# 1. Detect OS and install PostgreSQL
# ─────────────────────────────────────────
echo "[1/6] Installing PostgreSQL 15..."

if command -v apt-get &>/dev/null; then
    # Debian / Ubuntu
    sudo apt-get update -qq
    sudo apt-get install -y postgresql postgresql-contrib
    PG_SERVICE="postgresql"
elif command -v dnf &>/dev/null; then
    # Oracle Linux 8 / RHEL 8 / Rocky 8
    sudo dnf install -y postgresql-server postgresql-contrib
    # Initialize data directory (first time only)
    if [ ! -f /var/lib/pgsql/data/PG_VERSION ]; then
        sudo postgresql-setup --initdb
    fi
    PG_SERVICE="postgresql"
elif command -v yum &>/dev/null; then
    # Oracle Linux 7 / RHEL 7
    sudo yum install -y postgresql-server postgresql-contrib
    if [ ! -f /var/lib/pgsql/data/PG_VERSION ]; then
        sudo postgresql-setup initdb
    fi
    PG_SERVICE="postgresql"
else
    echo "[ERROR] Cannot detect package manager (apt/dnf/yum)"
    exit 1
fi

# Enable and start
sudo systemctl enable "$PG_SERVICE"
sudo systemctl start "$PG_SERVICE"
echo "[OK]  PostgreSQL service started"
echo ""

# ─────────────────────────────────────────
# 2. Create database and user
# ─────────────────────────────────────────
echo "[2/6] Creating database and user..."

sudo -u postgres psql -v ON_ERROR_STOP=0 <<SQL
-- Create user (ignore error if already exists)
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$PG_USER') THEN
        CREATE USER $PG_USER WITH PASSWORD '$PG_PASS';
    END IF;
END \$\$;

-- Create database (ignore error if already exists)
SELECT 'CREATE DATABASE $PG_DB OWNER $PG_USER'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '$PG_DB') \gexec

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE $PG_DB TO $PG_USER;

-- Enable citext extension (requires superuser — run as postgres)
\c $PG_DB
CREATE EXTENSION IF NOT EXISTS citext;
GRANT ALL ON SCHEMA public TO $PG_USER;
SQL

echo "[OK]  Database '$PG_DB' and user '$PG_USER' ready"
echo ""

# ─────────────────────────────────────────
# 3. Update .env with DATABASE_URL
# ─────────────────────────────────────────
echo "[3/6] Updating .env with DATABASE_URL..."

DATABASE_URL_VALUE="postgresql://$PG_USER:$PG_PASS@localhost/$PG_DB"

if grep -q "^DATABASE_URL" "$ENV_FILE" 2>/dev/null; then
    # Replace existing line
    sed -i "s|^DATABASE_URL=.*|DATABASE_URL='$DATABASE_URL_VALUE'|" "$ENV_FILE"
    echo "[OK]  Replaced DATABASE_URL in .env"
else
    # Append
    echo "" >> "$ENV_FILE"
    echo "DATABASE_URL='$DATABASE_URL_VALUE'" >> "$ENV_FILE"
    echo "[OK]  Added DATABASE_URL to .env"
fi

echo "      DATABASE_URL=$DATABASE_URL_VALUE"
echo ""

# ─────────────────────────────────────────
# 4. Install psycopg2 in venv (if not already)
# ─────────────────────────────────────────
echo "[4/6] Verifying psycopg2 is installed..."

source "$VENV/bin/activate"

if ! python -c "import psycopg2" 2>/dev/null; then
    echo "      Installing psycopg2-binary..."
    # Try pre-built binary first, fall back to source build
    pip install psycopg2-binary --quiet || pip install psycopg2 --quiet
fi

echo "[OK]  psycopg2 available: $(python -c 'import psycopg2; print(psycopg2.__version__)')"
echo ""

# ─────────────────────────────────────────
# 5. Run migration
# ─────────────────────────────────────────
echo "[5/6] Migrating SQLite → PostgreSQL..."
echo ""

cd "$PROJECT_DIR"

if [ -f "$SQLITE_DB" ]; then
    python scripts/migrate_sqlite_to_postgres.py --sqlite "$SQLITE_DB"
else
    echo "[WARN] SQLite DB not found at $SQLITE_DB"
    echo "       Running init_db() only to create empty schema..."
    python -c "
import sys; sys.path.insert(0, '.')
from db.schema import init_db
init_db()
print('[OK] Schema created (no data to migrate)')
"
fi

echo ""

# ─────────────────────────────────────────
# 6. Sanity check
# ─────────────────────────────────────────
echo "[6/6] Sanity check..."
echo ""

python - <<'PYEOF'
import sys
sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv()
from db.connection import get_conn

conn = get_conn()
tables = [
    "applications", "recruiters", "prospective_companies",
    "job_postings", "outreach",
]
print("  Table                        Rows")
print("  " + "-" * 40)
for t in tables:
    try:
        row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {t}").fetchone()
        print(f"  {t:<30} {row['cnt']:>6,}")
    except Exception as e:
        print(f"  {t:<30}  ERROR: {e}")
conn.close()
print()
print("[OK] PostgreSQL connection verified")
PYEOF

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Setup complete!"
echo ""
echo " Next step:"
echo "   git push origin main"
echo " This triggers CI/CD which runs all 1,371 tests"
echo " against an isolated Postgres instance, then"
echo " deploys the validated commit to this server."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
