#!/usr/bin/env bash
# =============================================================================
# CodeMed AI — Trust Infrastructure Setup
# =============================================================================
# Sets up Stripe billing + HIPAA BAA infrastructure in one command.
#
# Usage:
#   chmod +x setup-trust.sh
#   ./setup-trust.sh
#
# What it does:
#   1. Installs the stripe Python package
#   2. Adds Stripe + BAA env vars to .env (if missing)
#   3. Runs migration 004 against your DATABASE_URL
#   4. Verifies the Stripe connection (if keys are set)
#   5. Prints next steps
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║        CodeMed AI — Trust Infrastructure Setup       ║${NC}"
echo -e "${BOLD}║         Stripe Billing  ·  HIPAA BAA Records         ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""

# ---------------------------------------------------------------------------
# Step 1: Install stripe
# ---------------------------------------------------------------------------
info "Step 1/5 — Installing Stripe Python package..."

if python3 -c "import stripe" 2>/dev/null; then
    STRIPE_VER=$(python3 -c "import stripe; print(stripe.__version__)")
    success "stripe already installed (v$STRIPE_VER)"
else
    pip install -q "stripe>=9.0.0"
    STRIPE_VER=$(python3 -c "import stripe; print(stripe.__version__)")
    success "stripe v$STRIPE_VER installed"
fi

# ---------------------------------------------------------------------------
# Step 2: .env — add missing keys
# ---------------------------------------------------------------------------
info "Step 2/5 — Checking .env for Stripe + BAA variables..."

if [[ ! -f .env ]]; then
    if [[ -f .env.example ]]; then
        cp .env.example .env
        info "Created .env from .env.example"
    else
        touch .env
        info "Created empty .env"
    fi
fi

add_env_var() {
    local key="$1" value="$2"
    if ! grep -q "^${key}=" .env 2>/dev/null; then
        echo "${key}=${value}" >> .env
        warn "Added placeholder ${key} — fill in .env before going live"
    fi
}

add_env_var "STRIPE_SECRET_KEY"      "sk_test_REPLACE_ME"
add_env_var "STRIPE_PUBLISHABLE_KEY" "pk_test_REPLACE_ME"
add_env_var "STRIPE_WEBHOOK_SECRET"  "whsec_REPLACE_ME"
add_env_var "BUSINESS_NAME"          "CodeMed Inc."
add_env_var "BUSINESS_EMAIL"         "billing@codemed.com"
add_env_var "BUSINESS_PHONE"         "555-555-5555"

success ".env variables present"

# ---------------------------------------------------------------------------
# Step 3: Run database migration 004
# ---------------------------------------------------------------------------
info "Step 3/5 — Running database migration 004 (Stripe + BAA schema)..."

# Load DATABASE_URL from .env if not already in environment
if [[ -z "${DATABASE_URL:-}" ]] && [[ -f .env ]]; then
    export DATABASE_URL=$(grep -E "^DATABASE_URL=" .env | cut -d= -f2-)
fi

if [[ -z "${DATABASE_URL:-}" ]]; then
    warn "DATABASE_URL not set — skipping migration."
    warn "Run manually: psql \$DATABASE_URL -f database/migrations/004_stripe_baa.sql"
else
    MIGRATION="database/migrations/004_stripe_baa.sql"
    if psql "$DATABASE_URL" -f "$MIGRATION" -v ON_ERROR_STOP=1 -q 2>&1; then
        success "Migration 004 applied"
    else
        error "Migration failed. Check your DATABASE_URL and try again."
        error "Manual run: psql \$DATABASE_URL -f $MIGRATION"
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# Step 4: Verify Stripe connection
# ---------------------------------------------------------------------------
info "Step 4/5 — Verifying Stripe API connection..."

STRIPE_KEY=$(grep -E "^STRIPE_SECRET_KEY=" .env 2>/dev/null | cut -d= -f2- || true)

if [[ -z "$STRIPE_KEY" ]] || [[ "$STRIPE_KEY" == "sk_test_REPLACE_ME" ]]; then
    warn "Stripe key not configured — skipping live check"
    warn "Add your key to .env: STRIPE_SECRET_KEY=sk_test_..."
else
    if python3 - <<PYEOF
import os, sys
os.environ['STRIPE_SECRET_KEY'] = '$STRIPE_KEY'
import stripe
stripe.api_key = '$STRIPE_KEY'
try:
    stripe.Balance.retrieve()
    print('ok')
except stripe.error.AuthenticationError:
    print('auth_error')
    sys.exit(1)
except Exception as e:
    print(f'error: {e}')
    sys.exit(1)
PYEOF
    then
        success "Stripe API key is valid"
    else
        error "Stripe API key rejected. Check your STRIPE_SECRET_KEY in .env."
    fi
fi

# ---------------------------------------------------------------------------
# Step 5: BAA template
# ---------------------------------------------------------------------------
info "Step 5/5 — BAA template..."

if [[ -f "baas/CodeMed_BAA_Template.md" ]]; then
    success "BAA template ready: baas/CodeMed_BAA_Template.md"
else
    warn "BAA template not found at baas/CodeMed_BAA_Template.md"
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Trust Infrastructure Setup Complete!${NC}"
echo ""
echo -e "  ${BOLD}Next Steps:${NC}"
echo ""
echo -e "  1. ${YELLOW}Configure Stripe keys${NC} in .env:"
echo -e "       STRIPE_SECRET_KEY=sk_live_..."
echo -e "       STRIPE_PUBLISHABLE_KEY=pk_live_..."
echo ""
echo -e "  2. ${YELLOW}Register webhook endpoint${NC} in Stripe Dashboard:"
echo -e "       URL: https://api.codemed.com/v1/billing/webhook"
echo -e "       Events: invoice.payment_succeeded, invoice.payment_failed,"
echo -e "               customer.subscription.deleted, setup_intent.succeeded"
echo -e "       Copy the signing secret → STRIPE_WEBHOOK_SECRET in .env"
echo ""
echo -e "  3. ${YELLOW}Mount billing routes${NC} in codemed/api.py:"
echo -e "       from billing.routes import billing_router"
echo -e "       app.include_router(billing_router, prefix=\"/v1/billing\")"
echo ""
echo -e "  4. ${YELLOW}Customize BAA template${NC}:"
echo -e "       baas/CodeMed_BAA_Template.md"
echo -e "       - Replace [YOUR STATE] with your state"
echo -e "       - Add your business address and phone"
echo -e "       - Upload to HelloSign / DocuSign for e-signing"
echo ""
echo -e "  5. ${YELLOW}Schedule monthly billing cron${NC} (1st of each month, 8am):"
echo -e "       0 8 1 * * cd /app && python -m billing.monthly_billing_job"
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
