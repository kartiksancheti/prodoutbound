"""
create_tenant.py — onboard a new business onto the platform.

There is no public self-serve signup yet (onboarding is sales/ops-assisted),
so this is how a new tenant gets created: run it on the VPS, answer the
prompts (or pass flags for scripting), and it creates the organization row
plus its first login.

Usage (interactive):
    python3 create_tenant.py

Usage (scripted):
    python3 create_tenant.py --name "Acme Dental" --slug acme-dental \\
        --email owner@acmedental.com --password 'change-me-now' \\
        --outbound-number +14155551234 --transfer-number +14155559999 \\
        --max-calls-per-day 200
"""

import argparse
import asyncio
import re
import sys

from dotenv import load_dotenv

load_dotenv(override=False)

from auth import hash_password
from db import create_organization, create_user, get_organization_by_slug, get_user_by_email

SLUG_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")


def slugify(name: str) -> str:
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", name.lower())).strip("-")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Onboard a new tenant organization.")
    parser.add_argument("--name", help="Business name, e.g. 'Acme Dental'")
    parser.add_argument("--slug", help="URL-safe identifier, e.g. 'acme-dental' (auto-generated from --name if omitted)")
    parser.add_argument("--email", help="First admin user's login email")
    parser.add_argument("--password", help="First admin user's password (min 8 chars)")
    parser.add_argument("--outbound-number", default=None, help="This org's caller ID, E.164 (e.g. +14155551234). Can be set later.")
    parser.add_argument("--transfer-number", default=None, help="Where 'transfer to a human' should ring for this org. Can be set later.")
    parser.add_argument("--max-calls-per-day", type=int, default=500, help="Daily outbound call cap (default 500)")
    args = parser.parse_args()

    name = args.name or input("Business name: ").strip()
    if not name:
        print("❌ Business name is required.")
        sys.exit(1)

    slug = args.slug or slugify(name)
    if not args.slug:
        confirm = input(f"Slug [{slug}]: ").strip()
        slug = confirm or slug
    if not SLUG_RE.match(slug):
        print(f"❌ Slug '{slug}' must be lowercase letters/numbers/hyphens only.")
        sys.exit(1)
    if await get_organization_by_slug(slug):
        print(f"❌ Slug '{slug}' is already in use.")
        sys.exit(1)

    email = args.email or input("Admin email: ").strip()
    if not email or "@" not in email:
        print("❌ A valid email is required.")
        sys.exit(1)
    if await get_user_by_email(email):
        print(f"❌ Email '{email}' is already registered.")
        sys.exit(1)

    password = args.password
    if not password:
        import getpass
        password = getpass.getpass("Admin password (min 8 chars): ")

    outbound_number = args.outbound_number
    if outbound_number is None and not args.name:  # only prompt in interactive mode
        outbound_number = input("Outbound caller-ID number (E.164, blank to set later): ").strip() or None

    transfer_number = args.transfer_number
    if transfer_number is None and not args.name:
        transfer_number = input("Transfer-to-human number (E.164, blank to set later): ").strip() or None

    try:
        password_hash = hash_password(password)
    except ValueError as exc:
        print(f"❌ {exc}")
        sys.exit(1)

    org_id = await create_organization(
        name=name, slug=slug, outbound_number=outbound_number,
        transfer_number=transfer_number, max_calls_per_day=args.max_calls_per_day,
    )
    user_id = await create_user(email=email, password_hash=password_hash, org_id=org_id, role="owner")

    print("\n✅ Tenant created.")
    print(f"   Organization: {name}  (id: {org_id}, slug: {slug})")
    print(f"   Outbound number: {outbound_number or '(not set — set it via /api/admin/organizations before going live)'}")
    print(f"   Transfer number: {transfer_number or '(not set — falls back to platform DEFAULT_TRANSFER_NUMBER)'}")
    print(f"   Daily call cap:  {args.max_calls_per_day}")
    print(f"   First login:     {email}  (id: {user_id}, role: owner)")
    print("\nHand the email + password to the business — they log in at your dashboard URL.")
    if outbound_number:
        print("⚠️  Reminder: confirm with Vobiz that this number is allow-listed as a valid")
        print("   'From' on your trunk account before relying on it for live calls.")


if __name__ == "__main__":
    asyncio.run(main())
