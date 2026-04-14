#!/usr/bin/env python
"""
Test: Generate the exact Dashboard Trading Summary PDF (via headless browser)
and email it. Produces the SAME PDF as clicking "Download Trading Summary (PDF)".

Usage:
  python scripts/test_dashboard_email.py --to joshua.kress@metcon.co.za
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

SERVER_DIR = Path(__file__).resolve().parents[1]
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))


def _load_env(env_path: Path) -> None:
    if not env_path.is_file():
        return
    with open(env_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if key:
                os.environ.setdefault(key, value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Test dashboard PDF email (headless browser).")
    parser.add_argument("--to", required=True, help="Recipient email (comma-separated)")
    parser.add_argument("--app-url", default="http://localhost:5173", help="Frontend URL")
    parser.add_argument("--save-pdf", default="", help="Also save PDF to this path")
    args = parser.parse_args()

    print(f"[1/3] Loading .env ...")
    _load_env(SERVER_DIR / ".env")

    import dashboard_pdf_builder as dpb

    print(f"[2/3] Generating PDF via headless browser at {args.app_url} ...")
    print("       (This opens Chromium, navigates to Dashboard, clicks the PDF button)")
    pdf_bytes = dpb.generate_dashboard_pdf(
        app_url=args.app_url,
        auth_user=os.environ.get("APP_AUTH_USERNAME", "admin"),
        auth_password=os.environ.get("APP_AUTH_PASSWORD", "admin"),
    )
    if not pdf_bytes:
        print("ERROR: PDF generation returned empty bytes.")
        return 1
    print(f"       PDF captured: {len(pdf_bytes):,} bytes")

    if args.save_pdf:
        save_path = Path(args.save_pdf)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(pdf_bytes)
        print(f"       Saved to: {save_path}")

    today = datetime.now().strftime("%Y-%m-%d")
    to_list = [e.strip() for e in args.to.split(",") if e.strip()]

    print(f"[3/3] Sending email to {', '.join(to_list)} ...")
    result = dpb.send_email(
        pdf_bytes=pdf_bytes,
        to_list=to_list,
        subject=f"Dashboard Trading Summary | {today} (TEST)",
        body=(
            "This is a TEST email of the automated dashboard trading summary.\n"
            f"Date: {today}\n"
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        ),
        smtp_host=os.environ.get("SMTP_HOST", ""),
        smtp_port=int(os.environ.get("SMTP_PORT", "587")),
        smtp_user=os.environ.get("SMTP_USER", ""),
        smtp_password=os.environ.get("SMTP_PASSWORD", ""),
        smtp_from=os.environ.get("SMTP_FROM", ""),
        use_ssl=os.environ.get("SMTP_SSL", "").lower() in {"1", "true", "yes"},
        use_starttls=os.environ.get("SMTP_STARTTLS", "true").lower() in {"1", "true", "yes"},
    )

    if result.get("ok"):
        print(f"SUCCESS: {result.get('message')}")
        return 0
    else:
        print(f"FAILED: {result.get('error')}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
