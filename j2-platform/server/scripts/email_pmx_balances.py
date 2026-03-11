import argparse
import importlib.util
import os
import smtplib
import ssl
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict
import requests


def _load_server_module():
    server_path = Path(__file__).resolve().parents[1] / "server.py"
    spec = importlib.util.spec_from_file_location("j2_server_module", str(server_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load server module from: {server_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _today_mm_dd_yyyy() -> str:
    return datetime.now().strftime("%m/%d/%Y")


def _build_message_text(account_code: str, trade_name: str, start_date: str, end_date: str) -> str:
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"PMX Account Balances PDF - {account_code}",
        f"Trade Name: {trade_name}",
        f"Date Range: {start_date} to {end_date}",
        f"Fetched At: {fetched_at}",
        "",
        "Attached: PMX account balances PDF export.",
    ]
    return "\n".join(lines)


def _build_pdf_filename(account_code: str, start_date: str, end_date: str) -> str:
    start_token = str(start_date or "").replace("/", "-")
    end_token = str(end_date or "").replace("/", "-")
    return f"PMX_Account_Balances_{account_code}_{start_token}_to_{end_token}.pdf"


def _request_pmx_balances_pdf(
    server_mod: Any,
    account_code: str,
    trade_name: str,
    start_date: str,
    end_date: str,
    pdf_path: str,
    timeout: int,
    force_relogin: bool = False,
) -> Dict[str, Any]:
    data = {"force_pmx_relogin": True} if force_relogin else {}
    resolved_headers = server_mod._pmx_resolve_headers(data, None, auto_login=True)

    x_auth = str(resolved_headers.get("x_auth", "") or "").strip()
    sid = str(resolved_headers.get("sid", "") or "").strip()
    username = str(resolved_headers.get("username", "") or "").strip()
    platform = str(resolved_headers.get("platform", "") or "").strip()
    location = str(resolved_headers.get("location", "") or "").strip()
    cache_control = str(resolved_headers.get("cache_control", "") or "").strip()
    content_type = str(resolved_headers.get("content_type", "") or "").strip()

    host = str(server_mod._pmx_non_empty(os.getenv("PMX_API_HOST", ""), "pmxapi.stonex.com"))
    url = pdf_path if str(pdf_path).startswith("http") else f"https://{host}{pdf_path}"
    params = {
        "startDate": str(start_date),
        "endDate": str(end_date),
        "trd": str(trade_name),
        "trd_key": str(account_code),
    }
    headers = {
        "Accept": "application/pdf,application/octet-stream,*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "Origin": "https://pmxecute.stonex.com",
        "Referer": "https://pmxecute.stonex.com/",
    }
    if x_auth:
        headers["x-auth"] = x_auth
    if sid:
        headers["sid"] = sid
    if username:
        headers["username"] = username
        headers["usercode"] = username
    if platform:
        headers["platform"] = platform
    if location:
        headers["location"] = location
    if cache_control:
        headers["cache-control"] = cache_control
    if content_type:
        headers["content-type"] = content_type

    resp = requests.get(url, headers=headers, params=params, timeout=max(10, int(timeout)))
    content_type_resp = str(resp.headers.get("Content-Type", "") or "")
    content_disposition = str(resp.headers.get("Content-Disposition", "") or "")
    body_bytes = resp.content if isinstance(resp.content, (bytes, bytearray)) else b""
    is_pdf_content_type = "application/pdf" in content_type_resp.lower()
    is_pdf_disposition = "pdf" in content_disposition.lower()
    is_pdf_signature = bytes(body_bytes).startswith(b"%PDF")
    ok = bool(resp.ok) and (is_pdf_content_type or is_pdf_disposition or is_pdf_signature)

    body_text = ""
    if not ok:
        try:
            body_text = resp.text if isinstance(resp.text, str) else ""
        except Exception:
            body_text = ""

    return {
        "ok": ok,
        "status": int(resp.status_code),
        "reason": str(resp.reason or ""),
        "url": str(resp.url),
        "content_type": content_type_resp,
        "content_disposition": content_disposition,
        "body_bytes": bytes(body_bytes),
        "body_text": body_text,
        "error": body_text[:300].strip() if body_text.strip() else f"{resp.reason} (HTTP {resp.status_code})",
    }


def _send_email(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    sender: str,
    recipient: str,
    subject: str,
    body: str,
    use_ssl: bool,
    use_starttls: bool,
    attachment_bytes: bytes = b"",
    attachment_filename: str = "",
) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.set_content(body)
    if attachment_bytes:
        msg.add_attachment(
            bytes(attachment_bytes),
            maintype="application",
            subtype="pdf",
            filename=attachment_filename or "account_balances.pdf",
        )

    if use_ssl:
        with smtplib.SMTP_SSL(host=smtp_host, port=smtp_port, context=ssl.create_default_context()) as client:
            if smtp_user:
                client.login(smtp_user, smtp_password)
            client.send_message(msg)
        return

    with smtplib.SMTP(host=smtp_host, port=smtp_port, timeout=30) as client:
        client.ehlo()
        if use_starttls:
            client.starttls(context=ssl.create_default_context())
            client.ehlo()
        if smtp_user:
            client.login(smtp_user, smtp_password)
        client.send_message(msg)


def main() -> int:
    parser = argparse.ArgumentParser(description="Email PMX account balances PDF export.")
    parser.add_argument("--to", default=os.getenv("BALANCE_EMAIL_TO", "").strip(), help="Recipient email address")
    parser.add_argument("--account", default=os.getenv("PMX_ACC_OPT_KEY", "MT0601").strip() or "MT0601", help="PMX account code")
    parser.add_argument("--trade-name", default=os.getenv("PMX_TRADE_NAME", "Metal Concentrators").strip() or "Metal Concentrators", help="PMX trd query parameter")
    parser.add_argument("--start-date", default=os.getenv("PMX_REPORT_START_DATE", _today_mm_dd_yyyy()).strip() or _today_mm_dd_yyyy(), help="Report start date (MM/DD/YYYY)")
    parser.add_argument("--end-date", default=os.getenv("PMX_REPORT_END_DATE", _today_mm_dd_yyyy()).strip() or _today_mm_dd_yyyy(), help="Report end date (MM/DD/YYYY)")
    parser.add_argument("--pdf-path", default=os.getenv("PMX_BALANCE_PDF_PATH", "/user/export_NOPMgrPos_pdf").strip() or "/user/export_NOPMgrPos_pdf", help="PMX PDF export endpoint path")
    parser.add_argument("--timeout", type=int, default=int(os.getenv("PMX_REPORT_TIMEOUT", "60") or "60"), help="PMX HTTP timeout (seconds)")
    parser.add_argument("--subject-prefix", default=os.getenv("BALANCE_EMAIL_SUBJECT_PREFIX", "PMX Balances PDF").strip() or "PMX Balances PDF")
    parser.add_argument("--smtp-host", default=os.getenv("SMTP_HOST", "").strip(), help="SMTP host")
    parser.add_argument("--smtp-port", type=int, default=int(os.getenv("SMTP_PORT", "587") or "587"), help="SMTP port")
    parser.add_argument("--smtp-user", default=os.getenv("SMTP_USER", "").strip(), help="SMTP username")
    parser.add_argument("--smtp-password", default=os.getenv("SMTP_PASSWORD", "").strip(), help="SMTP password")
    parser.add_argument("--smtp-from", dest="smtp_from", default=os.getenv("SMTP_FROM", "").strip(), help="From address")
    parser.add_argument("--smtp-ssl", action="store_true", default=str(os.getenv("SMTP_SSL", "")).strip().lower() in {"1", "true", "yes", "y", "on"}, help="Use SMTP over SSL")
    parser.add_argument("--smtp-starttls", action="store_true", default=str(os.getenv("SMTP_STARTTLS", "true")).strip().lower() in {"1", "true", "yes", "y", "on"}, help="Use STARTTLS for non-SSL SMTP")
    parser.add_argument("--dry-run", action="store_true", help="Print the email body without sending")
    args = parser.parse_args()

    if not args.to:
        raise SystemExit("Missing recipient. Set --to or BALANCE_EMAIL_TO.")
    if not args.smtp_host and not args.dry_run:
        raise SystemExit("Missing SMTP host. Set --smtp-host or SMTP_HOST.")

    server_mod = _load_server_module()
    if not hasattr(server_mod, "_pmx_resolve_headers"):
        raise SystemExit("Could not access PMX auth helpers in server.py")

    result = _request_pmx_balances_pdf(
        server_mod=server_mod,
        account_code=args.account,
        trade_name=args.trade_name,
        start_date=args.start_date,
        end_date=args.end_date,
        pdf_path=args.pdf_path,
        timeout=args.timeout,
        force_relogin=False,
    )
    if not result.get("ok") and int(result.get("status") or 0) in {401, 403, 500}:
        result = _request_pmx_balances_pdf(
            server_mod=server_mod,
            account_code=args.account,
            trade_name=args.trade_name,
            start_date=args.start_date,
            end_date=args.end_date,
            pdf_path=args.pdf_path,
            timeout=args.timeout,
            force_relogin=True,
        )
    if not result.get("ok"):
        err = str(result.get("error") or "").strip() or f"HTTP {result.get('status')}"
        raise SystemExit(f"Failed to download PMX PDF: {err}")

    attachment_filename = _build_pdf_filename(args.account, args.start_date, args.end_date)
    subject = f"{args.subject_prefix} | {args.account} | {args.start_date} to {args.end_date}"
    body = _build_message_text(args.account, args.trade_name, args.start_date, args.end_date)

    if args.dry_run:
        print(subject)
        print("")
        print(body)
        print("")
        print(f"Attachment: {attachment_filename} ({len(result.get('body_bytes', b'')):,} bytes)")
        return 0

    sender = args.smtp_from or args.smtp_user
    if not sender:
        raise SystemExit("Missing sender email. Set --smtp-from/SMTP_FROM or --smtp-user/SMTP_USER.")

    _send_email(
        smtp_host=args.smtp_host,
        smtp_port=args.smtp_port,
        smtp_user=args.smtp_user,
        smtp_password=args.smtp_password,
        sender=sender,
        recipient=args.to,
        subject=subject,
        body=body,
        use_ssl=bool(args.smtp_ssl),
        use_starttls=bool(args.smtp_starttls),
        attachment_bytes=bytes(result.get("body_bytes", b"")),
        attachment_filename=attachment_filename,
    )
    print(f"Email sent to {args.to} with attachment: {attachment_filename}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
