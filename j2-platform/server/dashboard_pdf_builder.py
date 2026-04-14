"""
Server-side Dashboard Trading Summary PDF builder.

Uses Playwright (headless Chromium) to open the actual dashboard UI,
trigger the same client-side PDF generation (html2canvas + jsPDF),
and capture the resulting PDF bytes.

This produces the EXACT same PDF as clicking "Download Trading Summary (PDF)".
"""
from __future__ import annotations

import base64
import os
import smtplib
import ssl
import time
from datetime import datetime
from email.message import EmailMessage
from typing import Any, Dict, List, Optional


def generate_dashboard_pdf(
    app_url: str = "http://localhost:5173",
    auth_user: str = "",
    auth_password: str = "",
    timeout_ms: int = 120_000,
) -> bytes:
    """
    Launch headless Chromium, navigate to the Dashboard tab, and execute
    the client-side buildDashboardPdfBlob() to produce the exact same PDF.
    Returns the PDF as raw bytes.
    """
    from playwright.sync_api import sync_playwright

    auth_user = auth_user or os.environ.get("APP_AUTH_USERNAME", "admin")
    auth_password = auth_password or os.environ.get("APP_AUTH_PASSWORD", "admin")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = context.new_page()
        page.set_default_timeout(timeout_ms)

        # 1) Navigate to the app
        print("[headless] Loading app...")
        page.goto(app_url, wait_until="networkidle", timeout=timeout_ms)

        # 2) Login if a login form is present
        try:
            login_form = page.locator("input[type='text'], input[name='username']").first
            if login_form.is_visible(timeout=5000):
                print("[headless] Login form detected, logging in...")
                login_form.fill(auth_user)
                pw_field = page.locator("input[type='password']").first
                pw_field.fill(auth_password)
                pw_field.press("Enter")
                page.wait_for_load_state("networkidle", timeout=30000)
                page.wait_for_timeout(2000)
        except Exception:
            pass

        # 3) Click the Dashboard nav button in the sidebar
        print("[headless] Navigating to Dashboard tab...")
        try:
            # The nav buttons have class "nav-btn" and contain text
            nav_btns = page.locator("button.nav-btn")
            count = nav_btns.count()
            clicked = False
            for i in range(count):
                btn = nav_btns.nth(i)
                text = btn.inner_text()
                if "dashboard" in text.lower().strip():
                    btn.click()
                    clicked = True
                    break
            if not clicked:
                # Fallback: try any button containing "Dashboard"
                page.locator("button:has-text('Dashboard')").first.click()
        except Exception as e:
            print(f"[headless] Could not click Dashboard tab: {e}")

        # 4) Wait for dashboard to load fully (charts, data, etc.)
        print("[headless] Waiting for dashboard data to load...")
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(8000)

        # 5) Intercept blob download, then click the PDF button
        print("[headless] Triggering PDF generation...")
        pdf_base64 = page.evaluate("""
            () => {
                return new Promise(async (resolve, reject) => {
                    const timeout = setTimeout(() => reject('Timed out after 120s'), 120000);
                    try {
                        let capturedBlob = null;

                        // Monkey-patch to intercept the PDF blob
                        const origCreateObjectURL = URL.createObjectURL;
                        URL.createObjectURL = function(blob) {
                            if (blob instanceof Blob && blob.type === 'application/pdf') {
                                capturedBlob = blob;
                            }
                            return origCreateObjectURL.call(URL, blob);
                        };

                        const origAnchorClick = HTMLAnchorElement.prototype.click;
                        HTMLAnchorElement.prototype.click = function() {
                            if (this.download && capturedBlob) return;
                            return origAnchorClick.call(this);
                        };

                        // Find the PDF download button
                        const buttons = Array.from(document.querySelectorAll('button'));
                        const pdfBtn = buttons.find(b => {
                            const t = (b.textContent || '').trim();
                            return t.includes('Download Trading Summary') ||
                                   t.includes('Trading Summary (PDF)');
                        });

                        if (!pdfBtn) {
                            clearTimeout(timeout);
                            // List available buttons for debugging
                            const btnTexts = buttons
                                .map(b => (b.textContent || '').trim().substring(0, 60))
                                .filter(t => t.length > 0);
                            reject('No PDF button found. Buttons on page: ' +
                                   JSON.stringify(btnTexts.slice(0, 20)));
                            return;
                        }

                        pdfBtn.click();

                        // Poll for captured blob
                        while (!capturedBlob) {
                            await new Promise(r => setTimeout(r, 500));
                        }

                        // Restore
                        URL.createObjectURL = origCreateObjectURL;
                        HTMLAnchorElement.prototype.click = origAnchorClick;
                        clearTimeout(timeout);

                        // Convert to base64
                        const buf = await capturedBlob.arrayBuffer();
                        const bytes = new Uint8Array(buf);
                        let binary = '';
                        const chunk = 0x8000;
                        for (let i = 0; i < bytes.length; i += chunk) {
                            binary += String.fromCharCode.apply(
                                null, bytes.subarray(i, Math.min(i + chunk, bytes.length))
                            );
                        }
                        resolve(btoa(binary));
                    } catch (err) {
                        clearTimeout(timeout);
                        reject(String(err));
                    }
                });
            }
        """)

        print("[headless] PDF captured, closing browser.")
        browser.close()

    if not pdf_base64:
        return b""

    return base64.b64decode(pdf_base64)


def send_email(
    pdf_bytes: bytes,
    to_list: List[str],
    cc_list: Optional[List[str]] = None,
    subject: str = "",
    body: str = "",
    file_name: str = "",
    smtp_host: str = "",
    smtp_port: int = 587,
    smtp_user: str = "",
    smtp_password: str = "",
    smtp_from: str = "",
    use_ssl: bool = False,
    use_starttls: bool = True,
) -> Dict[str, Any]:
    """Send the dashboard PDF via email."""
    cc_list = cc_list or []
    if not to_list:
        return {"ok": False, "error": "No recipients."}
    if not smtp_host:
        return {"ok": False, "error": "SMTP_HOST not configured."}
    sender = smtp_from or smtp_user
    if not sender:
        return {"ok": False, "error": "No sender address."}

    today = datetime.now().strftime("%Y-%m-%d")
    final_subject = subject or f"Dashboard Trading Summary | {today}"
    final_body = body or (
        "Please find attached the dashboard trading summary report.\n"
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    safe_name = file_name or f"dashboard_trading_summary_{today}.pdf"
    if not safe_name.lower().endswith(".pdf"):
        safe_name += ".pdf"

    msg = EmailMessage()
    msg["Subject"] = final_subject
    msg["From"] = sender
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg.set_content(final_body)
    msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=safe_name)

    try:
        if use_ssl:
            with smtplib.SMTP_SSL(host=smtp_host, port=smtp_port,
                                  context=ssl.create_default_context(), timeout=60) as client:
                if smtp_user:
                    client.login(smtp_user, smtp_password)
                client.send_message(msg)
        else:
            with smtplib.SMTP(host=smtp_host, port=smtp_port, timeout=60) as client:
                client.ehlo()
                if use_starttls:
                    client.starttls(context=ssl.create_default_context())
                    client.ehlo()
                if smtp_user:
                    client.login(smtp_user, smtp_password)
                client.send_message(msg)
        delivered = to_list + cc_list
        return {"ok": True, "message": f"Email sent to {', '.join(delivered)}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
