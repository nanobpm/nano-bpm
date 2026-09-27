from playwright.sync_api import sync_playwright

errors = []
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.on("console", lambda m: errors.append(f"{m.type}: {m.text}") if m.type == "error" else None)
    page.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))
    page.goto("http://localhost:4173")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(500)

    heading = page.locator(".pc-heading").first
    print("HEADING:", heading.inner_text() if heading.count() else "(none)")

    # dataGrid rendered with rows?
    rows = page.locator("table.pc-grid tbody tr")
    print("GRID ROWS:", rows.count())
    print("FIRST ROW:", rows.first.inner_text().replace("\n", " | ") if rows.count() else "(none)")

    # tabs present?
    tabs = page.locator(".pc-tabs .pc-tab")
    print("TABS:", tabs.count(), [tabs.nth(i).inner_text() for i in range(tabs.count())])

    # form present?
    form_inputs = page.locator(".pc-field input")
    print("FORM INPUTS:", form_inputs.count())

    # interact: switch to second tab, confirm rows reload
    if tabs.count() > 1:
        tabs.nth(1).click()
        page.wait_for_timeout(400)
        print("AFTER TAB SWITCH ROWS:", page.locator("table.pc-grid tbody tr").count())

    # submit the form -> expect status message
    if form_inputs.count():
        form_inputs.first.fill("hello@nano.test")
        page.locator(".pc-btn").first.click()
        page.wait_for_timeout(500)
        print("FORM STATUS:", page.locator(".pc-msg").last.inner_text())

    page.screenshot(path="/tmp/foldkit-urban.png", full_page=True)
    print("CONSOLE ERRORS:", errors if errors else "none")
    browser.close()
