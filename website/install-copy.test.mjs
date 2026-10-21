import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { bindInstallCopy } from "./install-copy.mjs";

function control(command) {
  const attributes = new Map();
  const handlers = {};
  const button = {
    hidden: true,
    addEventListener: (event, handler) => { handlers[event] = handler; },
    setAttribute: (name, value) => attributes.set(name, value),
    removeAttribute: (name) => attributes.delete(name),
  };
  const code = { textContent: command };
  const status = { textContent: "" };
  const elements = { button, code, '[role="status"]': status };
  return {
    button, code, status, attributes,
    querySelector: (selector) => elements[selector],
    click: () => handlers.click(),
  };
}

function bind(controls, clipboard) {
  bindInstallCopy({
    querySelectorAll(selector) {
      assert.equal(selector, "[data-install-copy]");
      return controls;
    },
  }, clipboard);
}

test("each control copies its current visible command, with independent feedback", async () => {
  const controls = [control("first command"), control("second command")];
  const writes = [];
  bind(controls, { writeText: async (text) => { writes.push(text); } });
  assert.ok(controls.every(({ button }) => !button.hidden));
  controls[0].code.textContent = "updated visible command";
  await controls[0].click();
  assert.deepEqual(writes, ["updated visible command"]);
  assert.equal(controls[0].status.textContent, "Copied!");
  assert.equal(controls[1].status.textContent, "");
  await controls[1].click();
  assert.deepEqual(writes, ["updated visible command", "second command"]);
  assert.equal(controls[1].status.textContent, "Copied!");
});

test("unsupported clipboard APIs give actionable feedback", async () => {
  for (const clipboard of [undefined, {}]) {
    const item = control("command");
    bind([item], clipboard);
    await item.click();
    assert.match(item.status.textContent, /Copy failed.*copy it manually/);
    assert.equal(item.attributes.has("aria-busy"), false);
  }
});

test("clipboard rejection clears stale success and permits another attempt", async () => {
  const item = control("command");
  let denied = false;
  bind([item], {
    async writeText() {
      if (denied) throw new Error("Permission denied");
    },
  });
  await item.click();
  assert.equal(item.status.textContent, "Copied!");
  denied = true;
  await item.click();
  assert.match(item.status.textContent, /Copy failed/);
  assert.equal(item.attributes.has("aria-busy"), false);
  denied = false;
  await item.click();
  assert.equal(item.status.textContent, "Copied!");
});

test("a pending copy cannot be raced by repeated clicks", async () => {
  const item = control("command");
  let finish;
  let writes = 0;
  bind([item], {
    writeText() {
      writes += 1;
      return new Promise((resolve) => { finish = resolve; });
    },
  });
  const pending = item.click();
  assert.equal(item.attributes.get("aria-busy"), "true");
  await item.click();
  assert.equal(writes, 1);
  finish();
  await pending;
  assert.equal(item.attributes.has("aria-busy"), false);
  assert.equal(item.status.textContent, "Copied!");
});

const CURL = "curl -fsSL https://nanobpm.io/install.sh | sh";
const NPM = "npm install -g @nanobpm/nano-coder";

// `commands` lists, in page order, the install command each control must carry.
function assertInstallControls(html, commands = [CURL, NPM, CURL]) {
  const controls = [...html.matchAll(/<div class="install-copy" data-install-copy>\s*<div class="install install-command" role="group" aria-label="([^"]+)"[^>]*>([\s\S]*?)<\/div>\s*<p[^>]*role="status"[^>]*><\/p>\s*<\/div>/g)];
  assert.equal(controls.length, commands.length);
  assert.equal(new Set(controls.map(([, label]) => label)).size, commands.length);
  for (const [i, [, , content]] of controls.entries()) {
    assert.doesNotMatch(content, /<\/?div\b/, "the match must not cross a container boundary");
    assert.ok(content.includes(`<code>${commands[i].replaceAll("&", "&amp;")}</code>`), `control ${i} carries ${commands[i]}`);
    assert.match(content, /<button type="button"[^>]*aria-label="Copy install command"[^>]*hidden>/);
    assert.match(content, /<svg[^>]*aria-hidden="true"/);
  }
}

test("the nano-coder page wires its npm install control to the shipped handler", () => {
  execFileSync(process.execPath, [fileURLToPath(new URL("./build.mjs", import.meta.url))], {
    stdio: "pipe",
  });
  const html = readFileSync(new URL("./_site/nano-coder/index.html", import.meta.url), "utf8");
  assertInstallControls(html, [NPM]);
  assert.match(html, /bindInstallCopy\(document, navigator\.clipboard\)/);
});

test("the published homepage wires every install control to the shipped handler", () => {
  execFileSync(process.execPath, [fileURLToPath(new URL("./build.mjs", import.meta.url))], {
    stdio: "pipe",
  });
  const html = readFileSync(new URL("./_site/index.html", import.meta.url), "utf8");
  assertInstallControls(html);
  const misplacedStatus = html.replaceAll(
    /(<p class="install-copy-status" role="status"><\/p>)(\s*<\/div>)/g,
    "$2$1",
  );
  assert.notEqual(misplacedStatus, html);
  assert.throws(() => assertInstallControls(misplacedStatus), assert.AssertionError);
  assert.match(html, /import \{ bindInstallCopy \} from "\/install-copy\.mjs"/);
  assert.match(html, /bindInstallCopy\(document, navigator\.clipboard\)/);
  assert.equal(
    readFileSync(new URL("./_site/install-copy.mjs", import.meta.url), "utf8"),
    readFileSync(new URL("./install-copy.mjs", import.meta.url), "utf8"),
  );
});
