// Tests for the engine-wasm README API generator (#1899).
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { BEGIN, END, OPERATION_ALIASES, generate, parseExports, parseSpec, render, splice } from "./gen-readme-api.mjs";

const LIB = `
#[wasm_bindgen]
impl TestEngine {
    /// Make one. Extra detail.
    #[wasm_bindgen(constructor)]
    pub fn new() -> TestEngine {
        TestEngine { x: "}" }
    }

    /// Complete a job.
    #[wasm_bindgen(js_name = completeJob)]
    pub fn complete_job(
        &mut self,
        job_key: &str,
        lease_token: Option<String>,
    ) -> Result<String, JsValue> {
        todo!()
    }

    fn private_helper(&self) {}
}

impl TestEngine {
    pub fn not_exported(&self) {}
}

#[cfg(feature = "read-model")]
#[wasm_bindgen]
impl TestEngine {
    /// Get a form.
    #[wasm_bindgen(js_name = getFormByKey)]
    pub fn get_form_by_key(&self, form_key: &str) -> Result<String, JsValue> {
        todo!()
    }
}
`;

const SPEC = `paths:
  /jobs/{jobKey}/completion:
    post:
      operationId: completeJob
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/JobCompletionRequest'
      responses:
        "204":
          description: ok
        "404":
          content:
            application/problem+json:
              schema:
                $ref: 'problem-detail.yaml#/components/schemas/ProblemDetail'
  /forms/{formKey}:
    get:
      operationId: getFormByKey
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: 'form-models.yaml#/components/schemas/FormResult'
components:
  schemas:
    X:
      operationId: notAnOperation
`;

test("parses only #[wasm_bindgen] impl blocks, with feature gating", () => {
  const m = parseExports(LIB);
  assert.deepEqual(
    m.map((x) => [x.jsName, x.kind, x.readModel]),
    [
      ["new", "constructor", false],
      ["completeJob", "method", false],
      ["getFormByKey", "method", true],
    ],
  );
  assert.deepEqual(m[1].params, [
    { name: "jobKey", type: "string" },
    { name: "leaseToken", type: "string?" },
  ]);
  assert.equal(m[0].summary, "Make one.");
});

test("spec index takes the success response only, never an error schema", () => {
  const ops = parseSpec([["jobs.yaml", SPEC]]);
  assert.equal(ops.get("completeJob").request, "JobCompletionRequest");
  assert.equal(ops.get("completeJob").response, null);
  assert.equal(ops.get("getFormByKey").response, "FormResult");
  assert.equal(ops.get("getFormByKey").method, "GET");
  assert.equal(ops.has("notAnOperation"), false);
});

test("an alias to an unknown operationId fails loudly", () => {
  const ops = parseSpec([["jobs.yaml", SPEC]]);
  assert.throws(() => render([{ jsName: "deploy", kind: "method", params: [], returns: "void", summary: "", readModel: false }], ops), /unknown operationId/);
});

test("every alias target exists in the real spec, and the README is current", () => {
  const section = generate();
  for (const [method, opId] of Object.entries(OPERATION_ALIASES)) {
    assert.match(section, new RegExp(`\\[\`${opId}\`\\]`), `${method} -> ${opId}`);
  }
  const readme = readFileSync(new URL("../README.md", import.meta.url), "utf8");
  assert.equal(splice(readme, section), readme, "README API section is stale; run gen-readme-api.mjs");
});

test("splice requires markers", () => {
  assert.throws(() => splice("no markers", "x"), /markers/);
  assert.equal(splice(`a${BEGIN}old${END}b`, "NEW"), "aNEWb");
});

test("firstSentence keeps code spans and abbreviations intact", async () => {
  const { firstSentence } = await import("./gen-readme-api.mjs");
  assert.equal(firstSentence("Patch `name?` and `tags?` fields. More."), "Patch `name?` and `tags?` fields.");
  assert.equal(firstSentence("Deploy a resource (e.g. a BPMN file). Next."), "Deploy a resource (e.g. a BPMN file).");
  assert.equal(firstSentence("Plain i.e. text, etc. done! Tail"), "Plain i.e. text, etc. done!");
  assert.equal(firstSentence("No terminator"), "No terminator");
});
