/*
 * Part of the nano Zeebe differential-trace harness (formal/traces, #1391).
 */
package io.nanobpm.traces;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies {@code formal/traces/normaliser.json} — the ONE normaliser spec, mirrored by {@code
 * normalise} in {@code engine-core/tests/zeebe_traces.rs}. The algorithm is documented in the
 * spec's {@code doc} field; keep both implementations to that text.
 */
final class Normaliser {
  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
  private final JsonNode spec;

  Normaliser(final JsonNode spec) {
    this.spec = spec;
    if (spec.get("version").asInt() != 1) {
      throw new IllegalArgumentException("unsupported normaliser spec version");
    }
  }

  ArrayNode normalise(final ArrayNode raw) {
    final JsonNode lanes = spec.get("lanes");
    final List<ArrayNode> out = new ArrayList<>();
    for (int i = 0; i <= lanes.size(); i++) {
      out.add(NODES.arrayNode());
    }
    for (final JsonNode r : raw) {
      final String recordType = r.path("recordType").asText();
      if (recordType.equals("COMMAND_REJECTION")) {
        final ObjectNode n = NODES.objectNode();
        n.put("recordType", recordType);
        for (final JsonNode f : spec.get("rejection").get("fields")) {
          n.set(f.asText(), r.path(f.asText()).isMissingNode() ? NODES.nullNode() : r.get(f.asText()));
        }
        out.get(lanes.size()).add(n);
        continue;
      }
      if (!recordType.equals("EVENT")) {
        continue;
      }
      for (int i = 0; i < lanes.size(); i++) {
        final JsonNode lane = lanes.get(i);
        if (!lane.get("valueType").asText().equals(r.path("valueType").asText())) {
          continue;
        }
        boolean listed = false;
        for (final JsonNode intent : lane.get("intents")) {
          listed |= intent.asText().equals(r.path("intent").asText());
        }
        if (listed) {
          final ObjectNode n = NODES.objectNode();
          n.put("valueType", r.get("valueType").asText());
          n.put("intent", r.get("intent").asText());
          final JsonNode value = r.path("value");
          for (final JsonNode f : lane.get("fields")) {
            final JsonNode v = value.path(f.asText());
            n.set(f.asText(), v.isMissingNode() ? NODES.nullNode() : v);
          }
          out.get(i).add(n);
        }
      }
    }
    final ArrayNode all = NODES.arrayNode();
    out.forEach(all::addAll);
    renumberKeys(all);
    return all;
  }

  /**
   * Rewrite every {@code *Key} field of the kept records to a canonical id: the first distinct
   * raw key a step's records touch (in lane order) becomes 1, the next 2, and so on (spec {@code
   * doc} step 5). Raw key VALUES are engine-specific, but the identity pattern is behavioural:
   * completing a task in instance A must not read as completing it in instance B.
   */
  private static void renumberKeys(final ArrayNode records) {
    final Map<Long, Long> ids = new HashMap<>();
    for (final JsonNode r : records) {
      if (!r.isObject()) {
        continue;
      }
      final ObjectNode o = (ObjectNode) r;
      final List<String> names = new ArrayList<>();
      o.fieldNames().forEachRemaining(names::add);
      for (final String name : names) {
        // Only `*Key` fields carry runtime identity — a plain number (`retries`, a numeric
        // variable `value`) is data, not a key.
        if (!name.endsWith("Key")) {
          continue;
        }
        final JsonNode v = o.get(name);
        if (v != null && v.isIntegralNumber() && v.asLong() >= 0) {
          final long canonical = ids.computeIfAbsent(v.asLong(), k -> (long) ids.size() + 1);
          o.put(name, canonical);
        }
      }
    }
  }
}
