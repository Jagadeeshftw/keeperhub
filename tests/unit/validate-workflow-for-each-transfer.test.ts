import { describe, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

import { validateWorkflow } from "@/lib/mcp/validate-workflow";
import {
  actionNode,
  edge,
  makeWorkflow,
  triggerNode,
} from "./fixtures/validate-workflow";

const forEachNode = (id = "loop-1") => ({
  id,
  type: "action",
  data: {
    label: "For Each",
    type: "action",
    config: { actionType: "For Each" },
  },
});

function warningsFor(nodes: unknown[], edges: unknown[]) {
  const result = validateWorkflow(
    makeWorkflow({ nodes: nodes as never, edges: edges as never })
  );
  return result.warnings.filter((w) => w.code === "transfer-in-for-each-body");
}

describe("validateWorkflow - transfer inside a For Each body", () => {
  it("warns on transfer-funds reached via the loop handle", () => {
    const nodes = [
      triggerNode(),
      forEachNode("loop-1"),
      actionNode("t1", { actionType: "web3/transfer-funds" }),
    ];
    const edges = [
      edge("e1", "trigger-1", "loop-1"),
      { ...edge("e2", "loop-1", "t1"), sourceHandle: "loop" },
    ];

    const warnings = warningsFor(nodes, edges);
    expect(warnings).toHaveLength(1);
    expect(warnings[0].message).toMatch(/web3\/disburse/);
    expect(warnings[0].message).not.toMatch(
      /prevents|guards against|guardrail/i
    );
  });

  it("warns on transfer-token reached via a legacy (unhandled) edge", () => {
    const nodes = [
      triggerNode(),
      forEachNode("loop-1"),
      actionNode("t1", { actionType: "web3/transfer-token" }),
    ];
    const edges = [
      edge("e1", "trigger-1", "loop-1"),
      edge("e2", "loop-1", "t1"),
    ];

    expect(warningsFor(nodes, edges)).toHaveLength(1);
  });

  it("does not warn on the done branch, only the loop branch", () => {
    const nodes = [
      triggerNode(),
      forEachNode("loop-1"),
      actionNode("t1", { actionType: "web3/transfer-funds" }),
    ];
    const edges = [
      edge("e1", "trigger-1", "loop-1"),
      { id: "e2", source: "loop-1", target: "t1", sourceHandle: "done" },
    ];

    expect(warningsFor(nodes, edges)).toHaveLength(0);
  });

  it("does not warn on web3/disburse itself inside a For Each", () => {
    const nodes = [
      triggerNode(),
      forEachNode("loop-1"),
      actionNode("d1", { actionType: "web3/disburse" }),
    ];
    const edges = [
      edge("e1", "trigger-1", "loop-1"),
      edge("e2", "loop-1", "d1"),
    ];

    expect(warningsFor(nodes, edges)).toHaveLength(0);
  });

  it("does not warn when there is no For Each at all", () => {
    const nodes = [
      triggerNode(),
      actionNode("t1", { actionType: "web3/transfer-funds" }),
    ];
    const edges = [edge("e1", "trigger-1", "t1")];

    expect(warningsFor(nodes, edges)).toHaveLength(0);
  });

  it("warns once per transfer node even several steps deep in the body", () => {
    const nodes = [
      triggerNode(),
      forEachNode("loop-1"),
      actionNode("cond-1", { actionType: "Condition" }),
      actionNode("t1", { actionType: "web3/transfer-funds" }),
    ];
    const edges = [
      edge("e1", "trigger-1", "loop-1"),
      edge("e2", "loop-1", "cond-1"),
      edge("e3", "cond-1", "t1"),
    ];

    expect(warningsFor(nodes, edges)).toHaveLength(1);
  });
});
