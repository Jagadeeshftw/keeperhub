/**
 * web3/disburse: registration, and every refusal that has to happen before the
 * ledger is touched. The ledger mock throws if it is reached, so a refusal that
 * leaked past validation fails loudly rather than being counted as a pass.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

vi.mock("@/lib/logging", () => ({
  ErrorCategory: { DATABASE: "database", VALIDATION: "validation" },
  logSystemError: vi.fn(),
  logUserError: vi.fn(),
}));

const ledgerReached = vi.hoisted(() => vi.fn());
vi.mock("@/lib/web3/disbursement-ledger", () => {
  const reached = (...args: unknown[]) => {
    ledgerReached(...args);
    return Promise.reject(new Error("the ledger must not be reached"));
  };
  return {
    readRunLegs: reached,
    readReceiptRecords: reached,
    claimNewLeg: reached,
    reclaimLeg: reached,
    recordBroadcastEvent: reached,
    recordOutcome: reached,
    applyReceiptVerdict: reached,
  };
});

vi.mock("@/lib/execute/value-ledger", () => ({
  withStepValueCap: (_args: unknown, run: () => unknown) => run(),
}));
vi.mock("@/lib/safe/signer-resolver", () => ({
  SIGNER_MODE: { EOA: "eoa", SAFE: "safe", SAFE_ROLE: "safe-role" },
  resolveSignerForNode: () => Promise.resolve({ kind: "eoa" }),
}));
vi.mock("@/lib/web3/resolve-org-context", () => ({
  resolveOrganizationContext: () =>
    Promise.resolve({ success: true, organizationId: "org-1" }),
}));
vi.mock("@/plugins/web3/steps/transfer-token-core", () => ({
  transferTokenCore: vi.fn(),
}));
vi.mock("@/plugins/web3/steps/transfer-funds-core", () => ({
  transferFundsCore: vi.fn(),
}));
vi.mock("@/plugins/web3/steps/transfer-spl-token-core", () => ({
  transferSplTokenCore: vi.fn(),
}));

import { findActionById, flattenConfigFields } from "@/plugins/registry";
import {
  type DisburseCoreInput,
  disburseCore,
} from "@/plugins/web3/steps/disburse-core";

const EVM_RECIPIENT = "0x106175F175B940CcA1816d75eB19937a88BE7720";
const SOL_RECIPIENT = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM";
const USDC_BASE_SEPOLIA = "0x036CbD53842c5426634e7929541eC2318f3dCF7e";
const USDC_SOLANA_DEVNET = "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU";

function input(overrides: Partial<DisburseCoreInput>): DisburseCoreInput {
  return {
    network: "base-sepolia",
    assetType: "native",
    runKey: "payroll",
    legs: [{ recipient: EVM_RECIPIENT, amount: "1" }],
    _context: {
      organizationId: "org-1",
      nodeId: "disburse-1",
      nodeName: "Disburse",
      nodeType: "web3/disburse",
    },
    ...overrides,
  };
}

async function refused(overrides: Partial<DisburseCoreInput>): Promise<string> {
  const result = await disburseCore(input(overrides));
  expect(result.success).toBe(false);
  expect(ledgerReached).not.toHaveBeenCalled();
  return result.success ? "" : result.error;
}

beforeEach(() => {
  ledgerReached.mockClear();
});

describe("web3/disburse registration", () => {
  const action = findActionById("web3/disburse");

  it("is registered as a Web3 write action with explicit egress", () => {
    expect(action).toMatchObject({
      slug: "disburse",
      category: "Web3",
      requiresCredentials: true,
      egress: "fixed-host",
      stepFunction: "disburseStep",
      stepImportPath: "disburse",
    });
  });

  it("requires a run key and a leg list", () => {
    const fields = flattenConfigFields(action?.configFields ?? []);
    const byKey = new Map(fields.map((f) => [f.key, f]));
    expect(byKey.get("runKey")?.required).toBe(true);
    expect(byKey.get("legs")?.required).toBe(true);
    expect(byKey.get("network")).toMatchObject({
      type: "chain-select",
      chainTypeFilter: ["evm", "solana"],
    });
    // No private-mempool variants and no Web3 Connection field: both select
    // send paths the pre-broadcast record does not cover.
    expect(byKey.get("network")?.showPrivateVariants).toBeUndefined();
    expect(byKey.has("web3Connection")).toBe(false);
    expect(byKey.has("usePrivateMempool")).toBe(false);
  });
});

describe("web3/disburse chain and asset exclusions", () => {
  it("refuses ERC-20 legs on Solana", async () => {
    expect(
      await refused({
        network: "solana-devnet",
        assetType: "erc20",
        tokenAddress: USDC_BASE_SEPOLIA,
        legs: [{ recipient: SOL_RECIPIENT, amount: "1" }],
      })
    ).toMatch(/ERC-20 legs need an EVM network/);
  });

  it("refuses SPL legs on every EVM chain", async () => {
    for (const network of [
      "ethereum",
      "sepolia",
      "base",
      "base-sepolia",
      "tempo",
    ]) {
      expect(
        await refused({ network, assetType: "spl", mint: USDC_SOLANA_DEVNET })
      ).toMatch(/SPL legs need a Solana network/);
    }
  });

  it("refuses an EVM recipient on Solana and a Solana recipient on EVM", async () => {
    expect(
      await refused({
        network: "solana-devnet",
        legs: [{ recipient: EVM_RECIPIENT, amount: "1" }],
      })
    ).toMatch(/not a valid Solana address/);
    expect(
      await refused({ legs: [{ recipient: SOL_RECIPIENT, amount: "1" }] })
    ).toMatch(/not a valid EVM address/);
  });

  it("refuses an unknown asset type, a bad token and a bad mint", async () => {
    expect(await refused({ assetType: "erc721" })).toMatch(/Asset type/);
    expect(
      await refused({ assetType: "erc20", tokenAddress: "0x1234" })
    ).toMatch(/valid ERC-20 token address/);
    expect(
      await refused({
        network: "solana-devnet",
        assetType: "spl",
        mint: "not-base58!",
        legs: [{ recipient: SOL_RECIPIENT, amount: "1" }],
      })
    ).toMatch(/not a valid Solana address/);
  });

  it("refuses a missing network, run key or legs", async () => {
    expect(await refused({ network: "" })).toMatch(/Network is required/);
    expect(await refused({ runKey: "  " })).toMatch(/run key is required/);
    expect(await refused({ legs: "[]" })).toMatch(/non-empty/);
  });
});
