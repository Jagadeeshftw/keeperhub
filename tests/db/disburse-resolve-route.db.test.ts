/**
 * POST .../disbursement-legs/[runKey]/[legIndex]/resolve against a real
 * Postgres. Auth, scope and audit are mocked (the shape is the same as
 * execution-digest's route, covered elsewhere); resolveLeg runs its real SQL.
 */

import "dotenv/config";
import { eq } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { afterAll, beforeEach, describe, expect, it, vi } from "vitest";
import { disbursementLegs, organization, users } from "../../lib/db/schema";

vi.mock("server-only", () => ({}));
vi.unmock("@/lib/db");

vi.mock("@/lib/logging", () => ({
  ErrorCategory: { DATABASE: "database" },
  logSystemError: vi.fn(),
}));

const auth = vi.hoisted(() => ({
  context: null as unknown,
}));
vi.mock("@/lib/middleware/auth-helpers", () => ({
  getDualAuthContext: () => Promise.resolve(auth.context),
}));

vi.mock("@/lib/middleware/require-scope", () => ({
  requireScope: () => null,
}));

const audit = vi.hoisted(() => ({ events: [] as unknown[] }));
vi.mock("@/lib/security/audit-log", () => ({
  buildAuditMetadata: () => ({}),
  recordAuditEvent: (event: unknown) => {
    audit.events.push(event);
    return Promise.resolve();
  },
}));

const DATABASE_URL = process.env.DATABASE_URL ?? "";
const queryClient = postgres(DATABASE_URL, { max: 2 });
const testDb = drizzle(queryClient);

const PREFIX = "test_resolveroute_";
const USER = `${PREFIX}user`;
const ORG = `${PREFIX}org`;
const OTHER_ORG = `${PREFIX}org_other`;

async function post(
  runKey: string,
  legIndex: string,
  body: unknown,
  orgId = ORG
) {
  const { POST } = await import(
    "../../app/api/organizations/[organizationId]/disbursement-legs/[runKey]/[legIndex]/resolve/route"
  );
  const req = new Request(
    `http://localhost/api/organizations/${orgId}/disbursement-legs/${runKey}/${legIndex}/resolve`,
    { method: "POST", body: JSON.stringify(body) }
  );
  const res = await POST(req, {
    params: Promise.resolve({ organizationId: orgId, runKey, legIndex }),
  });
  return { status: res.status, body: await res.json() };
}

async function seed(): Promise<void> {
  await testDb
    .insert(users)
    .values({
      id: USER,
      name: "t",
      email: `${USER}@test.local`,
      createdAt: new Date(),
      updatedAt: new Date(),
    })
    .onConflictDoNothing();
  for (const org of [ORG, OTHER_ORG]) {
    await testDb
      .insert(organization)
      .values({ id: org, name: "t", slug: org, createdAt: new Date() })
      .onConflictDoNothing();
  }
  await testDb.insert(disbursementLegs).values({
    organizationId: ORG,
    runKey: "run-1",
    legIndex: 0,
    chainId: 84_532,
    asset: "native",
    recipient: "0x106175f175b940cca1816d75eb19937a88be7720",
    amount: "1",
    status: "unknown",
    claimToken: "t",
  });
}

async function clear(): Promise<void> {
  await testDb
    .delete(disbursementLegs)
    .where(eq(disbursementLegs.organizationId, ORG));
}

beforeEach(async () => {
  await clear();
  await seed();
  audit.events = [];
  auth.context = {
    userId: USER,
    organizationId: ORG,
    authMethod: "api-key",
    apiKeyId: "key-1",
    scope: "mcp:write",
    isAnonymous: false,
  };
});

afterAll(async () => {
  await clear();
  await testDb.delete(organization).where(eq(organization.id, ORG));
  await testDb.delete(organization).where(eq(organization.id, OTHER_ORG));
  await testDb.delete(users).where(eq(users.id, USER));
  await queryClient.end();
});

describe("POST disbursement-legs resolve route (real database)", () => {
  it("resolves an unknown leg as paid and audits it", async () => {
    const { status, body } = await post("run-1", "0", {
      outcome: "paid",
      transactionHash: "0xabc",
      note: "Found the transfer on Basescan",
    });

    expect(status).toBe(200);
    expect(body.leg).toMatchObject({
      status: "settled",
      transactionHash: "0xabc",
    });
    expect(audit.events).toEqual([
      expect.objectContaining({
        action: "disburse.leg.resolve",
        resourceId: `${ORG}:run-1:0`,
      }),
    ]);
  });

  it("400s on a missing note, and does not audit", async () => {
    const { status, body } = await post("run-1", "0", { outcome: "paid" });
    expect(status).toBe(400);
    expect(body.error).toMatch(/paid.*note|note/i);
    expect(audit.events).toEqual([]);
  });

  it("409s on a leg that is not resolvable", async () => {
    await testDb
      .update(disbursementLegs)
      .set({ status: "settled" })
      .where(eq(disbursementLegs.runKey, "run-1"));
    const { status } = await post("run-1", "0", {
      outcome: "not_paid",
      note: "n",
    });
    expect(status).toBe(409);
  });

  it("404s on an unknown run or leg", async () => {
    const { status } = await post("no-such-run", "0", {
      outcome: "not_paid",
      note: "n",
    });
    expect(status).toBe(404);
  });

  it("400s on a negative or non-numeric leg index, before touching the database", async () => {
    for (const bad of ["-1", "abc", "1.5"]) {
      const { status } = await post("run-1", bad, {
        outcome: "not_paid",
        note: "n",
      });
      expect(status).toBe(400);
    }
  });

  it("refuses an API key resolving a leg in a different organization", async () => {
    const { status } = await post(
      "run-1",
      "0",
      { outcome: "not_paid", note: "n" },
      OTHER_ORG
    );
    expect(status).toBe(403);
  });

  it("resolves 'self' to the credential's own org for an API-key caller", async () => {
    const { status, body } = await post(
      "run-1",
      "0",
      { outcome: "not_paid", note: "n" },
      "self"
    );
    expect(status).toBe(200);
    expect(body.leg.organizationId).toBe(ORG);
  });

  it("refuses 'self' for a session caller", async () => {
    auth.context = {
      userId: USER,
      organizationId: null,
      authMethod: "session",
      apiKeyId: null,
      scope: undefined,
      isAnonymous: false,
    };
    const { status } = await post(
      "run-1",
      "0",
      { outcome: "not_paid", note: "n" },
      "self"
    );
    expect(status).toBe(400);
  });

  it("401s when authentication fails", async () => {
    auth.context = { error: "Unauthorized", status: 401 };
    const { status } = await post("run-1", "0", {
      outcome: "not_paid",
      note: "n",
    });
    expect(status).toBe(401);
  });
});
