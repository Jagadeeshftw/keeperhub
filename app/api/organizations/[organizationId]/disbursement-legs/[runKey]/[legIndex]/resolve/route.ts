import { NextResponse } from "next/server";

const LEG_INDEX_PATTERN = /^\d+$/;

import { ErrorCategory, logSystemError } from "@/lib/logging";
import { SCOPE_MCP_WRITE } from "@/lib/mcp/oauth-scopes";
import { getDualAuthContext } from "@/lib/middleware/auth-helpers";
import { requireScope } from "@/lib/middleware/require-scope";
import { buildAuditMetadata, recordAuditEvent } from "@/lib/security/audit-log";
import { resolveLeg } from "@/lib/web3/disbursement-ledger";

/**
 * Operator recovery for a disburse leg the platform cannot resolve on its
 * own: `unknown` (may have paid), or `sending` whose run is presumed dead.
 * Everything else settles or stays failed from the run itself; this route
 * exists for the state neither our filed plan nor the reported issue offers a
 * way out of.
 *
 * Same authorization shape as
 * organizations/[organizationId]/execution-digest: dual auth (session or an
 * API-key/OAuth caller hard-scoped to its own org), gated by mcp:write since
 * this moves a leg toward being sent again.
 */

type Body = {
  outcome?: unknown;
  transactionHash?: unknown;
  note?: unknown;
};

function parseBody(raw: unknown): {
  outcome: "paid" | "not_paid";
  transactionHash?: string;
  note: string;
} | null {
  const body = raw as Body | null;
  if (body === null || typeof body !== "object") {
    return null;
  }
  if (body.outcome !== "paid" && body.outcome !== "not_paid") {
    return null;
  }
  if (typeof body.note !== "string") {
    return null;
  }
  const transactionHash =
    typeof body.transactionHash === "string" ? body.transactionHash : undefined;
  return { outcome: body.outcome, transactionHash, note: body.note };
}

export async function POST(
  request: Request,
  context: {
    params: Promise<{
      organizationId: string;
      runKey: string;
      legIndex: string;
    }>;
  }
): Promise<NextResponse> {
  const {
    organizationId: organizationIdParam,
    runKey,
    legIndex: legIndexRaw,
  } = await context.params;
  if (!LEG_INDEX_PATTERN.test(legIndexRaw)) {
    return NextResponse.json({ error: "Invalid leg index" }, { status: 400 });
  }
  const legIndex = Number.parseInt(legIndexRaw, 10);

  const authContext = await getDualAuthContext(request);
  if ("error" in authContext) {
    return NextResponse.json(
      { error: authContext.error },
      { status: authContext.status }
    );
  }
  const {
    userId,
    organizationId: callerOrgId,
    authMethod,
    scope,
    apiKeyId,
  } = authContext;
  if (!userId) {
    return NextResponse.json(
      { error: "Auth context missing user" },
      { status: 400 }
    );
  }
  // An MCP tool authenticates as one org and has no way to name its own id --
  // no MCP surface exposes it, by design, so an agent can never pass another
  // org's id in an argument. "self" lets a credential-authenticated caller
  // resolve itself without that path. A session caller, who may belong to
  // several orgs, must always name the org explicitly.
  let organizationId = organizationIdParam;
  if (organizationIdParam === "self") {
    if (authMethod === "session" || !callerOrgId) {
      return NextResponse.json(
        { error: "'self' requires an API-key or OAuth credential" },
        { status: 400 }
      );
    }
    organizationId = callerOrgId;
  } else if (authMethod !== "session" && callerOrgId !== organizationId) {
    // API-key and OAuth callers are hard-scoped to the org they authenticated
    // as; a session caller's org membership is not checked here on purpose --
    // resolving a leg is an operator action on a specific run, not an
    // org-settings change, so any member (not only an owner/admin) may do it.
    return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  }

  const scopeError = requireScope(scope, SCOPE_MCP_WRITE, {
    organizationId,
    credentialId: apiKeyId ?? undefined,
    credentialType: authMethod,
    endpoint:
      "/api/organizations/[organizationId]/disbursement-legs/[runKey]/[legIndex]/resolve",
  });
  if (scopeError) {
    return scopeError;
  }

  let raw: unknown;
  try {
    raw = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }
  const body = parseBody(raw);
  if (!body) {
    return NextResponse.json(
      {
        error:
          'Body must be {"outcome": "paid" | "not_paid", "note": string, "transactionHash"?: string}',
      },
      { status: 400 }
    );
  }

  try {
    const result = await resolveLeg({
      organizationId,
      runKey,
      legIndex,
      outcome: body.outcome,
      transactionHash: body.transactionHash,
      note: body.note,
      userId,
    });

    if (!result.ok) {
      const STATUS_BY_CODE: Record<typeof result.code, number> = {
        not_found: 404,
        invalid: 400,
        not_resolvable: 409,
      };
      return NextResponse.json(
        { error: result.error },
        { status: STATUS_BY_CODE[result.code] }
      );
    }

    await recordAuditEvent({
      actor: {
        userId,
        organizationId,
        authMethod,
        actorLabel: userId,
      },
      action: "disburse.leg.resolve",
      resourceType: "disbursement_leg",
      resourceId: `${organizationId}:${runKey}:${legIndex}`,
      after: { status: result.leg.status, outcome: body.outcome },
      metadata: buildAuditMetadata(request),
    });

    return NextResponse.json({ leg: result.leg });
  } catch (error) {
    logSystemError(
      ErrorCategory.DATABASE,
      "[Disburse] Failed to resolve a leg",
      error,
      { organizationId, runKey, legIndex: String(legIndex) }
    );
    return NextResponse.json(
      { error: "Failed to resolve the leg" },
      { status: 500 }
    );
  }
}
