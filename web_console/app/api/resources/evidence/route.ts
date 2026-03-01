import { NextRequest, NextResponse } from "next/server";

import { listEvidence } from "@/lib/resources";

export async function GET(request: NextRequest) {
  const limit = Number(request.nextUrl.searchParams.get("limit") || "100");
  const rows = await listEvidence(Number.isFinite(limit) ? limit : 100);
  return NextResponse.json(rows);
}
