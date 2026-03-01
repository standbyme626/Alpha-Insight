import { NextRequest, NextResponse } from "next/server";

import { listRuns } from "@/lib/resources";

export async function GET(request: NextRequest) {
  const limit = Number(request.nextUrl.searchParams.get("limit") || "50");
  const rows = await listRuns(Number.isFinite(limit) ? limit : 50);
  return NextResponse.json(rows);
}
