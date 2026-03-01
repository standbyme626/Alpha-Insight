import { NextRequest, NextResponse } from "next/server";

import { listAlerts } from "@/lib/resources";

export async function GET(request: NextRequest) {
  const limit = Number(request.nextUrl.searchParams.get("limit") || "100");
  const rows = await listAlerts(Number.isFinite(limit) ? limit : 100);
  return NextResponse.json(rows);
}
