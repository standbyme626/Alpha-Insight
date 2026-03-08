import { NextRequest, NextResponse } from "next/server";

import { listMonitors } from "@/lib/resources";

export async function GET(request: NextRequest) {
  const limit = Number(request.nextUrl.searchParams.get("limit") || "200");
  const rows = await listMonitors(Number.isFinite(limit) ? limit : 200);
  return NextResponse.json(rows);
}
