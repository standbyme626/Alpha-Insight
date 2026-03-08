import { NextRequest, NextResponse } from "next/server";

import { listEvents } from "@/lib/resources";

export async function GET(request: NextRequest) {
  const limit = Number(request.nextUrl.searchParams.get("limit") || "200");
  const since = request.nextUrl.searchParams.get("since");

  const rows = await listEvents({
    limit: Number.isFinite(limit) ? limit : 200,
    since
  });
  return NextResponse.json(rows);
}
