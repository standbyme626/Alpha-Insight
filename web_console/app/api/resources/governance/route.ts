import { NextResponse } from "next/server";

import { listGovernance } from "@/lib/resources";

export async function GET() {
  const rows = await listGovernance();
  return NextResponse.json(rows);
}
