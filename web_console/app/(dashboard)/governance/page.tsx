import { GovernancePanel } from "@/components/governance-panel";
import { frontendClient } from "@/lib/client";

export default async function GovernancePage() {
  const rows = await frontendClient.listGovernance();
  return <GovernancePanel rows={rows} />;
}
