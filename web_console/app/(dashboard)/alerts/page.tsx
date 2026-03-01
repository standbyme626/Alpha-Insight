import { AlertsPanel } from "@/components/alerts-panel";
import { frontendClient } from "@/lib/client";

export default async function AlertsPage() {
  const rows = await frontendClient.listAlerts(120);
  const governance = await frontendClient.listGovernance();
  const activeDegradeStates = governance.filter((row) => row.status === "active").length;
  return <AlertsPanel rows={rows} activeDegradeStates={activeDegradeStates} />;
}
