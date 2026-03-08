import { SectionTitle, StatCard } from "@/components/cards";
import { EvidenceTable } from "@/components/tables";
import { frontendClient } from "@/lib/client";

export default async function EvidencePage() {
  const rows = await frontendClient.listEvidence(150);
  const p1Files = rows.filter((row) => row.name.startsWith("upgrade7_")).length;
  const totalBytes = rows.reduce((acc, row) => acc + row.size_bytes, 0);

  return (
    <section className="panel">
      <SectionTitle title="验收证据 Evidence" subtitle="验收产物与可复现追踪记录" />
      <div className="stats-grid">
        <StatCard label="文件数 Files" value={rows.length} />
        <StatCard label="Upgrade7 证据文件" value={p1Files} />
        <StatCard label="总大小 Total Size" value={`${(totalBytes / 1024).toFixed(1)} KB`} />
      </div>
      <EvidenceTable rows={rows} />
    </section>
  );
}
