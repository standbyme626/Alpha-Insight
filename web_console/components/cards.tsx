export function StatCard({ label, value, detail }: { label: string; value: string | number; detail?: string }) {
  return (
    <article className="stat-card">
      <p className="stat-label">{label}</p>
      <p className="stat-value">{value}</p>
      {detail ? <p className="stat-detail">{detail}</p> : null}
    </article>
  );
}

export function SectionTitle({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <header className="section-title">
      <h2>{title}</h2>
      <p>{subtitle}</p>
    </header>
  );
}
