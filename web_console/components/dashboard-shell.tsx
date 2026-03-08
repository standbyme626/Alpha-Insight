"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { navSections } from "@/components/nav";

const titleMap: Record<string, string> = {
  "/runs": "Runs",
  "/alerts": "Alerts",
  "/monitors": "Monitors",
  "/evidence": "Evidence",
  "/governance": "Governance"
};

export default function DashboardShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const sectionTitle = titleMap[pathname] || "Console";

  return (
    <div className="frame">
      <aside className="rail">
        <div className="brand">
          <p className="eyebrow">Alpha-Insight</p>
          <h1>Upgrade7 Console</h1>
        </div>
        {navSections.map((section) => (
          <nav key={section.title} className="nav-section">
            <p className="nav-section-title">{section.title}</p>
            {section.items.map((item) => {
              const active = pathname === item.href;
              return (
                <Link key={item.href} href={item.href} className={`nav-link ${active ? "active" : ""}`}>
                  <span>{item.label}</span>
                  <small>{item.description}</small>
                </Link>
              );
            })}
          </nav>
        ))}
        <div className="rail-footnote">
          <p>resource-first routes</p>
          <p>runs / alerts / monitors / evidence / governance</p>
        </div>
      </aside>
      <main className="content">
        <header className="content-topbar">
          <div>
            <p className="topbar-eyebrow">Control Plane</p>
            <h2>{sectionTitle}</h2>
          </div>
          <div className="topbar-badge">Next14 + React18 compatible</div>
        </header>
        {children}
      </main>
    </div>
  );
}
