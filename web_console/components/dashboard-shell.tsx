"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { navSections } from "@/components/nav";

const titleMap: Record<string, string> = {
  "/runs": "运行记录 Runs",
  "/alerts": "告警中心 Alerts",
  "/monitors": "监控任务 Monitors",
  "/evidence": "验收证据 Evidence",
  "/governance": "治理面板 Governance"
};

export default function DashboardShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const sectionTitle = titleMap[pathname] || "控制台 Console";

  return (
    <div className="frame">
      <aside className="rail">
        <div className="brand">
          <p className="eyebrow">Alpha-Insight</p>
          <h1>升级12 控制台 / Upgrade12 Console</h1>
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
          <p>资源主链路（Resource-first）</p>
          <p>runs / alerts / monitors / evidence / governance</p>
        </div>
      </aside>
      <main className="content">
        <header className="content-topbar">
          <div>
            <p className="topbar-eyebrow">运营控制平面 Control Plane</p>
            <h2>{sectionTitle}</h2>
          </div>
          <div className="topbar-badge">兼容 Next14 + React18</div>
        </header>
        {children}
      </main>
    </div>
  );
}
