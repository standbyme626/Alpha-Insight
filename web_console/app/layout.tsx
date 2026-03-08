import "@/app/globals.css";

import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Alpha-Insight 控制台（升级12）",
  description: "升级12控制台：资源主链路、事件流补强与治理视图"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="zh-CN">
      <body>{children}</body>
    </html>
  );
}
