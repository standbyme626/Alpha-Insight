import "@/app/globals.css";

import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Alpha-Insight Upgrade7 Console",
  description: "Reference-based frontend shell for Upgrade7 resources"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
