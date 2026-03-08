export type NavItem = {
  label: string;
  href: string;
  description: string;
};

export type NavSection = {
  title: string;
  items: NavItem[];
};

export const navSections: NavSection[] = [
  {
    title: "Execution",
    items: [
      {
        label: "Runs",
        href: "/runs",
        description: "Execution history"
      },
      {
        label: "Alerts",
        href: "/alerts",
        description: "Channel delivery states"
      },
      {
        label: "Monitors",
        href: "/monitors",
        description: "Watch jobs and next runs"
      }
    ]
  },
  {
    title: "Reliability",
    items: [
      {
        label: "Governance",
        href: "/governance",
        description: "Degrade/recover states"
      },
      {
        label: "Evidence",
        href: "/evidence",
        description: "Acceptance artifacts"
      }
    ]
  }
];

export const navItems: NavItem[] = navSections.flatMap((section) => section.items);
