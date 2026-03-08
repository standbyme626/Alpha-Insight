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
    title: "执行与运行",
    items: [
      {
        label: "运行记录 Runs",
        href: "/runs",
        description: "执行历史与关键指标"
      },
      {
        label: "告警中心 Alerts",
        href: "/alerts",
        description: "通道投递状态与异常"
      },
      {
        label: "监控任务 Monitors",
        href: "/monitors",
        description: "监控任务与下次执行"
      }
    ]
  },
  {
    title: "可靠性治理",
    items: [
      {
        label: "治理面板 Governance",
        href: "/governance",
        description: "降级/恢复状态与时间线"
      },
      {
        label: "验收证据 Evidence",
        href: "/evidence",
        description: "验收产物与可追溯记录"
      }
    ]
  }
];

export const navItems: NavItem[] = navSections.flatMap((section) => section.items);
