import type { HiveTopLevelView } from "../hive/types";

export interface HiveDrawerItem {
  id: HiveTopLevelView;
  label: string;
  detail: string;
}

/** Primary Hive destinations shown in the mode drawer. Internal ids stay
 * stable for deep links; labels are the current product language. */
export const HIVE_PRIMARY_NAV_ITEMS: HiveDrawerItem[] = [
  {
    id: "crew",
    label: "Workers",
    detail: "Durable identities with private DMs",
  },
  {
    id: "groups",
    label: "Groups",
    detail: "Rooms where Workers collaborate",
  },
  {
    id: "attention",
    label: "Activity",
    detail: "Approvals, wakes, and updates",
  },
  {
    id: "schedule",
    label: "Calendar",
    detail: "Recurring and one-time wakes",
  },
  {
    id: "memory",
    label: "Memory",
    detail: "Shared notes and Worker recall",
  },
];
