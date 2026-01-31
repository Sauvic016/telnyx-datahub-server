import prisma from "../db";
import { BotJobs } from "../generated/prisma/client";
import { IRecordFilter } from "../services/contacts-check";
import { BOTMAP } from "./constants";
import fs from "fs/promises";

export function getFlowNames(botId: number) {
  return BOTMAP[botId].flow?.map((flowId) => BOTMAP[flowId].name ?? "");
}

export const getLatestJobsPerBot = async (): Promise<BotJobs[]> => {
  const grouped = await prisma.botJobs.groupBy({
    by: ["startedByBotId"],
    _max: { updatedAt: true },
  });

  const latest = await Promise.all(
    grouped.map((g) =>
      prisma.botJobs.findFirst({
        where: {
          startedByBotId: g.startedByBotId,
          updatedAt: g._max.updatedAt!,
        },
        orderBy: { updatedAt: "desc" },
      }),
    ),
  );

  const notNull = (item: BotJobs | null): item is BotJobs => item !== null;
  const jobs = latest.filter(notNull);
  // Sort by updatedAt ASC so that we process older jobs first, then newer jobs.
  // This ensures the final state in MongoDB reflects the latest job.
  return jobs.sort((a, b) => a.updatedAt.getTime() - b.updatedAt.getTime());
};

export function makeIdentityKey(first: string, second: string, third: string, fourth?: string): string {
  if (fourth) {
    return `${first.trim().toLowerCase()}|${second.trim().toLowerCase()}|${third.trim().toLowerCase()}|${fourth
      .trim()
      .toLowerCase()}`;
  }
  return `${first.trim().toLowerCase()}|${second.trim().toLowerCase()}|${third.trim().toLowerCase()}`;
}

export const getAllJobs = async (): Promise<BotJobs[]> => {
  return prisma.botJobs.findMany({
    orderBy: { updatedAt: "asc" },
  });
};
export const getAllJobsAfterLastSync = async (): Promise<BotJobs[]> => {
  const lastSyncedAt = new Date("2026-01-07T18:16:33Z");

  return prisma.botJobs.findMany({
    where: {
      updatedAt: {
        gt: lastSyncedAt, // fetch jobs after last sync
      },
    },
    orderBy: {
      updatedAt: "asc",
    },
  });
};

export async function fileExists(filePath: string) {
  try {
    await fs.access(filePath);
    return true; // file exists
  } catch {
    return false; // file does NOT exist
  }
}

export function pickField(doc: any, candidates: string[]): string {
  const normalize = (s: string) => s.trim().toLowerCase().replace(/^"|"$/g, "");

  for (const cand of candidates) {
    const candNorm = normalize(cand);

    // Check for exact match first
    if (doc[cand]) return String(doc[cand]);

    // Check for normalized match
    for (const key of Object.keys(doc)) {
      if (normalize(key) === candNorm) {
        return String(doc[key]);
      }
    }
  }
  return "";
}

interface IDateFilter {
  filterDateType?: string;
  startDate?: string;
  endDate?: string;
}
export function resolveDateRange(filter: IDateFilter) {
  const now = new Date();
  let startDate, endDate;
  try {
    switch (filter?.filterDateType) {
      case "today": {
        startDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0, 0));

        endDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 23, 59, 59, 999));
        break;
      }

      case "yesterday": {
        startDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0, 0));

        endDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 23, 59, 59, 999));
        break;
      }

      case "last48hours": {
        startDate = new Date(now.getTime() - 48 * 60 * 60 * 1000);
        endDate = now;
        break;
      }

      case "this_week": {
        const day = now.getUTCDay() || 7; // Sunday = 7
        startDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - day + 1, 0, 0, 0, 0));
        endDate = now;
        break;
      }

      case "last_week": {
        const day = now.getUTCDay() || 7;

        startDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - day - 6, 0, 0, 0, 0));

        endDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - day, 23, 59, 59, 999));
        break;
      }

      case "this_month": {
        startDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1, 0, 0, 0, 0));
        endDate = now;
        break;
      }

      case "last_month": {
        startDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1, 0, 0, 0, 0));

        endDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 0, 23, 59, 59, 999));
        break;
      }

      case "custom": {
        startDate = new Date(filter.startDate!);
        endDate = new Date(filter.endDate!);

        if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
          throw new Error("Invalid custom date range");
        }

        if (startDate > endDate) {
          throw new Error("Start date cannot be after end date");
        }
        break;
      }

      default:
        throw new Error("Invalid filterDateType");
    }
  } catch (error) {
    console.log(error);
  }

  return { startDate, endDate };
}

export function parseAmPmTime(timeStr: string): Date {
  // Parse "9:00 AM" or "5:30 PM" format to a Date with time component
  const match = timeStr.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
  if (!match) {
    throw new Error(`Invalid time format: ${timeStr}`);
  }

  let hours = parseInt(match[1], 10);
  const minutes = parseInt(match[2], 10);
  const period = match[3].toUpperCase();

  if (period === "PM" && hours !== 12) {
    hours += 12;
  } else if (period === "AM" && hours === 12) {
    hours = 0;
  }

  // Create a date with just time (use epoch date, only time matters for time slots)
  const date = new Date(1970, 0, 1, hours, minutes, 0, 0);
  return date;
}

export function isInTimeSlot(timeSlots: { startTime: Date; endTime: Date }[]): boolean {
  if (timeSlots.length === 0) return true;

  const now = new Date();
  const currentHours = now.getHours();
  const currentMinutes = now.getMinutes();
  const currentTimeMinutes = currentHours * 60 + currentMinutes;

  return timeSlots.some(slot => {
    const startHours = slot.startTime.getHours();
    const startMinutes = slot.startTime.getMinutes();
    const startTimeMinutes = startHours * 60 + startMinutes;

    const endHours = slot.endTime.getHours();
    const endMinutes = slot.endTime.getMinutes();
    const endTimeMinutes = endHours * 60 + endMinutes;

    return currentTimeMinutes >= startTimeMinutes && currentTimeMinutes <= endTimeMinutes;
  });
}
