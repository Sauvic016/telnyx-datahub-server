import prisma from "../db";
import { BotJobs } from "../generated/prisma/client";
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
      })
    )
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
