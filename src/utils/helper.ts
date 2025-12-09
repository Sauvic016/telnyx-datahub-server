import prisma from "../db";
import { BotJobs } from "../generated/prisma/client";
import { BOTMAP } from "./constants";

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

  return latest.filter(notNull);
};

export function makeIdentityKey(first: string, last: string, addr: string): string {
  return `${first.trim().toLowerCase()}|${last.trim().toLowerCase()}|${addr.trim().toLowerCase()}`;
}

export const getAllJobs = async (): Promise<BotJobs[]> => {
  return prisma.botJobs.findMany({
    orderBy: { updatedAt: "desc" },
  });
};
