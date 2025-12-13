import prisma from "../db";
import { getFlowNames, makeIdentityKey } from "../utils/helper";
import { DirectSkipStatus, RowDecisionStatus } from "../generated/prisma/enums";
import { ScrappedData } from "../models/ScrappedData";
import { BOTMAP } from "../utils/constants";
import { RowStatus } from "../types/records";

// export interface JobCheckResult {
//   jobId: number;
//   startedByBot: string;
//   flow: string[];
//   records: RowStatus[];
// }

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

/**
 * Fetch data from MongoDB and check against contacts in Postgres
 */
interface IRecordFilter {
  botId?: number;
  listName?: string;
  isListChanged?: boolean;
}
const recordGetter = async (
  page: number,
  limit: number,
  recordTab?: "all" | "clean" | "incomplete",
  filter?: IRecordFilter
) => {
  const skip = (page - 1) * limit;

  // Build match object dynamically
  const match: any = {};
  if (filter?.botId !== undefined) match.botId = filter.botId;
  if (filter?.listName) {
    match.currList = { $regex: filter.listName, $options: "i" };
  }
  if (filter?.isListChanged !== undefined) match.isListChanged = filter.isListChanged;

  const pipeline: any = [];

  if (recordTab === "incomplete") {
    pipeline.push({
      $match: {
        clean: false,
      },
    });
  } else if (recordTab === "clean") {
    pipeline.push({
      $match: {
        clean: true,
      },
    });
  }

  if (Object.keys(match).length > 0) {
    pipeline.push({ $match: match });
  }

  pipeline.push({
    $facet: {
      data: [{ $sort: { updatedAt: -1 } }, { $skip: skip }, { $limit: limit }],
      totalCount: [{ $count: "count" }],
    },
  });

  const result = await ScrappedData.aggregate(pipeline);

  const items = result[0].data;
  const total = result[0].totalCount[0]?.count || 0;

  return { items, total };
};
export default recordGetter;
