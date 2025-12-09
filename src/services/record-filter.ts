import prisma from "../db";

export type RecordFilterParams = {
  listId?: number;
  startedByBot?: string;
  dataType?: "all" | "clean" | "incomplete";
};

export async function filterRecords(
  allRecords: any[],
  cleanRecords: any[],
  incompleteRecords: any[],
  params: RecordFilterParams
) {
  let targetRecords = allRecords;

  // 1. Filter by Data Type
  if (params.dataType === "clean") {
    targetRecords = cleanRecords;
  } else if (params.dataType === "incomplete") {
    targetRecords = incompleteRecords;
  }

  // 2. Filter by List ID
  if (params.listId && !isNaN(params.listId)) {
    const list = await prisma.list.findUnique({
      where: { id: params.listId },
      select: { name: true },
    });

    if (list) {
      targetRecords = targetRecords.filter((record) =>
        record.lists?.includes(list.name)
      );
    } else {
      // List provided but not found -> return empty
      return [];
    }
  }

  // 3. Filter by Started By Bot
  if (params.startedByBot) {
    targetRecords = targetRecords.filter(
      (record) =>
        record.startedByBot?.toLowerCase() === params.startedByBot!.toLowerCase()
    );
  }

  return targetRecords;
}
