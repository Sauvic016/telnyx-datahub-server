import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";

interface IRecordFilter {
  botId?: number;
  listName?: string;
  isNewDataSection?: boolean;
  startDate?: string;
  endDate?: string;
}

const recordGetter = async (
  page: number,
  limit: number,
  recordTab: "all" | "clean" | "incomplete" = "all",
  filter?: IRecordFilter
) => {
  const skip = (page - 1) * limit;

  const pipeline: any[] = [];

  /* --------------------------------------------------
     1️⃣ OWNER-LEVEL FILTERS (ONLY OWNER FIELDS)
  -------------------------------------------------- */

  const ownerMatch: any = {};

  if (filter?.botId !== undefined) {
    ownerMatch.botId = filter.botId;
  }

  pipeline.push({ $match: ownerMatch });

  /* --------------------------------------------------
     2️⃣ ONE ROW PER PROPERTY
  -------------------------------------------------- */

  pipeline.push({
    $unwind: {
      path: "$propertyIds",
      preserveNullAndEmptyArrays: false,
    },
  });

  /* --------------------------------------------------
     3️⃣ LOOKUP PROPERTY DATA
  -------------------------------------------------- */

  pipeline.push({
    $lookup: {
      from: "propertydatas",
      localField: "propertyIds",
      foreignField: "_id",
      as: "property",
    },
  });

  pipeline.push({
    $unwind: {
      path: "$property",
      preserveNullAndEmptyArrays: false,
    },
  });

  /* --------------------------------------------------
     4️⃣ CLEAN / INCOMPLETE LOGIC (✔️ CORRECT PLACE)
  -------------------------------------------------- */

  if (recordTab === "clean") {
    pipeline.push({
      $match: {
        clean: { $eq: true }, // Owner.clean
        "property.clean": { $eq: true }, // PropertyData.clean
      },
    });
  }

  if (recordTab === "incomplete") {
    pipeline.push({
      $match: {
        $or: [{ clean: { $ne: true } }, { "property.clean": { $ne: true } }],
      },
    });
  }

  /* --------------------------------------------------
     5️⃣ PROPERTY-LEVEL FILTERS
  -------------------------------------------------- */

  const propertyMatch: any = {};

  if (filter?.listName) {
    propertyMatch["property.currList"] = {
      $elemMatch: { name: filter.listName },
    };
  }

  if (filter?.startDate || filter?.endDate) {
    propertyMatch["property.syncedAt"] = {};
    if (filter.startDate) {
      propertyMatch["property.syncedAt"].$gte = new Date(filter.startDate);
    }
    if (filter.endDate) {
      propertyMatch["property.syncedAt"].$lte = new Date(filter.endDate);
    }
  }

  if (Object.keys(propertyMatch).length > 0) {
    pipeline.push({ $match: propertyMatch });
  }

  /* --------------------------------------------------
     6️⃣ SORTING
  -------------------------------------------------- */

  if (filter?.isNewDataSection && filter?.listName) {
    pipeline.push({
      $addFields: {
        sortKey: {
          $let: {
            vars: {
              targetList: {
                $arrayElemAt: [
                  {
                    $filter: {
                      input: { $ifNull: ["$property.currList", []] },
                      as: "item",
                      cond: { $eq: ["$$item.name", filter.listName] },
                    },
                  },
                  0,
                ],
              },
            },
            in: "$$targetList.list_updated_at",
          },
        },
      },
    });
    pipeline.push({ $sort: { sortKey: -1 } });
  } else if (filter?.isNewDataSection) {
    pipeline.push({
      $addFields: {
        sortKey: { $max: "$property.currList.list_updated_at" },
      },
    });
    pipeline.push({ $sort: { sortKey: -1 } });
  } else {
    pipeline.push({ $sort: { "property.updatedAt": -1 } });
  }

  /* --------------------------------------------------
     7️⃣ FINAL SHAPE
  -------------------------------------------------- */

  pipeline.push({
    $project: {
      identityKey: 1,
      owner_first_name: 1,
      owner_last_name: 1,
      company_name_full_name: 1,
      mailing_address: 1,
      stage: 1,
      decision: 1,
      mailing_city: 1,
      mailing_state: 1,
      mailing_zip_code: 1,
      botId: 1,
      clean: 1,
      inPostgres: 1,
      property: 1,
    },
  });

  /* --------------------------------------------------
     8️⃣ PAGINATION + TOTAL COUNT
  -------------------------------------------------- */

  pipeline.push({
    $facet: {
      data: [{ $skip: skip }, { $limit: limit }],
      totalCount: [{ $count: "count" }],
    },
  });

  /* --------------------------------------------------
     9️⃣ EXECUTE
  -------------------------------------------------- */

  const result = await Owner.aggregate(pipeline);

  const items = result[0]?.data ?? [];
  const total = result[0]?.totalCount?.[0]?.count ?? 0;

  return { items, total };
};

export default recordGetter;
