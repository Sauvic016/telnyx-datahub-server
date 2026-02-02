import { Owner } from "../models/Owner";
import { resolveDateRange } from "../utils/helper";

export interface IRecordFilter {
  botId?: number;
  listName?: string;
  filterDateType?: string;
  startDate?: string;
  endDate?: string;
  sortBy?: string;
  sortOrder?: string;
}

const recordGetter = async (
  startIndex: number,
  limit: number,
  pageType: "Records" | "Newdata" | "Decision" = "Records",
  recordTab: "all" | "clean" | "incomplete" = "all",
  filter?: IRecordFilter,
  excludedIds?: string[],
) => {
  console.log("\n========== [recordGetter] START ==========");
  console.log("[recordGetter] Input params:", {
    startIndex,
    limit,
    pageType,
    recordTab,
    filter: JSON.stringify(filter, null, 2),
    excludedIdsCount: excludedIds?.length ?? 0,
    excludedIds: excludedIds,
  });

  const skip = startIndex * limit;
  console.log("[recordGetter] Pagination: skip =", skip, ", limit =", limit);

  const pipeline: any[] = [];

  // Resolve date range from filter
  let startDate, endDate;
  if (filter?.filterDateType) {
    const result = resolveDateRange(filter);
    startDate = result.startDate;
    endDate = result.endDate;
    console.log("[recordGetter] Date filter resolved:", { filterDateType: filter.filterDateType, startDate, endDate });
  } else {
    console.log("[recordGetter] No date filter applied");
  }

  /* --------------------------------------------------
     1️⃣ OWNER-LEVEL FILTERS (ONLY OWNER FIELDS)
  -------------------------------------------------- */

  const ownerMatch: any = {};

  if (filter?.botId !== undefined) {
    ownerMatch.botId = filter.botId;
  }

  if (pageType === "Newdata") {
    ownerMatch.$or = [{ decision: { $exists: false } }, { decision: { $ne: "APPROVED" } }];
  }

  if (Object.keys(ownerMatch).length > 0) {
    pipeline.push({ $match: ownerMatch });
  }

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

  pipeline.push({
    $group: {
      _id: {
        ownerId: "$_id",
        propertyId: "$property._id",
      },
      doc: { $first: "$$ROOT" },
    },
  });

  pipeline.push({
    $replaceRoot: { newRoot: "$doc" },
  });

  /* --------------------------------------------------
     3.5️⃣ EXCLUDE SPECIFIC OWNER-PROPERTY COMBINATIONS
  -------------------------------------------------- */

  if (excludedIds && excludedIds.length > 0) {
    // excludedIds format: "ownerId_propertyId"
    console.log("[recordGetter] Processing excludedIds:", excludedIds.length, "items");
    const { ObjectId } = require("mongoose").Types;
    const excludedCombinations = excludedIds.map((id) => {
      const [ownerId, propertyId] = id.split("_");
      console.log("[recordGetter] Excluding:", { raw: id, ownerId, propertyId });
      return {
        $and: [
          { _id: new ObjectId(ownerId) },
          { "property._id": new ObjectId(propertyId) },
        ],
      };
    });

    console.log("[recordGetter] $nor exclusion stage added with", excludedCombinations.length, "combinations");
    pipeline.push({
      $match: {
        $nor: excludedCombinations,
      },
    });
  } else {
    console.log("[recordGetter] No excludedIds to process");
  }

  /* --------------------------------------------------
   3.6️⃣ ADD MOST RECENT LIST UPDATE FIELD
-------------------------------------------------- */

  pipeline.push({
    $addFields: {
      mostRecentListUpdate: {
        $max: {
          $map: {
            input: { $ifNull: ["$property.currList", []] },
            as: "item",
            in: "$$item.list_updated_at",
          },
        },
      },
    },
  });

  /* --------------------------------------------------
     4️⃣ CLEAN / INCOMPLETE LOGIC
  -------------------------------------------------- */

  if (recordTab === "clean") {
    pipeline.push({
      $match: {
        clean: { $eq: true },
        "property.clean": { $eq: true },
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
    if (filter?.listName === "all-lists") {
      propertyMatch["property.currList"] = {
        $exists: true,
        $not: { $size: 0 },
      };
    } else {
      propertyMatch["property.currList"] = {
        $elemMatch: { name: filter.listName },
      };
    }
  }

  // Use resolved dates from resolveDateRange function
  // if (startDate || endDate) {
  //   propertyMatch["property.updatedAt"] = {};

  //   // Check if it's a single day (same date, ignoring time)
  //   if (startDate && endDate) {
  //     // Don't check for same day - use the dates as sent from frontend
  //     propertyMatch["property.updatedAt"].$gte = startDate;
  //     propertyMatch["property.updatedAt"].$lte = endDate;
  //   } else if (startDate) {
  //     propertyMatch["property.updatedAt"].$gte = startDate;
  //   } else if (endDate) {
  //     propertyMatch["property.updatedAt"].$lte = endDate;
  //   }
  // }

  // Use resolved dates from resolveDateRange function
  if (startDate || endDate) {
    propertyMatch["mostRecentListUpdate"] = {};

    if (startDate && endDate) {
      propertyMatch["mostRecentListUpdate"].$gte = startDate;
      propertyMatch["mostRecentListUpdate"].$lte = endDate;
    } else if (startDate) {
      propertyMatch["mostRecentListUpdate"].$gte = startDate;
    } else if (endDate) {
      propertyMatch["mostRecentListUpdate"].$lte = endDate;
    }
  }

  if (Object.keys(propertyMatch).length > 0) {
    console.log("[recordGetter] Property match filters:", JSON.stringify(propertyMatch, null, 2));
    pipeline.push({ $match: propertyMatch });
  } else {
    console.log("[recordGetter] No property match filters applied");
  }

  /* --------------------------------------------------
     6️⃣ SORTING
  -------------------------------------------------- */

  // Convert sortOrder to MongoDB format: 'asc' -> 1, 'desc' -> -1, default to -1
  const sortOrderValue = filter?.sortOrder === "asc" ? 1 : -1;

  // Special case: Newdata page with listName filter
  if (pageType === "Newdata" && filter?.listName) {
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
    // Add secondary sort keys (_id, property._id) for stable, deterministic ordering
    pipeline.push({ $sort: { sortKey: sortOrderValue, _id: 1, "property._id": 1 } });
  } else {
    // Default sorting: use sortBy field or default to mostRecentListUpdate
    let sortField = "mostRecentListUpdate"; // default

    if (filter?.sortBy) {
      if (filter.sortBy === "list_updatedAt") {
        sortField = "mostRecentListUpdate";
      } else if (filter.sortBy === "Name") {
        sortField = "owner_first_name";
      } else if (filter.sortBy === "Address") {
        sortField = "mailing_address";
      } else if (filter.sortBy === "last_sale_date") {
        // Property fields need the "property." prefix
        sortField = "property.last_sale_date";
      } else {
        // Use sortBy value as-is for other fields
        sortField = filter.sortBy;
      }
    }

    console.log("[recordGetter] Sorting debug:", {
      sortBy: filter?.sortBy,
      sortField,
      sortOrderValue,
    });

    // Add secondary sort keys (_id, property._id) for stable, deterministic ordering
    pipeline.push({ $sort: { [sortField]: sortOrderValue, _id: 1, "property._id": 1 } });
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

  console.log("[recordGetter] Executing pipeline with", pipeline.length, "stages");

  const result = await Owner.aggregate(pipeline);

  const items = result[0]?.data ?? [];
  const total = result[0]?.totalCount?.[0]?.count ?? 0;

  console.log("[recordGetter] Results: items =", items.length, ", total =", total);
  if (items.length > 0) {
    console.log("[recordGetter] First 3 item IDs:", items.slice(0, 3).map((item: any) => ({
      ownerId: item._id?.toString(),
      propertyId: item.property?._id?.toString(),
      combined: `${item._id?.toString()}_${item.property?._id?.toString()}`
    })));

    // Debug: Show last_sale_date values from first 5 items
    console.log("[recordGetter] last_sale_date debug (first 5 items):", items.slice(0, 5).map((item: any) => ({
      propertyId: item.property?._id?.toString(),
      last_sale_date: item.property?.last_sale_date,
      last_sale_date_type: typeof item.property?.last_sale_date,
      last_sale_date_raw: item.property?.last_sale_date instanceof Date
        ? item.property.last_sale_date.toISOString()
        : String(item.property?.last_sale_date),
    })));
  }
  console.log("========== [recordGetter] END ==========\n");

  return { items, total };
};

export default recordGetter;
