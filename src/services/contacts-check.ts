import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
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
  page: number,
  limit: number,
  pageType: "Records" | "Newdata" | "Decision" = "Records",
  recordTab: "all" | "clean" | "incomplete" = "all",
  filter?: IRecordFilter
) => {
  const skip = (page - 1) * limit;

  const pipeline: any[] = [];

  // Resolve date range from filter
  let startDate, endDate;
  if (filter?.filterDateType) {
    const result = resolveDateRange(filter);
    startDate = result.startDate;
    endDate = result.endDate;
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
    propertyMatch["property.currList"] = {
      $elemMatch: { name: filter.listName },
    };
  }

  // Use resolved dates from resolveDateRange function
  if (startDate || endDate) {
    propertyMatch["property.updatedAt"] = {};

    // Check if it's a single day (same date, ignoring time)
    if (startDate && endDate) {
      const isSameDay =
        startDate.getUTCFullYear() === endDate.getUTCFullYear() &&
        startDate.getUTCMonth() === endDate.getUTCMonth() &&
        startDate.getUTCDate() === endDate.getUTCDate();

      if (isSameDay) {
        // Match entire day: 00:00:00 to 23:59:59
        const dayStart = new Date(
          Date.UTC(startDate.getUTCFullYear(), startDate.getUTCMonth(), startDate.getUTCDate(), 0, 0, 0, 0)
        );
        const dayEnd = new Date(
          Date.UTC(startDate.getUTCFullYear(), startDate.getUTCMonth(), startDate.getUTCDate(), 23, 59, 59, 999)
        );
        propertyMatch["property.updatedAt"].$gte = dayStart;
        propertyMatch["property.updatedAt"].$lte = dayEnd;
      } else {
        // Different days: use as-is
        propertyMatch["property.updatedAt"].$gte = startDate;
        propertyMatch["property.updatedAt"].$lte = endDate;
      }
    } else if (startDate) {
      propertyMatch["property.updatedAt"].$gte = startDate;
    } else if (endDate) {
      propertyMatch["property.updatedAt"].$lte = endDate;
    }
  }

  if (Object.keys(propertyMatch).length > 0) {
    pipeline.push({ $match: propertyMatch });
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
    pipeline.push({ $sort: { sortKey: sortOrderValue } });
  } else {
    // Default sorting: use sortBy field or default to property.updatedAt
    let sortField = "property.updatedAt"; // default

    if (filter?.sortBy) {
      if (filter.sortBy === "Date") {
        sortField = "property.updatedAt";
      } else if (filter.sortBy === "Name") {
        sortField = "owner_first_name";
      } else if (filter.sortBy === "Address") {
        sortField = "mailing_address";
      } else {
        // Use sortBy value as-is for other fields
        sortField = filter.sortBy;
      }
    }

    pipeline.push({ $sort: { [sortField]: sortOrderValue } });
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
