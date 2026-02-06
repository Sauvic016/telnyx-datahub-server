import { PropertyData } from "../models/PropertyData";
import { resolveDateRange } from "../utils/helper";
import { IRecordFilter } from "./contacts-check";

const recordGetterFast = async (
  startIndex: number,
  limit: number,
  pageType: "Records" | "Newdata" | "Decision" = "Records",
  recordTab: "all" | "clean" | "incomplete" = "all",
  filter?: IRecordFilter,
  excludedIds?: string[],
) => {
  const skip = startIndex * limit;
  const pipeline: any[] = [];

  // Resolve date range from filter
  let startDate, endDate;
  if (filter?.filterDateType) {
    const result = resolveDateRange(filter);
    startDate = result.startDate;
    endDate = result.endDate;
  }

  /* --------------------------------------------------
       1️⃣ PROPERTY-LEVEL FILTERS
    -------------------------------------------------- */
  const match: any = {};

  // 1a. Bot ID (Assuming duplicated on PropertyData)
  if (filter?.botId !== undefined) {
    match.botId = filter.botId;
  }

  // 1b. Clean / Incomplete
  if (recordTab === "clean") {
    match.clean = true;
  } else if (recordTab === "incomplete") {
    match.clean = { $ne: true };
  }

  if (pageType === "Newdata") {
    match.decision = { $exists: false };
  }

  // 1c. Exclude specific property IDs (from ownerId_propertyId format)
  if (excludedIds && excludedIds.length > 0) {
    const { ObjectId } = require("mongoose").Types;
    const excludedPropertyIds = excludedIds.map((id) => {
      const [, propertyId] = id.split("_");
      return new ObjectId(propertyId);
    });
    console.log("[recordGetterFast] Excluding", excludedPropertyIds.length, "property IDs");
    match._id = { $nin: excludedPropertyIds };
  }

  // 1d. List Name
  if (filter?.listName) {
    if (filter?.listName === "all-lists") {
      match["currList"] = {
        $exists: true,
        $not: { $size: 0 },
      };
    } else {
      match["currList"] = {
        $elemMatch: { name: filter.listName },
      };
    }
  }

  /* --------------------------------------------------
     1.5️⃣ DATE FILTERS - OPTIMIZED
  -------------------------------------------------- */
  // Instead of computing a field for every doc, we filter directly on the array
  if (startDate || endDate) {
    match["currList"] = match["currList"] || {};

    // We want at least one list item to be within the range.
    // Logic: currList element exists where date is in range.
    const dateQuery: any = {};
    if (startDate) dateQuery.$gte = startDate;
    if (endDate) dateQuery.$lte = endDate;

    // Merge with existing listName filter if present
    if (match["currList"].$elemMatch) {
      Object.assign(match["currList"].$elemMatch, { list_updated_at: dateQuery });
    } else {
      match["currList"].$elemMatch = { list_updated_at: dateQuery };
    }
  }

  // Apply the Match Stage
  if (Object.keys(match).length > 0) {
    pipeline.push({ $match: match });
  }

  /* --------------------------------------------------
       2️⃣ SORTING (Property Side) - DIRECT ACCESS
    -------------------------------------------------- */
  const sortOrderValue = filter?.sortOrder === "asc" ? 1 : -1;
  let sortField = "currList.list_updated_at"; // Optimized default

  if (filter?.sortBy) {
    if (filter.sortBy === "list_updatedAt") {
      sortField = "currList.list_updated_at";
    } else if (filter.sortBy === "last_sale_date") {
      sortField = "last_sale_date";
    } else {
      sortField = filter.sortBy;
    }
  }

  // Special case: Newdata page with listName filter sorting
  if (pageType === "Newdata" && filter?.listName && !filter?.sortBy) {
    pipeline.push({
      $addFields: {
        sortKey: {
          $let: {
            vars: {
              targetList: {
                $arrayElemAt: [
                  {
                    $filter: {
                      input: { $ifNull: ["$currList", []] },
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
    pipeline.push({ $sort: { sortKey: sortOrderValue, _id: 1 } });
  } else {
    pipeline.push({ $sort: { [sortField]: sortOrderValue, _id: 1 } });
  }

  /* --------------------------------------------------
       3️⃣ PAGINATION (Fetch Page of Properties First)
    -------------------------------------------------- */
  pipeline.push({
    $facet: {
      data: [{ $skip: skip }, { $limit: limit }],
      totalCount: [{ $count: "count" }],
    },
  });

  /* --------------------------------------------------
       4️⃣ EXECUTE FIRST AGGREGATION (Get Properties)
    -------------------------------------------------- */
  const propertyResult = await PropertyData.aggregate(pipeline);
  const properties = propertyResult[0]?.data ?? [];
  const total = propertyResult[0]?.totalCount?.[0]?.count ?? 0;

  if (properties.length === 0) {
    return { items: [], total: 0 };
  }

  /* --------------------------------------------------
       5️⃣ REVERSE LOOKUP (Get Owners for these Properties)
    -------------------------------------------------- */
  // We need to fetch owners who have these property IDs.
  // We can do this with a second query or a pipeline extension.
  // Since we already executed the facet, we have the specialized list.

  // Method: Manual population effectively.
  // 1. Extract Property IDs
  const propertyIds = properties.map((p: any) => p._id);

  // 2. Find Owners who have these properties
  // We use $in. Note: An owner might appear multiple times if they have multiple properties
  // in this list (unlikely with pagination but possible).
  // Actually, the Relationship is: Owner -> [PropertyIds].
  // We need to find the Owner for EACH property in our list.

  // Efficient Query:
  const { ObjectId } = require("mongoose").Types;

  // We aggregate on Owner to match the filtered properties, but strict to our list.
  const ownerPipeline = [
    {
      $match: {
        propertyIds: { $in: propertyIds },
      },
    },
    // We only want the specific properties we found.
    // Unwind is necessary to match the property to the owner correctly.
    {
      $unwind: "$propertyIds",
    },
    {
      $match: {
        propertyIds: { $in: propertyIds },
      },
    },
    // Now we have Owner + Single PropertyId.
    // We need to join the Property Data back (or just merge it in JS).
    // Merging in JS is faster since we already have the full Property objects!
    {
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
        propertyId: "$propertyIds", // keep track to join back
      },
    },
  ];

  const owners = await require("../models/Owner").Owner.aggregate(ownerPipeline);

  /* --------------------------------------------------
       6️⃣ JOIN & FORMAT (In Memory)
    -------------------------------------------------- */
  // Map propertyId -> Property Document
  const propertyMap = new Map(properties.map((p: any) => [p._id.toString(), p]));

  // We need to return exactly `limit` items, in the correct sort order.
  // The `properties` array is already sorted and paginated.
  // We iterate through `properties` and find the matching owner.

  // Map propertyId -> Owner Document
  const ownerMap = new Map();
  owners.forEach((o: any) => {
    // Assuming 1-to-1 mapping for the unwound pair
    ownerMap.set(o.propertyId.toString(), o);
  });

  const finalItems = properties
    .map((prop: any) => {
      const owner = ownerMap.get(prop._id.toString());

      if (!owner) {
        // Only warn if strictly expected. Sometimes data drift happens.
        // console.warn("[recordGetterFast] Orphaned Property found:", prop._id);
        return null;
      }

      // Compute mostRecentListUpdate on the fly for the return object
      let mostRecentListUpdate = null;
      if (prop.currList && Array.isArray(prop.currList)) {
        // Find max date
        mostRecentListUpdate = prop.currList.reduce((max: any, item: any) => {
          if (!item.list_updated_at) return max;
          const current = new Date(item.list_updated_at);
          return !max || current > max ? current : max;
        }, null);
      }

      // Construct the combined object structurally identical to recordGetter output
      // recordGetter output structure: Owner fields + "property" object
      return {
        ...owner,
        _id: owner._id, // Owner ID
        mostRecentListUpdate, // Computed Post-Fetch
        property: prop, // Embed full property data
      };
    })
    .filter((item: any) => item !== null);

  return { items: finalItems, total };
};

export default recordGetterFast;
