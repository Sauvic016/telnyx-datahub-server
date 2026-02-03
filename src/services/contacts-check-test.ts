import { PropertyData } from "../models/PropertyData";
import { resolveDateRange } from "../utils/helper";
import { IRecordFilter } from "./contacts-check";

const recordGetterTest = async (
    startIndex: number,
    limit: number,
    pageType: "Records" | "Newdata" | "Decision" = "Records",
    recordTab: "all" | "clean" | "incomplete" = "all",
    filter?: IRecordFilter,
) => {
    console.log("\n========== [recordGetterTest] START ==========");
    console.log("[recordGetterTest] NO SORTING MODE");
    console.log("[recordGetterTest] Input params:", {
        startIndex,
        limit,
        recordTab,
    });

    const skip = startIndex * limit;
    const pipeline: any[] = [];

    /* --------------------------------------------------
       1️⃣ PROPERTY-LEVEL FILTERS
    -------------------------------------------------- */
    const match: any = {};

    if (filter?.botId !== undefined) match.botId = filter.botId;

    if (recordTab === "clean") {
        match.clean = true;
    } else if (recordTab === "incomplete") {
        match.clean = { $ne: true };
    }

    if (filter?.listName) {
        if (filter?.listName === "all-lists") {
            match["currList"] = { $exists: true, $not: { $size: 0 } };
        } else {
            match["currList"] = { $elemMatch: { name: filter.listName } };
        }
    }

    if (Object.keys(match).length > 0) {
        console.log("[recordGetterTest] Match criteria keys:", Object.keys(match));
        pipeline.push({ $match: match });
    }

    /* --------------------------------------------------
       2️⃣ NO SORTING - JUST LIMIT
    -------------------------------------------------- */
    // Intentionally skipping sort to test raw filter speed
    // Default natural order (insertion order usually)

    /* --------------------------------------------------
       3️⃣ PAGINATION
    -------------------------------------------------- */
    pipeline.push({
        $facet: {
            data: [{ $skip: skip }, { $limit: limit }],
            totalCount: [{ $count: "count" }],
        },
    });

    /* --------------------------------------------------
       4️⃣ EXECUTE
    -------------------------------------------------- */
    const propertyResult = await PropertyData.aggregate(pipeline);
    const properties = propertyResult[0]?.data ?? [];
    const total = propertyResult[0]?.totalCount?.[0]?.count ?? 0;

    if (properties.length === 0) return { items: [], total: 0 };

    /* --------------------------------------------------
       5️⃣ JOIN OWNERS
    -------------------------------------------------- */
    const propertyIds = properties.map((p: any) => p._id);
    const { ObjectId } = require("mongoose").Types;

    const ownerPipeline = [
        { $match: { propertyIds: { $in: propertyIds } } },
        { $unwind: "$propertyIds" },
        { $match: { propertyIds: { $in: propertyIds } } },
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
                propertyId: "$propertyIds"
            }
        }
    ];

    const owners = await require("../models/Owner").Owner.aggregate(ownerPipeline);

    const ownerMap = new Map();
    owners.forEach((o: any) => ownerMap.set(o.propertyId.toString(), o));

    const finalItems = properties.map((prop: any) => {
        const owner = ownerMap.get(prop._id.toString());
        if (!owner) return null;

        // Compute simple date for display if needed
        let mostRecentListUpdate = null;
        if (prop.currList && Array.isArray(prop.currList)) {
            mostRecentListUpdate = prop.currList.reduce((max: any, item: any) => {
                if (!item.list_updated_at) return max;
                const current = new Date(item.list_updated_at);
                return (!max || current > max) ? current : max;
            }, null);
        }

        return {
            ...owner,
            _id: owner._id,
            mostRecentListUpdate,
            property: prop
        };
    }).filter((item: any) => item !== null);

    console.log("========== [recordGetterTest] END ==========\n");
    return { items: finalItems, total };
};

export default recordGetterTest;
