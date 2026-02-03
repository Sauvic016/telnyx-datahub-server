import { Router } from "express";
import { sendApprovedToDirectSkip } from "../services/directskip-batch";
import prisma from "../db";
import recordGetter from "../services/contacts-check";
import recordGetterFast from "../services/contacts-check-fast";
import recordGetterTest from "../services/contacts-check-test";
import { BOTMAP } from "../utils/constants";
import editDetails from "../services/edit-details";
import { ProcessingStage } from "../generated/prisma/enums";
import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";

const router = Router();

router.get("/", async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page as string) || 1);
    const startIndex = page - 1; // Convert 1-indexed page to 0-indexed startIndex
    // Allow limit="all" or custom number
    let limit: number;
    if (req.query.limit === "all") {
      limit = Number.MAX_SAFE_INTEGER;
    } else {
      limit = Math.max(1, parseInt(req.query.limit as string) || 10);
    }
    const listIdParam = req.query.listName as string | undefined;
    const listName = listIdParam || undefined;
    const startedByBotParam = req.query.startedByBot;

    const dataType = (req.query.dataType as string as "all" | "clean" | "incomplete") || "all";
    const filterDateType = req.query.filterDateType;
    const startDate = req.query.startDate;
    const endDate = req.query.endDate;
    const sortBy = req.query.sortBy;
    const sortOrder = req.query.sortOrder;

    // Build list of available bots for frontend dropdown
    const availableBots = Object.entries(BOTMAP).map(([id, config]) => ({
      id: parseInt(id),
      name: config.name,
      isStarter: config.isStarter,
    }));

    const filterObject = Object.fromEntries(
      Object.entries({
        filterDateType,
        startDate,
        endDate,
        sortOrder,
        sortBy,
        listName,
        botId: startedByBotParam ? Number(startedByBotParam) : undefined,
      }).filter(([_, v]) => v !== undefined),
    );

    // Determine which getter to use
    const slowSorts = ["Name", "Address", "first_name", "last_name", "mailing_address", "owner_first_name"];
    const useFast = !slowSorts.includes(sortBy as string);
    const useTest = req.query.mode === "test";

    let items, total;

    if (useTest) {
      console.log("[/records] Using TEST getter (No Sorting)");
      const result = await recordGetterTest(startIndex, limit, "Records", dataType, filterObject);
      items = result.items;
      total = result.total;
    } else if (useFast) {
      console.log("[/records] Using FAST getter");
      const result = await recordGetterFast(startIndex, limit, "Records", dataType, filterObject);
      items = result.items;
      total = result.total;
    } else {
      console.log("[/records] Using SLOW getter (fallback)");
      const result = await recordGetter(startIndex, limit, "Records", dataType, filterObject);
      items = result.items;
      total = result.total;
    }

    const getLists = await prisma.list.findMany({});
    const propertyStatuses = await prisma.propertyStatus.findMany();

    res.json({
      data: items,
      page,
      limit: limit === Number.MAX_SAFE_INTEGER ? total : limit,
      total,
      totalPages: limit === Number.MAX_SAFE_INTEGER ? 1 : Math.ceil(total / limit),
      availableBots,
      botMap: BOTMAP,
      availableLists: getLists,
      propertyStatuses,
    });
    return;
  } catch (error) {
    console.error("Error in /records:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});
router.post("/decisions", async (req, res) => {
  try {
    const body = req.body;
    let ownerIdList: any[] = [];
    let totalKeyList: any[] = [];

    // Check if it's a bulk request
    if (body.isBulk) {
      console.log("[/decisions] Processing BULK decision request");
      console.log("[/decisions] Raw body:", JSON.stringify(body, null, 2));
      const { filter, limit, startIndex, excludedIds, decision } = body;
      console.log("[/decisions] Destructured values:", {
        filter,
        limit,
        startIndex,
        excludedIdsCount: excludedIds?.length,
        excludedIds,
        decision,
      });

      const dataType = (filter?.dataType as string as "all" | "clean" | "incomplete") || "all";
      const listId = filter?.listId;
      const listName = filter?.listName;

      const filterDateType = filter?.filterDateType;
      const startDate = filter?.startDate;
      const endDate = filter?.endDate;
      const sortBy = filter?.sortBy;
      const sortOrder = filter?.sortOrder;

      const startedByBot = filter?.startedByBot;
      const filterObject = Object.fromEntries(
        Object.entries({
          filterDateType,
          startDate,
          endDate,
          sortOrder,
          sortBy,
          listName,
          listId,
          botId: startedByBot ? Number(startedByBot) : undefined,
        }).filter(([_, v]) => v !== undefined),
      );
      // console.log("[/decisions] Calling recordGetter with:", {
      //   startIndex,
      //   limit,
      //   pageType: "Decision",
      //   dataType,
      //   filterObject,
      //   excludedIdsCount: excludedIds?.length,
      // });
      const { items, total } = await recordGetterFast(
        startIndex,
        limit,
        "Decision",
        dataType,
        filterObject,
        excludedIds,
      );
      // console.log("[/decisions] recordGetter returned:", { itemsCount: items.length, total });

      ownerIdList = items.map((r: any) => r._id.toString());
      totalKeyList = items.map((r: any) => ({
        ownerId: r._id.toString(),
        propertyId: r.property._id.toString(),
      }));
    } else {
      ownerIdList = body.map((b: any) => b.ownerId);

      totalKeyList = body.map((r: any) => ({
        ownerId: r.ownerId,
        propertyId: r.propertyId,
      }));
    }

    if (!ownerIdList.length) {
      return res.status(400).json({
        error: "Identity list cannot be empty, select atleast one record",
      });
    }
    let ownerIdSet = new Set(ownerIdList);
    ownerIdList = [...ownerIdSet];

    await prisma.$transaction(
      totalKeyList
        .filter((key) => key.propertyId && key.ownerId)
        .map((key) =>
          prisma.pipeline.upsert({
            where: {
              ownerId_propertyId: {
                ownerId: key.ownerId,
                propertyId: key.propertyId,
              },
            },
            update: {
              decision: ProcessingStage.APPROVED,
              stage: ProcessingStage.APPROVED,
              updatedAt: new Date(),
            },
            create: {
              decision: ProcessingStage.APPROVED,
              stage: ProcessingStage.APPROVED,
              ownerId: key.ownerId,
              propertyId: key.propertyId,
            },
          }),
        ),
    );

    await Owner.updateMany(
      { _id: { $in: ownerIdList } },
      {
        $set: {
          decision: ProcessingStage.APPROVED,
          stage: ProcessingStage.APPROVED,
        },
      },
      { strict: false },
    );

    try {
      await sendApprovedToDirectSkip(totalKeyList);
    } catch (error) {
      console.error("[/decisions] ❌ Failed to create DirectSkip batches:", error);
      return res.status(500).json({
        error: "Decisions saved but failed to create DirectSkip batches",
        details: error instanceof Error ? error.message : "Unknown error",
      });
    }
    res.json({
      success: true,
      summary: {
        total: totalKeyList.length,
      },
      message: `${totalKeyList.length} record(s) queued for DirectSkip processing.`,
    });
    return;
  } catch (error) {
    console.error("Error in /decisions:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/update-address", async (req, res) => {
  try {
    const {
      ownerId,
      propertyId,
      type,

      newPropertyAddress,
      newPropertyCity,
      newPropertyState,
      newPropertyZip,

      newMailingAddress,
      newMailingCity,
      newMailingState,
      newMailingZip,
    } = req.body;

    // 1. Validate required fields
    if (!ownerId || !propertyId || !type) {
      return res.status(400).json({
        error: "Bad Request",
        message: "ownerId, propertyId and type are required",
      });
    }

    // 2. Validate type value
    if (!["property", "mailing"].includes(type)) {
      return res.status(400).json({
        error: "Bad Request",
        message: "Invalid type. Must be 'property' or 'mailing'",
      });
    }

    // 3. Prepare update data object
    const updateData = {
      newPropertyAddress,
      newPropertyCity,
      newPropertyState,
      newPropertyZip,
      newMailingAddress,
      newMailingCity,
      newMailingState,
      newMailingZip,
    };

    // 4. Validate that at least one field is provided based on type
    if (type === "property") {
      const hasPropertyField = newPropertyAddress || newPropertyCity || newPropertyState || newPropertyZip;
      if (!hasPropertyField) {
        return res.status(400).json({
          error: "Bad Request",
          message: "At least one property field must be provided for update",
        });
      }
    } else if (type === "mailing") {
      const hasMailingField = newMailingAddress || newMailingCity || newMailingState || newMailingZip;
      if (!hasMailingField) {
        return res.status(400).json({
          error: "Bad Request",
          message: "At least one mailing field must be provided for update",
        });
      }
    }

    // 5. Call service layer
    console.log(`[/edit-details] Updating ${type} for owner ${ownerId}, property ${propertyId}`);
    const result = await editDetails(type, ownerId, propertyId, updateData);

    // 6. Handle service response
    if (!result.success) {
      return res.status(result.statusCode).json({
        error: result.error,
        message: result.error,
      });
    }

    // 7. Return success response
    return res.status(200).json({
      success: true,
      summary: result.summary,
      message: result.message,
      modifiedFields: result.modifiedFields,
    });
  } catch (err) {
    console.error("[/edit-details] Unexpected error:", err);
    return res.status(500).json({
      error: "Internal Server Error",
      message: "Failed to update details",
      details: err instanceof Error ? err.message : "Unknown error",
    });
  }
});

router.post("/delete", async (req, res) => {
  try {
    const body = req.body;

    let deletedCount = 0;

    let ownerIdList: any[] = [];
    let totalKeyList: any[] = [];

    // Check if it's a bulk request
    if (body.isBulk) {
      const { filter, limit, startIndex, excludedIds } = body;

      const dataType = (filter?.dataType as string as "all" | "clean" | "incomplete") || "all";
      const listId = filter?.listId;
      const listName = filter?.listName;

      const filterDateType = filter?.filterDateType;
      const startDate = filter?.startDate;
      const endDate = filter?.endDate;
      const sortBy = filter?.sortBy;
      const sortOrder = filter?.sortOrder;

      const startedByBot = filter?.startedByBot;
      const filterObject = Object.fromEntries(
        Object.entries({
          filterDateType,
          startDate,
          endDate,
          sortOrder,
          sortBy,
          listName,
          listId,
          botId: startedByBot ? Number(startedByBot) : undefined,
        }).filter(([_, v]) => v !== undefined),
      );
      const { items, total } = await recordGetterFast(
        startIndex,
        limit,
        "Decision",
        dataType,
        filterObject,
        excludedIds,
      );

      ownerIdList = items.map((r: any) => r._id.toString());
      totalKeyList = items
        .filter((r: any) => r.property?._id)
        .map((r: any) => ({
          ownerId: r._id.toString(),
          propertyId: r.property._id.toString(),
        }));
    } else {
      if (!Array.isArray(body)) {
        return res.status(400).json({
          error: "Request body must be an array of records",
        });
      }
      ownerIdList = body.map((b: any) => b.ownerId).filter(Boolean);

      totalKeyList = body
        .filter((r: any) => r.ownerId && r.propertyId)
        .map((r: any) => ({
          ownerId: r.ownerId,
          propertyId: r.propertyId,
        }));
    }

    if (!ownerIdList.length) {
      return res.status(400).json({
        error: "Identity list cannot be empty, select atleast one record",
      });
    }

    const propertyIds = totalKeyList.map((key) => key.propertyId).filter(Boolean);

    // Delete the properties from PropertyData
    const result = await PropertyData.deleteMany({ _id: { $in: propertyIds } });
    deletedCount = result.deletedCount ?? 0;

    // Remove propertyIds from Owner documents
    await Owner.updateMany({ _id: { $in: ownerIdList } }, { $pull: { propertyIds: { $in: propertyIds } } });

    return res.status(200).json({
      success: true,
      deletedCount,
      message: `Successfully deleted ${deletedCount} record(s)`,
    });
  } catch (error) {
    console.error("Error deleting records:", error);
    return res.status(500).json({
      success: false,
      error: "Failed to delete records",
    });
  }
});

export default router;
