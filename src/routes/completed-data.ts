import { Router } from "express";
import { CompletedRecordsFilters, fetchCompletedRecords } from "../services/completed-data";
import { getValidParam } from "../utils/query-params";
import prisma from "../db";

const router = Router();

interface DeleteRequestBody {
  isBulk: boolean;
  // For manual selection
  ids?: string[];
  // For bulk selection
  filter?: {
    listName?: string;
    dateRange?: { start: string; end: string };
    propertyStatus?: string;
    activity?: string;
  };
  limit?: number | "all";
  startIndex?: number;
  excludedIds?: string[];
}

router.get("/", async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page as string) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 10));
    const skip = (page - 1) * limit;

    const filters: CompletedRecordsFilters = {};

    if (req.query.listName) {
      filters.listName = req.query.listName as string;
    }

    filters.propertyStatusId = getValidParam(req.query.propertyStatusId);

    if (req.query.startDate || req.query.endDate || req.query.filterDateType) {
      filters.dateRange = {
        type: req.query.filterDateType as string | undefined,
        startDate: req.query.startDate as string | undefined,
        endDate: req.query.endDate as string | undefined,
      };
    }

    const sortBy = req.query.sortBy as string | undefined;
    if (sortBy === "updatedAt" || sortBy === "lastSold") {
      filters.sortBy = sortBy;
    }

    const sortOrder = req.query.sortOrder as string | undefined;
    if (sortOrder === "asc" || sortOrder === "desc") {
      filters.sortOrder = sortOrder;
    }

    const searchQuery = req.query.search as string | undefined;

    const result = await fetchCompletedRecords(
      Object.keys(filters).length > 0 ? filters : undefined,
      { skip, take: limit },
      searchQuery,
    );
    const lists = await prisma.list.findMany({
      select: { id: true, name: true },
      orderBy: { name: "asc" },
    });
    const propertyStatuses = await prisma.propertyStatus.findMany();
    res.json({
      data: result.rows,
      lists,
      page,
      limit,
      totalItems: result.total,
      totalPages: Math.ceil(result.total / limit),
      propertyStatuses,
    });
  } catch (error) {
    console.error("Error in /completed-data:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/delete", async (req, res) => {
  try {
    const body: DeleteRequestBody = req.body;

    let deletedCount = 0;

    if (body.isBulk) {
      // ============ BULK DELETE ============
      const { filter, limit, startIndex = 0, excludedIds = [] } = body;

      // Parse excluded IDs into ownerId-propertyId pairs
      const excludedPairs = excludedIds.map((id) => {
        const [propertyId, ownerId] = id.split("-");
        return { ownerId, propertyId };
      });

      const whereClause: any = {};

      // Apply filters
      if (filter?.listName && filter.listName !== "all-lists") {
        whereClause.propertyDetails = {
          lists: {
            some: {
              name: filter.listName,
            },
          },
        };
      }

      if (filter?.dateRange?.start && filter?.dateRange?.end) {
        whereClause.updatedAt = {
          gte: new Date(filter.dateRange.start),
          lte: new Date(filter.dateRange.end),
        };
      }

      if (filter?.propertyStatus) {
        whereClause.propertyDetails = {
          ...whereClause.propertyDetails,
          OR: [
            { primaryPropertyStatusId: filter.propertyStatus },
            { secondaryPropertyStatusId: filter.propertyStatus },
          ],
        };
      }

      if (filter?.activity) {
        // Add activity filter based on your schema
        // e.g., whereClause.sms_sent = filter.activity.sms_sent;
      }

      if (limit === "all") {
        const allRecords = await prisma.pipeline.findMany({
          where: whereClause,
          select: {
            ownerId: true,
            propertyId: true,
          },
        });

        // Filter out excluded records
        const recordsToDelete = allRecords.filter(
          (record) =>
            !excludedPairs.some(
              (excluded) => excluded.ownerId === record.ownerId && excluded.propertyId === record.propertyId,
            ),
        );

        // Delete in batches
        if (recordsToDelete.length > 0) {
          const result = await prisma.pipeline.deleteMany({
            where: {
              OR: recordsToDelete.map((record) => ({
                ownerId: record.ownerId,
                propertyId: record.propertyId,
              })),
            },
          });
          deletedCount = result.count;
        }
      } else {
        const recordsInRange = await prisma.pipeline.findMany({
          where: whereClause,
          orderBy: {
            updatedAt: "desc", // Use same ordering as your fetchCompletedRecords
          },
          skip: startIndex,
          take: limit,
          select: {
            ownerId: true,
            propertyId: true,
          },
        });

        // Filter out excluded records
        const recordsToDelete = recordsInRange.filter(
          (record) =>
            !excludedPairs.some(
              (excluded) => excluded.ownerId === record.ownerId && excluded.propertyId === record.propertyId,
            ),
        );

        // Delete the records
        if (recordsToDelete.length > 0) {
          const result = await prisma.pipeline.deleteMany({
            where: {
              OR: recordsToDelete.map((record) => ({
                ownerId: record.ownerId,
                propertyId: record.propertyId,
              })),
            },
          });
          deletedCount = result.count;
        }
      }
    } else {
      // ============ MANUAL DELETE ============
      const { ids = [] } = body;

      if (ids.length === 0) {
        return res.status(400).json({ error: "No IDs provided" });
      }

      // Parse IDs into ownerId-propertyId pairs
      const pairs = ids.map((id) => {
        const [propertyId, ownerId] = id.split("-");
        return { ownerId, propertyId };
      });

      // Delete the records
      const result = await prisma.pipeline.deleteMany({
        where: {
          OR: pairs.map((pair) => ({
            ownerId: pair.ownerId,
            propertyId: pair.propertyId,
          })),
        },
      });

      deletedCount = result.count;
    }

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

interface UpdateAddressBody {
  propertyDetailsId: string;
  contactId: string;
  addressType: "mailing" | "property" | "confirmed";
  address: {
    address: string;
    city: string;
    state: string;
    zip: string;
  };
}

router.patch("/update-address", async (req, res) => {
  try {
    const { propertyDetailsId, contactId, addressType, address }: UpdateAddressBody = req.body;

    // Validate required fields
    if (!addressType || !address) {
      res.status(400).json({
        success: false,
        error: "addressType and address are required",
      });
      return;
    }

    if (!["mailing", "property", "confirmed"].includes(addressType)) {
      res.status(400).json({
        success: false,
        error: "Invalid addressType. Must be 'mailing', 'property', or 'confirmed'",
      });
      return;
    }

    let result;

    switch (addressType) {
      case "mailing":
        if (!contactId) {
          res.status(400).json({
            success: false,
            error: "contactId is required for mailing address",
          });
          return;
        }

        result = await prisma.contacts.update({
          where: { id: contactId },
          data: {
            mailing_address: address.address,
            mailing_city: address.city,
            mailing_state: address.state,
            mailing_zip: address.zip,
          },
        });
        break;

      case "property":
        if (!propertyDetailsId) {
          res.status(400).json({
            success: false,
            error: "propertyDetailsId is required for property address",
          });
          return;
        }

        result = await prisma.property_details.update({
          where: { id: propertyDetailsId },
          data: {
            property_address: address.address,
            property_city: address.city,
            property_state: address.state,
            property_zip: address.zip,
          },
        });
        break;

      case "confirmed":
        if (!contactId) {
          res.status(400).json({
            success: false,
            error: "contactId is required for confirmed address",
          });
          return;
        }

        // Find the DirectSkip record by contactId
        const directSkip = await prisma.directSkip.findUnique({
          where: { contactId: contactId },
        });

        if (!directSkip) {
          res.status(404).json({
            success: false,
            error: "DirectSkip record not found for this contact",
          });
          return;
        }

        // Update the confirmedAddress JSON field
        result = await prisma.directSkip.update({
          where: { contactId: contactId },
          data: {
            confirmedAddress: {
              street: address.address,
              city: address.city,
              state: address.state,
              zip: address.zip,
            },
          },
        });
        break;
    }

    res.status(200).json({
      success: true,
      message: `${addressType} address updated successfully`,
      data: result,
    });
    return;
  } catch (error) {
    console.error("Error updating address:", error);

    if ((error as any).code === "P2025") {
      res.status(404).json({
        success: false,
        error: "Record not found",
      });
      return;
    }

    res.status(500).json({
      success: false,
      error: "Failed to update address",
    });
    return;
  }
});
export default router;
