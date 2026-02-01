import { Router } from "express";
import prisma from "../db";
import { PropertyStatusType } from "../generated/prisma/enums";

interface MyQueryParams {
  type: PropertyStatusType;
}

const router = Router();

router.get("/property-status", async (req, res) => {
  const { type } = req.query as unknown as MyQueryParams;
  const statusList = await prisma.propertyStatus.findMany({
    where: {
      statusType: type,
    },
  });
  res.status(200).json({
    statusList,
    type,
  });
  return;
});
/* ===========================
   POST /property-status
=========================== */
router.post("/property-status", async (req, res) => {
  try {
    const userId = "1";
    const { name, description, color, type } = req.body;

    if (!name || !type) {
      return res.status(400).json({ message: "Name and type are required" });
    }

    if (!Object.values(PropertyStatusType).includes(type)) {
      return res.status(400).json({ message: "Invalid status type" });
    }

    const status = await prisma.propertyStatus.create({
      data: {
        userId,
        name: name.trim(),
        description,
        color,
        statusType: type,
      },
    });

    res.status(201).json(status);
  } catch (error: any) {
    if (error.code === "P2002") {
      return res.status(409).json({
        message: "Status with this name already exists",
      });
    }

    console.error(error);
    res.status(500).json({ message: "Failed to create status" });
  }
});

/* ===========================
   PUT /property-status/:id
=========================== */
router.put("/property-status/:id", async (req, res) => {
  try {
    const userId = "1";
    const { id } = req.params;
    const { name, description, color, type } = req.body;

    if (!Object.values(PropertyStatusType).includes(type)) {
      return res.status(400).json({ message: "Invalid status type" });
    }

    const updated = await prisma.propertyStatus.updateMany({
      where: {
        id,
        userId,
        statusType: type,
      },
      data: {
        name: name?.trim(),
        description,
        color,
      },
    });

    if (updated.count === 0) {
      return res.status(404).json({ message: "Status not found" });
    }

    res.json({ success: true });
  } catch (error: any) {
    if (error.code === "P2002") {
      return res.status(409).json({
        message: "Status with this name already exists",
      });
    }

    console.error(error);
    res.status(500).json({ message: "Failed to update status" });
  }
});

/* ===========================
   DELETE /property-status/:id
=========================== */
router.delete("/property-status/:id", async (req, res) => {
  console.log("hua");
  try {
    const userId = "1";
    const { id } = req.params;
    const { type } = req.query;

    if (!Object.values(PropertyStatusType).includes(type as PropertyStatusType)) {
      return res.status(400).json({ message: "Invalid status type" });
    }

    const deleted = await prisma.propertyStatus.deleteMany({
      where: {
        id,
        userId,
        statusType: type as PropertyStatusType,
      },
    });

    if (deleted.count === 0) {
      return res.status(404).json({ message: "Status not found" });
    }

    res.status(204).send();
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Failed to delete status" });
  }
});

router.post("/add-property-status", async (req, res) => {
  const { propertyId, propertyStatusId } = req.body;

  const existing = await prisma.propertyStatusAssociation.findUnique({
    where: {
      propertyId_propertyStatusId: {
        propertyId,
        propertyStatusId,
      },
    },
  });
  if (existing) {
    // remove association
    // await prisma.propertyStatusAssociation.delete({
    //   where: {
    //     propertyId_propertyStatusId: {
    //       propertyId,
    //       propertyStatusId,
    //     },
    //   },
    // });
    return;
  } else {
    // add association
    await prisma.propertyStatusAssociation.create({
      data: {
        propertyId,
        propertyStatusId,
      },
    });
  }
});

router.patch("/property-status/:propertyId", async (req, res) => {
  const { propertyId } = req.params as any;
  console.log(propertyId);
  const { property_status, statusIdToRemove } = req.body;

  if (property_status === null && statusIdToRemove) {
    try {
      const deleted = await prisma.propertyStatusAssociation.deleteMany({
        where: {
          propertyId,
          propertyStatusId: statusIdToRemove,
        },
      });

      if (deleted.count === 0) {
        return res.status(404).json({ message: "Status association not found" });
      }

      res.status(200).json({
        message: "Status removed successfully",
      });
      return;
    } catch (error) {
      console.error(error);
      res.status(500).json({ message: "Internal server error" });
      return;
    }
  }

  const { id: propertyStatusId } = property_status;
  if (!propertyStatusId) {
    return res.status(400).json({ message: "propertyStatusId is required" });
  }

  try {
    const status = await prisma.propertyStatus.findUnique({
      where: { id: propertyStatusId },
    });

    if (!status) {
      return res.status(404).json({ message: "PropertyStatus not found" });
    }

    // Find existing association of same status type
    const existingAssociation = await prisma.propertyStatusAssociation.findFirst({
      where: {
        propertyId,
        PropertyStatus: {
          statusType: status.statusType,
        },
      },
    });

    // 1️⃣ Same status already attached → do nothing
    if (existingAssociation && existingAssociation.propertyStatusId === propertyStatusId) {
      res.status(200).json({
        message: "Status already attached",
      });
      return;
    }

    // 2️⃣ Different status of same type → replace
    if (existingAssociation) {
      await prisma.propertyStatusAssociation.update({
        where: {
          propertyId_propertyStatusId: {
            propertyId,
            propertyStatusId: existingAssociation.propertyStatusId,
          },
        },
        data: {
          propertyStatusId,
        },
      });

      return res.status(200).json({
        message: `${status.statusType} status replaced`,
      });
    }

    // 3️⃣ No status of this type → attach
    await prisma.propertyStatusAssociation.create({
      data: {
        propertyId,
        propertyStatusId,
      },
    });

    return res.status(200).json({
      message: `${status.statusType} status attached`,
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ message: "Internal server error" });
  }
});

export default router;
