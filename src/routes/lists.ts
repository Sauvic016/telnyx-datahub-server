import express from "express";
import prisma from "../db";

import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
import { makeIdentityKey, getFlowNames } from "../utils/helper";
import { pickField } from "../utils/helper";
import { JobCheckResult } from "../types/records";
import { BOTMAP } from "../utils/constants";
import { DirectSkipStatus, RowDecisionStatus } from "../generated/prisma/enums";
import { RowStatus } from "../types/records";

const router = express.Router();

router.get("/", async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page as string) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 10));
    const skip = (page - 1) * limit;

    const lists = await prisma.list.findMany({
      skip,
      take: limit,
    });

    const formattedLists = await Promise.all(
      lists.map(async ({ id, name }) => {
        // const count = await ScrappedData.countDocuments({
        //   currList: { $regex: name, $options: "i" },
        // });
        // Count properties in this list
        const count = await PropertyData.countDocuments({
          currList: {
            $elemMatch: {
              name: { $regex: `^${name}$`, $options: "i" },
            },
          },
        });

        return { id, name, countofRecords: count };
      })
    );

    res.json({
      data: formattedLists,
      page,
      limit,
      totalItems: lists.length,
      totalPages: Math.ceil(lists.length / limit),
    });
  } catch (error) {
    console.error("Error fetching lists:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
