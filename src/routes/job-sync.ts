import express from "express";
import prisma from "../db"; // adjust path to your Prisma client
import { JobType } from "../generated/prisma/enums"; // if JobType is an enum in Prisma

const router = express.Router();

router.post("/internal/bot-jobs/sync", async (req, res) => {
  try {
    const { jobId, status, currentBotId, startedByBotId, type, serverIp, createdAt, updatedAt } = req.body ?? {};

    // Basic validation
    if (typeof jobId !== "string") {
      return res.status(400).json({ error: "jobId (string) is required" });
    }
    if (!status || typeof status !== "string") {
      return res.status(400).json({ error: "status (string) is required" });
    }
    if (!type || typeof type !== "string") {
      return res.status(400).json({ error: "type (string) is required" });
    }
    if (!serverIp || typeof serverIp !== "string") {
      return res.status(400).json({ error: "serverIp (string) is required" });
    }

    // Parse dates if provided, otherwise default to now
    const createdAtDate = createdAt && typeof createdAt === "string" ? new Date(createdAt) : new Date();
    const updatedAtDate = updatedAt && typeof updatedAt === "string" ? new Date(updatedAt) : new Date();

    // Optional: guard JobType enum cast (if you want to be strict)
    let jobType: JobType;
    try {
      jobType = type as JobType;
    } catch {
      return res.status(400).json({ error: `Invalid JobType: ${type}` });
    }

    // Upsert so repeat syncs are safe and idempotent
    await prisma.botJobs.upsert({
      where: { jobId },
      update: {
        status,
        currentBotId: currentBotId ?? null,
        startedByBotId: startedByBotId ?? null,
        type: jobType,
        serverIp,
        createdAt: createdAtDate,
        updatedAt: updatedAtDate,
      },
      create: {
        jobId,
        status,
        currentBotId: currentBotId ?? null,
        startedByBotId: startedByBotId ?? null,
        type: jobType,
        serverIp,
        createdAt: createdAtDate,
        updatedAt: updatedAtDate,
      },
    });

    return res.sendStatus(200);
  } catch (err) {
    console.error("[CRM] Error in /internal/bot-jobs/sync:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
