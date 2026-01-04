import express from "express";
import prisma from "../db";
import { BOTMAP } from "../utils/constants";
import { BotJobs } from "../generated/prisma/client";
import { sendRecordForJob } from "../services/webapp-service";

const router = express.Router();

// Define the interface that was causing issues
interface IResult extends BotJobs {
  startedByBot: string | null;
  currentBot: string | null;
}

router.get("/api/bot-jobs", async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page as string) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 10));
    const skip = (page - 1) * limit;

    const [jobs, totalItems] = await Promise.all([
      prisma.botJobs.findMany({
        orderBy: { updatedAt: "desc" },
        skip,
        take: limit,
      }),
      prisma.botJobs.count(),
    ]);

    // Map the results to match IResult interface
    const results: IResult[] = jobs.map((job) => ({
      ...job,
      startedByBot: job.startedByBotId ? BOTMAP[job.startedByBotId]?.name ?? null : null,
      currentBot: job.currentBotId ? BOTMAP[job.currentBotId]?.name ?? null : null,
    }));

    res.status(200).json({
      data: results,
      page,
      limit,
      totalItems,
      totalPages: Math.ceil(totalItems / limit),
    });
  } catch (err) {
    console.error("Error fetching BotJobs:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/api/bot-jobs/:jobId", async (req, res) => {
  try {
    const jobId = req.params.jobId;
    const records = await sendRecordForJob(jobId);
    res.json(records);
  } catch (err) {
    console.error("Error fetching BotJob:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
