import express from "express";
import cors from "cors";
import multer from "multer";
import path from "path";
import fs from "fs";
import "dotenv/config";
import prisma from "./db";

import { isValidQueryParam, getValidParam } from "./utils/query-params";
import { JobType, RowDecisionStatus, ProcessingStage } from "./generated/prisma/enums";

import recordGetter from "./services/contacts-check";
import { connectToMongo } from "./mongoose";

import router from "./routes/bot-jobs";
import jobSyncRouter from "./routes/job-sync";
import propertyStatusRouter from "./routes/property-status";

import { processSkipTraceResponse } from "./services/skip-trace-processing";
import listsRouter from "./routes/lists";
import completedDataRouter from "./routes/completed-data";
import recordsRouter from "./routes/records";
import newDataRouter from "./routes/new-data";

import { BOTMAP } from "./utils/constants";
import { Owner } from "./models/Owner";
import { syncScrappedDataOptimized } from "./services/mongo-sync-optimized";
import editDetails from "./services/edit-details";
import { backfillPipelineLinks } from "./services/backfill-pipeline";

import { migrateLastSaleDateStrings } from "./utils/mongo-last-sale-date-migration";
import { backfillSaleAndCaseDates } from "./utils/completed-page-backfill";
// import { cleanUpNewData } from "./utils/new-data-page-cleanup";

const app = express();

const PORT = 5000;

app.use(cors());
app.use(express.json({ limit: "100mb" }));

const uploadDir = path.join(process.cwd(), "job_result");
console.log("Upload directory:", uploadDir);

if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadDir),
  filename: (_req, file, cb) => cb(null, file.originalname),
});

const upload = multer({ storage });

app.use(router);
app.use(jobSyncRouter);
app.use(propertyStatusRouter);
app.use("/lists", listsRouter);
app.use("/completed-data", completedDataRouter);
app.use("/records", recordsRouter);
app.use("/new-data", newDataRouter);

app.post("/receive", upload.array("files"), async (req, res) => {
  try {
    let metadata: any = null;
    if (typeof req.body.metadata === "string") {
      try {
        metadata = JSON.parse(req.body.metadata);
      } catch {
        metadata = null;
      }
    }

    const files = req.files as Express.Multer.File[];
    console.log("BODY:", req.body);
    console.log("FILES:", files);

    if (!files?.length) {
      return res.status(400).json({ error: "No files received" });
    }

    const jobs = metadata?.jobs ?? [];
    if (!jobs.length) {
      console.warn("No jobs provided in metadata.jobs");
      return res.status(400).json({ error: "No jobs in metadata" });
    }

    // Build a lookup by jobId so we can match by file name
    const jobsById = new Map<string, any>();
    for (const job of jobs) {
      jobsById.set(job.jobId, job);
    }

    const upserts: any[] = [];
    const mismatches: Array<{ file: string; jobIdInFile: string | null }> = [];

    for (const file of files) {
      const base = path.basename(file.originalname); // final_output_<jobId>.csv
      const match = base.match(/final_output_([a-f0-9-]{36})\.csv$/i);

      if (!match) {
        console.warn(`File ${base} does not match pattern final_output_<jobId>.csv, skipping.`);
        mismatches.push({ file: base, jobIdInFile: null });
        continue;
      }

      const jobIdFromFile = match[1];
      const job = jobsById.get(jobIdFromFile);

      if (!job) {
        console.warn(`No job found in metadata.jobs for jobId ${jobIdFromFile} (file: ${base}). Skipping.`);
        mismatches.push({ file: base, jobIdInFile: jobIdFromFile });
        continue;
      }

      const resultFilePath = file.path; // stored path on disk

      // Upsert BotJobs so you can call this many times safely
      upserts.push(
        prisma.botJobs.upsert({
          where: { jobId: jobIdFromFile },
          update: {
            status: job.status,
            currentBotId: job.currentBotId,
            startedByBotId: job.startedByBotId,
            type: job.type as JobType,
            serverIp: job.serverIp,
            createdAt: new Date(job.createdAt),
            updatedAt: new Date(job.updatedAt),
            // NEW: store file path
            resultFilePath,
          },
          create: {
            jobId: jobIdFromFile,
            status: job.status,
            currentBotId: job.currentBotId,
            startedByBotId: job.startedByBotId,
            type: job.type as JobType,
            serverIp: job.serverIp,
            createdAt: new Date(job.createdAt),
            updatedAt: new Date(job.updatedAt),
            resultFilePath,
          },
        }),
      );
    }

    if (!upserts.length) {
      console.warn("No valid job/file pairs found; nothing to insert.");
      return res.status(400).json({
        error: "No valid job/file matches. Check filenames and metadata.jobs.",
        mismatches,
      });
    }

    await prisma.$transaction(upserts);

    // Automatically sync to MongoDB after successful file upload
    // Run in background without blocking the response
    syncScrappedDataOptimized()
      .then(() => {
        console.log("✅ MongoDB sync completed successfully after file upload");
      })
      .catch((err) => {
        console.error("❌ MongoDB sync failed after file upload:", err);
        // Note: We don't fail the request if MongoDB sync fails
        // User can manually retry using /mongodbsave endpoint
      });

    res.json({
      status: "ok",
      updated: upserts.length,
      mismatches,
      mongoSyncTriggered: true, // Indicates that MongoDB sync has been triggered
    });
  } catch (err) {
    console.error("Error in /receive:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

// ... (existing imports)

// Webhook endpoint to receive DirectSkip status updates
app.post("/webhook/directskip", async (req, res) => {
  try {
    // Fire and forget processing only if results exist
    const results = req.body;
    if (results && Array.isArray(results) && results.length > 0) {
      processSkipTraceResponse(results).catch((err) => {
        console.error("[Webhook] Error processing skip trace results:", err);
      });
    }
    res.status(200).json({
      status: "ok",
    });
  } catch (error) {
    console.error("[Webhook] Error processing DirectSkip webhook:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get DirectSkip batch status
app.get("/directskip-batch/:batchId", async (req, res) => {
  try {
    const { batchId } = req.params;

    const batch = await prisma.directSkipBatch.findUnique({
      where: { id: batchId },
    });

    if (!batch) {
      return res.status(404).json({ error: "Batch not found" });
    }

    res.json({
      batchId: batch.id,
      rowCount: batch.rowCount,
      status: batch.status,
      submittedAt: batch.submittedAt,
      completedAt: batch.completedAt,
      error: batch.error,
      responseData: batch.responseData,
    });
  } catch (error) {
    console.error("[GET /directskip-batch] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Manual trigger for MongoDB sync (useful for retries or scheduled jobs)

app.get("/mongodbsave", async (req, res) => {
  try {
    console.log("📋 Manual MongoDB sync triggered...");
    await syncScrappedDataOptimized(true);
    res.json({
      status: "ok",
      message: "MongoDB sync completed successfully",
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Error in /mongodbsave:", error);
    res.status(500).json({
      status: "error",
      message: "MongoDB sync failed",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.listen(PORT, async () => {
  console.log(`Receiver running on port ${PORT}`);
  connectToMongo();
  // await syncScrappedDataOptimized(true);
  // backfillSaleAndCaseDates();
  // backfillPipelineLinks();
  // cleanUpNewData();
  // migrateLastSaleDateStrings();
});
