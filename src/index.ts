import express from "express";
import cors from "cors";
import multer from "multer";
import path from "path";
import fs from "fs";
import "dotenv/config";
import prisma from "./db";
import { sendApprovedToDirectSkip } from "./services/directskip-batch";

import { JobType, RowDecisionStatus, ProcessingStage } from "./generated/prisma/enums";

import recordGetter from "./services/contacts-check";
import { connectToMongo } from "./mongoose";
import { makeIdentityKey } from "./utils/helper";
import { syncScrappedData } from "./services/mongo-sync";
import router from "./routes/bot-jobs";
import jobSyncRouter from "./routes/job-sync";
import { processSkipTraceResponse } from "./services/skip-trace-processing";
import listsRouter from "./routes/lists";
import { getCompletedData, getCompletedDataForContactId } from "./services/completed-data";
import { BOTMAP } from "./utils/constants";
import { ScrappedData } from "./models/ScrappedData";

const app = express();

const PORT = 5000;

app.use(cors());
app.use(express.json());

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
app.use("/lists", listsRouter);

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
    const jobsById = new Map<number, any>();
    for (const job of jobs) {
      jobsById.set(Number(job.jobId), job);
    }

    const upserts: any[] = [];
    const mismatches: Array<{ file: string; jobIdInFile: number | null }> = [];

    for (const file of files) {
      const base = path.basename(file.originalname); // final_output_<jobId>.csv
      const match = base.match(/final_output_(\d+)\.csv$/i);

      if (!match) {
        console.warn(`File ${base} does not match pattern final_output_<jobId>.csv, skipping.`);
        mismatches.push({ file: base, jobIdInFile: null });
        continue;
      }

      const jobIdFromFile = Number(match[1]);
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
        })
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
    syncScrappedData()
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

app.get("/records", async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page as string) || 1);
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

    // Build list of available bots for frontend dropdown
    const availableBots = Object.entries(BOTMAP).map(([id, config]) => ({
      id: parseInt(id),
      name: config.name,
      isStarter: config.isStarter,
    }));

    const filterObject = Object.fromEntries(
      Object.entries({
        listName,
        botId: startedByBotParam ? Number(startedByBotParam) : undefined,
      }).filter(([_, v]) => v !== undefined)
    );
    const { items, total } = await recordGetter(page, limit, dataType, filterObject);

    const getLists = await prisma.list.findMany({});

    res.json({
      data: items,
      page,
      limit: limit === Number.MAX_SAFE_INTEGER ? total : limit,
      total,
      totalPages: limit === Number.MAX_SAFE_INTEGER ? 1 : Math.ceil(total / limit),
      availableBots,
      botMap: BOTMAP,
      availableLists: getLists,
    });
    return;
  } catch (error) {
    console.error("Error in /records:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/new-data-records", async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page as string) || 1);
    // Allow limit="all" or custom number
    let limit: number;
    if (req.query.limit === "all") {
      limit = Number.MAX_SAFE_INTEGER;
    } else {
      limit = Math.max(1, parseInt(req.query.limit as string) || 10);
    }
    const listIdParam = req.query.listId as string | undefined;
    const listId = listIdParam ? parseInt(listIdParam) : undefined;
    const listNameParam = req.query.listName as string | undefined;
    const listName = listNameParam || undefined;
    const startedByBotParam = req.query.startedByBot;

    const dataType = (req.query.dataType as string as "all" | "clean" | "incomplete") || "all";

    // Build list of available bots for frontend dropdown
    const availableBots = Object.entries(BOTMAP).map(([id, config]) => ({
      id: parseInt(id),
      name: config.name,
      isStarter: config.isStarter,
    }));

    const filterObject = Object.fromEntries(
      Object.entries({
        isListChanged: true,
        listName,
        listId,
        botId: startedByBotParam ? Number(startedByBotParam) : undefined,
      }).filter(([_, v]) => v !== undefined)
    );
    const { items, total } = await recordGetter(page, limit, dataType, filterObject);

    const getLists = await prisma.list.findMany({});

    res.json({
      data: items,
      page,
      limit: limit === Number.MAX_SAFE_INTEGER ? total : limit,
      total,
      totalPages: limit === Number.MAX_SAFE_INTEGER ? 1 : Math.ceil(total / limit),
      availableBots,
      botMap: BOTMAP,
      availableLists: getLists,
    });
    return;
  } catch (error) {
    console.error("Error in /records:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/decisions", async (req, res) => {
  try {
    const body = req.body;
    let identityKeyList: any[] = [];

    // Check if it's a bulk request
    if (body.isBulk) {
      console.log("[/decisions] Processing BULK decision request");
      const { filter, limit, decision } = body;

      const dataType = (filter?.dataType as string as "all" | "clean" | "incomplete") || "all";
      const listId = filter?.listId;
      const startedByBot = filter?.startedByBot;
      const filterObject = Object.fromEntries(
        Object.entries({
          listId,
          botId: startedByBot ? Number(startedByBot) : undefined,
        }).filter(([_, v]) => v !== undefined)
      );
      const { items, total } = await recordGetter(1, limit, dataType, filterObject);
      identityKeyList = items.map((r: any) => r.identityKey);
    } else {
      identityKeyList = body.map((b: any) => b.identityKey);
      console.log("single ", identityKeyList);
    }

    if (!identityKeyList.length) {
      return res.status(400).json({
        error: "Identity list cannot be empty, select atleast one record",
      });
    }

    await prisma.$transaction(
      identityKeyList.map((key) =>
        prisma.pipeline.upsert({
          where: { identityKey: key },
          update: {
            decision: ProcessingStage.APPROVED,
            stage: ProcessingStage.APPROVED,
            updatedAt: new Date(),
          },
          create: { identityKey: key, decision: ProcessingStage.APPROVED, stage: ProcessingStage.APPROVED },
        })
      )
    );
    await ScrappedData.updateMany(
      { identityKey: { $in: identityKeyList } },
      {
        $set: {
          decision: ProcessingStage.APPROVED,
          stage: ProcessingStage.APPROVED,
        },
      }
    );

    try {
      await sendApprovedToDirectSkip(identityKeyList);
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
        total: identityKeyList.length,
      },
      message: `${identityKeyList.length} record(s) queued for DirectSkip processing.`,
    });
  } catch (error) {
    console.error("Error in /decisions:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

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

app.get("/completed-data", async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page as string) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 10));
    const skip = (page - 1) * limit;
    const listName = req.query.listName as string | undefined;

    const result = await getCompletedData({ listName });
    const totalItems = result.length;
    const paginatedData = result.slice(skip, skip + limit);

    // Fetch all available lists for the frontend dropdown
    const lists = await prisma.list.findMany({
      select: { id: true, name: true },
      orderBy: { name: "asc" },
    });

    res.json({
      data: paginatedData,
      lists,
      page,
      limit,
      totalItems,
      totalPages: Math.ceil(totalItems / limit),
    });
  } catch (error) {
    console.error("Error in /completed-data:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/record-detail/:id", async (req, res) => {
  const contactId = req.params.id;

  try {
    const contact = await getCompletedDataForContactId(contactId);
    // const contact = await prisma.contacts.findUnique({
    //   where: { id: contactId },
    //   include: {
    //     contact_phones: true,
    //     property_details: {
    //       include: {
    //         lists: {
    //           include: {
    //             list: true,
    //           },
    //         },
    //       },
    //     },
    //   },
    // });

    if (!contact) {
      return res.status(404).json({ error: "Contact not found" });
    }

    res.json(contact);
  } catch (error) {
    console.error("Error fetching contact details:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/mongodbsave", async (req, res) => {
  try {
    console.log("📋 Manual MongoDB sync triggered...");
    await syncScrappedData();
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
});
