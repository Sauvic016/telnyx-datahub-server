import express from "express";
import prisma from "../db";

import { ScrappedData } from "../models/ScrappedData";
import { makeIdentityKey, getFlowNames } from "../utils/helper";
import { pickField } from "../services/contacts-check";
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
        const count = await ScrappedData.countDocuments({
          currList: name,
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

router.get("/:id", async (req, res) => {
  try {
    const listId = parseInt(req.params.id);
    if (isNaN(listId)) {
      return res.status(400).json({ error: "Invalid list ID" });
    }

    const page = Math.max(1, parseInt(req.query.page as string) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 10));
    const skip = (page - 1) * limit;

    // 1. Fetch contacts associated with this list
    const contacts = await prisma.contacts.findMany({
      where: {
        property_details: {
          some: {
            lists: {
              some: {
                listId: listId,
              },
            },
          },
        },
      },
      include: {
        directskips: true,
        property_details: {
          include: {
            lists: {
              include: {
                list: true,
              },
            },
          },
        },
      },
    });

    if (!contacts.length) {
      return res.json({
        data: [],
        page,
        limit,
        totalItems: 0,
        totalPages: 0,
      });
    }

    // 2. Build identity mapping and list of identities to search
    const identityKeyToContact = new Map<string, any>();
    const identities: { first: string; last: string; addr: string }[] = [];

    for (const contact of contacts) {
      const first = (contact.first_name ?? "").trim();
      const last = (contact.last_name ?? "").trim();
      const addr = (contact.mailing_address ?? "").trim();

      if (!first || !last || !addr) continue;

      const key = makeIdentityKey(first, last, addr);

      const dbPropertyAddresses =
        contact.property_details
          ?.map((p) => p.property_address)
          .filter((a): a is string => a !== null && a !== undefined && a.trim() !== "") ?? [];

      const dbLists = Array.from(
        new Set(
          contact.property_details
            ?.flatMap((p) => p.lists.map((l) => l.list.name))
            .filter((name): name is string => name !== null && name !== undefined) ?? []
        )
      );

      identityKeyToContact.set(key, {
        contactId: contact.id,
        directSkipId: contact.directskips?.id,
        skipTracedAt: contact.directskips?.skipTracedAt ?? null,
        directSkipStatus: contact.directskips?.status ?? null,
        propertyAddresses: dbPropertyAddresses,
        lists: dbLists,
      });

      identities.push({ first, last, addr });
    }

    // 3. Fetch MongoDB records matching these identities
    // We batch the query to avoid huge $or clauses
    const BATCH_SIZE = 50;
    const mongoRecords: any[] = [];

    for (let i = 0; i < identities.length; i += BATCH_SIZE) {
      const batch = identities.slice(i, i + BATCH_SIZE);
      const orConditions = batch.flatMap(({ first, last, addr }) => {
        const regexFirst = new RegExp(`^${first}$`, "i");
        const regexLast = new RegExp(`^${last}$`, "i");
        const regexAddr = new RegExp(`^${addr}$`, "i");

        return [
          { first_name: regexFirst, last_name: regexLast, mailing_address: regexAddr },
          { "First Name": regexFirst, "Last Name": regexLast, "Mailing Address": regexAddr },
          { "Owner First Name": regexFirst, "Owner Last Name": regexLast, "Mailing Address": regexAddr },
        ];
      });

      const batchResults = await ScrappedData.find({ $or: orConditions }).lean();
      mongoRecords.push(...batchResults);
    }

    // 4. Group by Job ID
    const recordsByJobId = new Map<string, any[]>();
    const jobIds = new Set<string>();

    for (const doc of mongoRecords) {
      const jobId = doc.jobId;
      if (jobId) {
        if (!recordsByJobId.has(jobId)) {
          recordsByJobId.set(jobId, []);
          jobIds.add(jobId);
        }
        recordsByJobId.get(jobId)!.push(doc);
      }
    }

    // 5. Fetch Job Details
    const jobs = await prisma.botJobs.findMany({
      where: {
        jobId: { in: Array.from(jobIds) },
      },
    });

    const jobsMap = new Map(jobs.map((j) => [j.jobId, j]));

    // 6. Fetch Decisions
    const allIdentityKeys = Array.from(identityKeyToContact.keys());
    const decisions = await prisma.pipeline.findMany({
      where: {
        identityKey: { in: allIdentityKeys },
      },
    });
    const decisionMap = new Map(decisions.map((d) => [d.identityKey, { status: d.decision, decidedAt: d.updatedAt }]));

    // 7. Build Result
    const results: JobCheckResult[] = [];

    for (const jobId of jobIds) {
      const job = jobsMap.get(jobId);
      if (!job) continue;

      const docs = recordsByJobId.get(jobId) || [];
      const jobRecords: RowStatus[] = [];

      for (const doc of docs) {
        const first = pickField(doc, ["first_name", "First Name", "Owner First Name"]);
        const last = pickField(doc, ["last_name", "Last Name", "Owner Last Name"]);
        const mailingAddr = pickField(doc, ["mailing_address", "Mailing Address"]);
        const propertyAddr = pickField(doc, ["property_address", "Property Address"]);
        const csvId = pickField(doc, ["id", "Id", "ID"]) || String(doc._id);
        const listStr = pickField(doc, ["list", "lists"]);

        if (!first || !last || !mailingAddr) continue;

        const key = makeIdentityKey(first, last, mailingAddr);
        const match = identityKeyToContact.get(key);

        // If this record doesn't match one of our list contacts, skip it
        // (This handles the case where a job has other records not in our list)
        if (!match) continue;

        const decision = decisionMap.get(key);
        const userDecision: RowDecisionStatus = decision?.status ?? "PENDING";
        const decidedAt = decision?.decidedAt ?? null;

        const csvLists = listStr
          ? listStr
              .split(",")
              .map((s) => s.trim().replace(/^"|"$/g, ""))
              .filter((s) => s.length > 0)
          : [];

        const dbAddresses = match.propertyAddresses || [];
        const csvAddrNormalized = propertyAddr.trim().toLowerCase();
        const existsInDb = dbAddresses.some((a: string) => a.trim().toLowerCase() === csvAddrNormalized);
        const finalPropertyAddresses = existsInDb ? dbAddresses : [...dbAddresses, propertyAddr];
        const mergedLists = Array.from(new Set([...(match.lists || []), ...csvLists]));

        jobRecords.push({
          csvRecordId: csvId,
          first_name: first,
          last_name: last,
          mailing_address: mailingAddr,
          property_address: finalPropertyAddresses,
          lists: mergedLists,
          jobId: jobId,
          contactId: match.contactId,
          directSkipId: match.directSkipId,
          skipTracedAt: match.skipTracedAt,
          directSkipStatus: match.directSkipStatus,
          userDecision,
          decidedAt,
          ...doc,
        });
      }

      if (jobRecords.length > 0) {
        const botName = BOTMAP[job.startedByBotId!]?.name;
        const flow = getFlowNames(job.startedByBotId!)!;

        results.push({
          jobId: job.jobId,
          startedByBot: botName,
          flow,
          records: jobRecords,
        });
      }
    }

    // Flatten all records across all jobs for pagination
    const allRecords = results.flatMap((job) =>
      job.records.map((record) => ({
        ...record,
        startedByBot: job.startedByBot,
        flow: job.flow,
      }))
    );

    const totalItems = allRecords.length;
    const paginatedRecords = allRecords.slice(skip, skip + limit);

    res.json({
      data: paginatedRecords,
      page,
      limit,
      totalItems,
      totalPages: Math.ceil(totalItems / limit),
    });
  } catch (error) {
    console.error("Error fetching list details:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
