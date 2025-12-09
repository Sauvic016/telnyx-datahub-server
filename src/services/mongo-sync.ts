import path from "path";
import fs from "fs";
import pl from "nodejs-polars";
import { getLatestJobsPerBot, getAllJobs } from "../utils/helper";
import { ScrappedData } from "../models/ScrappedData";

function pickColumn(columns: string[], candidates: string[]): string | null {
  const normalize = (s: string) => s.trim().toLowerCase().replace(/^"|"$/g, "");
  const lowerCols = columns.map(normalize);

  for (const cand of candidates) {
    const candNorm = normalize(cand);
    const idx = lowerCols.indexOf(candNorm);
    if (idx !== -1) return columns[idx];
  }
  return null;
}

function makeIdentityKey(first: string, last: string, addr: string): string {
  return `${first.trim().toLowerCase()}|${last.trim().toLowerCase()}|${addr.trim().toLowerCase()}`;
}

import prisma from "../db";

// ... (existing imports)

export const syncScrappedData = async () => {
  try {
    console.log("Starting MongoDB sync...");
    // const latestJobs = await getLatestJobsPerBot();
    const latestJobs = await getAllJobs();
    console.log(`Found ${latestJobs.length} jobs to process.`);

    for (const job of latestJobs) {
      const fileName = `final_output_${job.jobId}.csv`;
      const filePath = path.join(process.cwd(), "job_result", fileName);

      if (!fs.existsSync(filePath)) {
        console.warn(`File not found for job ${job.jobId}: ${filePath}`);
        continue;
      }

      console.log(`Processing job ${job.jobId} from ${fileName}`);

      try {
        const df = pl.readCSV(filePath);
        const cols = df.columns;

        const firstNameCol = pickColumn(cols, ["first_name", "First Name", "Owner First Name"]);
        const lastNameCol = pickColumn(cols, ["last_name", "Last Name", "Owner Last Name"]);
        const mailingAddressCol = pickColumn(cols, ["mailing_address", "Mailing Address"]);
        const propertyAddressCol = pickColumn(cols, ["property_address", "Property Address"]);
        const mailingZipCol = pickColumn(cols, ["mailing_zip", "Mailing Zip Code"]);
        const mailingCityCol = pickColumn(cols, ["mailing_city", "Mailing City"]);
        const mailingStateCol = pickColumn(cols, ["mailing_state", "Mailing State"]);
        const propertyZipCol = pickColumn(cols, ["property_zip", "Property Zip Code"]);
        const propertyCityCol = pickColumn(cols, ["property_city", "Property City"]);
        const propertyStateCol = pickColumn(cols, ["property_state", "Property State"]);


        if (!firstNameCol && !lastNameCol && !mailingAddressCol ) {
          console.warn(`Missing required identity columns in ${fileName}. Skipping.`);
          continue;
        }

        const rows = df.rows() as unknown[][];
        const fnIdx = firstNameCol?cols.indexOf(firstNameCol): -1;
        const lnIdx = lastNameCol?cols.indexOf(lastNameCol): -1;
        const addrIdx = mailingAddressCol?cols.indexOf(mailingAddressCol): -1;
        const propAddrIdx = propertyAddressCol ? cols.indexOf(propertyAddressCol) : -1;
        const mailingZipIdx = mailingZipCol ? cols.indexOf(mailingZipCol) : -1;
        const mailingCityIdx = mailingCityCol ? cols.indexOf(mailingCityCol) : -1;
        const mailingStateIdx = mailingStateCol ? cols.indexOf(mailingStateCol) : -1;
        const propertyZipIdx = propertyZipCol ? cols.indexOf(propertyZipCol) : -1;
        const propertyCityIdx = propertyCityCol ? cols.indexOf(propertyCityCol) : -1;
        const propertyStateIdx = propertyStateCol ? cols.indexOf(propertyStateCol) : -1;

        // 1. Collect all valid identities from CSV to batch query Postgres
        const identityKeysInCsv = new Set<string>();
        for (const row of rows) {
          const first = String(row[fnIdx] ?? "").trim();
          const last = String(row[lnIdx] ?? "").trim();
          const addr = String(row[addrIdx] ?? "").trim();
          if (first && last && addr) {
            identityKeysInCsv.add(makeIdentityKey(first, last, addr));
          }
        }

        // 2. Batch query Postgres for existing contacts
        // We can't easily query by computed identityKey in Prisma without a raw query or many ORs.
        // Given the potential size, let's use a raw query or fetch in chunks if needed.
        // For simplicity and safety with Prisma, we'll fetch matches using OR conditions in chunks.
        const existingIdentityKeys = new Set<string>();
        const csvIdentities = Array.from(identityKeysInCsv);
        const BATCH_SIZE = 500;

        for (let i = 0; i < csvIdentities.length; i += BATCH_SIZE) {
          const batch = csvIdentities.slice(i, i + BATCH_SIZE);
          // We need to split the key back to query components
          const orConditions = batch.map((key) => {
            const [f, l, a] = key.split("|");
            return {
              first_name: { equals: f, mode: "insensitive" as const },
              last_name: { equals: l, mode: "insensitive" as const },
              mailing_address: { equals: a, mode: "insensitive" as const },
            };
          });

          const found = await prisma.contacts.findMany({
            where: { OR: orConditions },
            select: { first_name: true, last_name: true, mailing_address: true },
          });

          found.forEach((c) => {
            if (c.first_name && c.last_name && c.mailing_address) {
              existingIdentityKeys.add(makeIdentityKey(c.first_name, c.last_name, c.mailing_address));
            }
          });
        }

        let upsertCount = 0;

        for (const row of rows) {
          const first = String(row[fnIdx] ?? "").trim();
          const last = String(row[lnIdx] ?? "").trim();
          const addr = String(row[addrIdx] ?? "").trim();
          const propAddr = propAddrIdx !== -1 ? String(row[propAddrIdx] ?? "").trim() : "";
          const mailingCity = mailingCityIdx !== -1 ? String(row[mailingCityIdx] ?? "").trim() : "";
          const mailingState = mailingStateIdx !== -1 ? String(row[mailingStateIdx] ?? "").trim() : "";
          const mailingZip = mailingZipIdx !== -1 ? String(row[mailingZipIdx] ?? "").trim() : "";
          const propertyCity = propertyCityIdx !== -1 ? String(row[propertyCityIdx] ?? "").trim() : "";
          const propertyState = propertyStateIdx !== -1 ? String(row[propertyStateIdx] ?? "").trim() : "";
          const propertyZip = propertyZipIdx !== -1 ? String(row[propertyZipIdx] ?? "").trim() : "";

          // Skip only if ALL THREE identity fields are missing (at least 1 is required)
          if (!first && !last && !addr) continue;

          const identityKey = makeIdentityKey(first, last, addr);

          // --- Calculate 'clean' ---
          // Condition: false if required fields are empty (only checks columns that exist in CSV)
          // Condition: false if first+last contains "LLC"
          let isClean = true;
          if (
            !first ||
            !last ||
            !addr ||
            (propertyAddressCol && !propAddr) ||
            (mailingCityCol && !mailingCity) ||
            (mailingStateCol && !mailingState) ||
            (mailingZipCol && !mailingZip) ||
            (propertyCityCol && !propertyCity) ||
            (propertyStateCol && !propertyState) ||
            (propertyZipCol && !propertyZip)
          ) {
            isClean = false;
          } else {
            const ownerCompleteName = first + last;
            const llcRegex = /\bllc\b/i;
            if (llcRegex.test(ownerCompleteName)) {
              isClean = false;
            }
          }

          // --- Calculate 'inPostgres' ---
          const inPostgres = existingIdentityKeys.has(identityKey);

          // Create a flexible object with all columns
          const docData: Record<string, any> = {
            identityKey,
            jobId: job.jobId,
            botId: job.startedByBotId,
            syncedAt: new Date(),
            clean: isClean,
            inPostgres: inPostgres,
            prevList: [],
            currList: [],
            isListChanged: false,
          };

          // Add all CSV columns to the document
          cols.forEach((col, idx) => {
            const value = row[idx];

            // Normalize column name to check if it's a list field
            const normalizedCol = col.trim().toLowerCase().replace(/^"|"$/g, "");

            // Convert null/empty values to empty string for list fields
            if (normalizedCol === "list" || normalizedCol === "lists") {
              docData["prevList"] = docData["currList"];
              docData["currList"] =
                value === null || value === undefined || String(value).trim() === ""
                  ? []
                  : String(value)
                      .split(",")
                      .map((item: string) => item.trim());
              if (docData["prevList"].length !== docData["currList"].length) {
                docData["isListChanged"] = true;
              } else {
                const prevSet = new Set(docData["prevList"]);
                const currSet = new Set(docData["currList"]);
                const areSame =
                  docData["prevList"].every((item: string) => currSet.has(item)) &&
                  docData["currList"].every((item: string) => prevSet.has(item));
                docData["isListChanged"] = !areSame;
              }
            } else {
             let colKey = col
  .trim()
              .replace(/^"|"$/g, "")           // Remove leading/trailing quotes
              .toLowerCase()
              .replace(/[\s\/\-\.]+/g, "_")    // Replace spaces, slashes, dashes, dots with underscore (ALL occurrences)
              .replace(/[^a-z0-9_]/g, "")      // Remove any other special characters
              .replace(/_+/g, "_")             // Collapse multiple consecutive underscores into one
              .replace(/^_|_$/g, "");          // Remove leading/trailing underscores
              docData[colKey] = value;
            }
          });

          await ScrappedData.findOneAndUpdate(
            { identityKey }, // Filter by unique key
            { $set: docData }, // Update fields
            { upsert: true, new: true } // Create if not exists
          );
          upsertCount++;
        }
        console.log(`Synced ${upsertCount} records for job ${job.jobId}`);
      } catch (err) {
        console.error(`Error processing file ${fileName}:`, err);
      }
    }
    console.log("MongoDB sync completed.");
  } catch (error) {
    console.error("Error in syncScrappedData:", error);
  }
};
