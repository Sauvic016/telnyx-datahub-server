import path from "path";
import fs from "fs";
import pl from "nodejs-polars";
import { getLatestJobsPerBot, getAllJobs } from "../utils/helper";
import { ScrappedData } from "../models/ScrappedData";
import prisma from "../db";

const mongoToPropertyDetailsMap: Record<string, string> = {
  // ─── PROPERTY ADDRESS ───────────────────────────
  property_address: "property_address",
  property_city: "property_city",
  property_state: "property_state",
  property_zip: "property_zip_code",
  property_zip_code: "property_zip_code", // CSV variation
  list_stack: "list_stack",

  // ─── PROPERTY CHARACTERISTICS ───────────────────
  bedrooms: "bedrooms",
  bathrooms: "bathrooms",
  sqft: "square_feet",
  square_feet: "square_feet", // CSV variation
  air_conditioner: "air_conditioner",
  heating_type: "heat",
  heat: "heat", // CSV variation
  storeys: "storeys",
  year: "year_built",
  year_built: "year_built", // CSV variation
  above_grade: "above_grade",
  rental_value: "rental_value",

  // ─── PROPERTY CLASSIFICATION ────────────────────
  building_use_code: "land_use_code",
  land_use_code: "land_use_code", // CSV variation
  neighborhood_rating: "cdu",
  cdu: "cdu", // CSV variation
  structure_type: "structure_type",
  number_of_units: "number_of_units",

  // ─── LAND / PARCEL ──────────────────────────────
  apn: "apn",
  parcel_id: "parcel",
  parcel: "parcel", // CSV variation
  legal_description: "legal_description",
  lot_size: "lot_size",
  land_zoning: "land_zoning",

  // ─── TAX DATA ───────────────────────────────────
  tax_auction_date: "tax_auction_date",
  total_taxes: "total_taxes",
  tax_delinquent_value: "tax_delinquent_amount",
  tax_delinquent_amount: "tax_delinquent_amount", // CSV variation
  tax_delinquent_year: "tax_delinquent_year",
  year_behind_on_taxes: "years_delinquent",
  years_delinquent: "years_delinquent", // CSV variation

  // ─── DEED / MLS ─────────────────────────────────
  deed: "deed",
  mls: "mls",

  // ─── SALE HISTORY ───────────────────────────────
  last_sale_price: "last_sale_price",
  last_sold: "last_sale_date",
  last_sale_date: "last_sale_date", // CSV variation
  previous_sale_date: "previous_sale_date",
  previous_sale_price: "previous_sale_price",

  // ─── LIENS / LEGAL EVENTS ───────────────────────
  lien_type: "tax_lien",
  tax_lien: "tax_lien", // CSV variation
  lien_recording_date: "lien_recording_date",

  personal_representative: "personal_representative",
  personal_representative_phone: "personal_representative_phone",
  probate_open_date: "probate_open_date",
  attorney_on_file: "attorney_on_file",

  // ─── FORECLOSURE / BANKRUPTCY ───────────────────
  foreclosure_date: "foreclosure_date",
  foreclosure: "foreclosure", // CSV variation
  bankruptcy_recording_date: "bankruptcy_recording_date",
  bankruptcy: "bankruptcy", // CSV variation
  divorce_file_date: "divorce_file_date",

  // ─── MORTGAGE DATA ──────────────────────────────
  loan_to_value: "loan_to_value",
  open_mortgages: "open_mortgages",
  mortgage_type: "mortgage_type",

  // ─── OWNERSHIP / VALUE ──────────────────────────
  owned_since: "previous_sale_date",
  estimated_value: "estimated_value",
};

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

export const syncScrappedDataOptimized = async () => {
  try {
    console.log("Starting MongoDB sync (Optimized)...");
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

        if (!firstNameCol && !lastNameCol && !mailingAddressCol) {
          console.warn(`Missing required identity columns in ${fileName}. Skipping.`);
          continue;
        }

        const rows = df.rows() as unknown[][];
        const fnIdx = firstNameCol ? cols.indexOf(firstNameCol) : -1;
        const lnIdx = lastNameCol ? cols.indexOf(lastNameCol) : -1;
        const addrIdx = mailingAddressCol ? cols.indexOf(mailingAddressCol) : -1;
        const propAddrIdx = propertyAddressCol ? cols.indexOf(propertyAddressCol) : -1;
        const mailingZipIdx = mailingZipCol ? cols.indexOf(mailingZipCol) : -1;
        const mailingCityIdx = mailingCityCol ? cols.indexOf(mailingCityCol) : -1;
        const mailingStateIdx = mailingStateCol ? cols.indexOf(mailingStateCol) : -1;
        const propertyZipIdx = propertyZipCol ? cols.indexOf(propertyZipCol) : -1;
        const propertyCityIdx = propertyCityCol ? cols.indexOf(propertyCityCol) : -1;
        const propertyStateIdx = propertyStateCol ? cols.indexOf(propertyStateCol) : -1;

        // 1. Collect all valid identities from CSV to batch query Postgres & Mongo
        const identityKeysInCsv = new Set<string>();
        for (const row of rows) {
          const first = String(row[fnIdx] ?? "").trim();
          const last = String(row[lnIdx] ?? "").trim();
          const addr = String(row[addrIdx] ?? "").trim();
          if (first && last && addr) {
            identityKeysInCsv.add(makeIdentityKey(first, last, addr));
          }
        }

        // 2. Batch query Postgres for existing contacts (for 'inPostgres' flag)
        const existingIdentityKeys = new Set<string>();
        const csvIdentities = Array.from(identityKeysInCsv);
        const BATCH_SIZE = 500;

        for (let i = 0; i < csvIdentities.length; i += BATCH_SIZE) {
          const batch = csvIdentities.slice(i, i + BATCH_SIZE);
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

        // 3. Batch query MongoDB for existing records (to get prevList)
        // We fetch only the fields we need to determine transition state
        const mongoExisting = await ScrappedData.find(
          { identityKey: { $in: Array.from(identityKeysInCsv) } },
          { identityKey: 1, currList: 1 }
        ).lean();

        // Create a lookup map for O(1) access
        const listMap = new Map(mongoExisting.map((d: any) => [d.identityKey, d.currList || []]));

        const bulkOps: any[] = [];

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

          // --- Calculate Lists (Transition Logic) ---
          const prevList = listMap.get(identityKey) || [];
          let currList: string[] = [];
          let isListChanged = false;

          const docData: Record<string, any> = {
            identityKey,
            jobId: job.jobId,
            botId: job.startedByBotId,
            syncedAt: new Date(),
            clean: isClean,
            inPostgres: inPostgres,
            prevList: prevList,
            // currList will be set below
          };

          let property_data: Record<string, any> = {};
          
          cols.forEach((col, idx) => {
            const value = row[idx];

            const normalizedCol = col
              .trim()
              .replace(/^"|"$/g, "")
              .toLowerCase()
              .replace(/[\s\/\-\.]+/g, "_")
              .replace(/[^a-z0-9_]/g, "")
              .replace(/_+/g, "_")
              .replace(/^_|_$/g, "");

            if (normalizedCol === "list" || normalizedCol === "lists") {
              currList =
                value === null || value === undefined || String(value).trim() === ""
                  ? []
                  : String(value)
                      .split(",")
                      .map((item: string) => item.trim());
            }

            if (normalizedCol in mongoToPropertyDetailsMap) {
              property_data[mongoToPropertyDetailsMap[normalizedCol]] = value;
            } else {
              let colKey = col
                .trim()
                .replace(/^"|"$/g, "")
                .toLowerCase()
                .replace(/[\s\/\-\.]+/g, "_")
                .replace(/[^a-z0-9_]/g, "")
                .replace(/_+/g, "_")
                .replace(/^_|_$/g, "");
              docData[colKey] = value;
            }
          });

          docData["currList"] = currList;

          // Calculate isListChanged based on prevList (from DB) and currList (from CSV)
          if (prevList.length !== currList.length) {
            isListChanged = true;
          } else {
            const prevSet = new Set(prevList);
            const currSet = new Set(currList);
            const areSame =
              prevList.every((item: string) => currSet.has(item)) &&
              currList.every((item: string) => prevSet.has(item));
            isListChanged = !areSame;
          }
          docData["isListChanged"] = isListChanged;

          // CRITICAL FIX: Update the in-memory map so that if this same identityKey
          // appears in a subsequent job in this same loop, it sees the NEW list as "prevList".
          listMap.set(identityKey, currList);

          // Construct the update operation
          // We want to $set the main fields and $push the new property_data
          // However, if we use $set for everything, we overwrite property_datas.
          // To append property_datas, we use $push.
          
          // We remove property_datas from docData because we will $push it separately
          // But wait, if the document is new (upsert), we need to create the array.
          // $push works on upsert too (it creates the array).
          
          bulkOps.push({
            updateOne: {
              filter: { identityKey },
              update: {
                $set: docData,
                $addToSet: { property_datas: property_data }
              },
              upsert: true,
            },
          });
        }

        if (bulkOps.length > 0) {
          console.log(`Bulk writing ${bulkOps.length} records for job ${job.jobId}...`);
          await ScrappedData.bulkWrite(bulkOps);
          console.log(`Synced ${bulkOps.length} records for job ${job.jobId}`);
        } else {
            console.log(`No records to sync for job ${job.jobId}`);
        }

      } catch (err) {
        console.error(`Error processing file ${fileName}:`, err);
      }
    }
    console.log("MongoDB sync (Optimized) completed.");
  } catch (error) {
    console.error("Error in syncScrappedDataOptimized:", error);
  }
};
