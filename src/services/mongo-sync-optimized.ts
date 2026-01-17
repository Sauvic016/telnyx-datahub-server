import path from "path";
import fs from "fs";
import pl from "nodejs-polars";
import { getLatestJobsPerBot, getAllJobs, makeIdentityKey, getAllJobsAfterLastSync } from "../utils/helper";
import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
import prisma from "../db";
import mongoose from "../mongoose";

type ListType = {
  name: string;
  list_updated_at: Date;
};

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

  pay_all_current_taxes: "pay_all_current_taxes",
  pay_current_installment: "pay_current_installment",
  pay_delinquent_taxes: "pay_delinquent_taxes",
  pay_second_installment: "pay_second_installment",
  vacant_abandon: "vacant_abandon",

  // ─── SPECIAL ASSESSMENTS ────────────────────────
  special_assessment_amount_2021: "special_assessment_amount_2021",
  special_assessment_amount_2022: "special_assessment_amount_2022",
  special_assessment_amount_2023: "special_assessment_amount_2023",
  special_assessment_amount_2024: "special_assessment_amount_2024",
  special_assessment_amount_2025: "special_assessment_amount_2025",

  special_assessments_2021: "special_assessments_2021",
  special_assessments_2022: "special_assessments_2022",
  special_assessments_2023: "special_assessments_2023",
  special_assessments_2024: "special_assessments_2024",
  special_assessments_2025: "special_assessments_2025",

  special_assessments_code_2021: "special_assessments_code_2021",
  special_assessments_code_2022: "special_assessments_code_2022",
  special_assessments_code_2023: "special_assessments_code_2023",
  special_assessments_code_2024: "special_assessments_code_2024",
  special_assessments_code_2025: "special_assessments_code_2025",
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

export const syncScrappedDataOptimized = async (manual?: boolean) => {
  try {
    console.log("Starting MongoDB sync (Optimized)...");
    let latestJobs;
    if (manual) {
      latestJobs = await getAllJobsAfterLastSync();
    } else {
      latestJobs = await getLatestJobsPerBot();
    }

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
        const parcelStateCol = pickColumn(cols, ["parcel", "Parcel"]);

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
        const parcelIndex = parcelStateCol ? cols.indexOf(parcelStateCol) : -1;

        // 1. Collect all valid identities from CSV to batch query Postgres & Mongo
        const ownerIdentityKeys = new Set<string>();
        const propertyIdentityKeys = new Set<string>();

        const updatedPropertyMap = new Map();
        const updatedOwnerMap = new Map();

        const ownerIdentityKeyToPropertyMap = new Map<string, any[]>();

        for (const row of rows) {
          // propertyIdentityKey
          const propAddr = propAddrIdx !== -1 ? String(row[propAddrIdx] ?? "").trim() : "";
          const propertyCity = propertyCityIdx !== -1 ? String(row[propertyCityIdx] ?? "").trim() : "";
          const propertyState = propertyStateIdx !== -1 ? String(row[propertyStateIdx] ?? "").trim() : "";
          const propertyZip = propertyZipIdx !== -1 ? String(row[propertyZipIdx] ?? "").trim() : "";
          const propertyIdentityKey = makeIdentityKey(propAddr, propertyCity, propertyState, propertyZip);
          propertyIdentityKeys.add(propertyIdentityKey);

          //owner IdentityKey
          const first = String(row[fnIdx] ?? "").trim();
          const last = String(row[lnIdx] ?? "").trim();
          const addr = String(row[addrIdx] ?? "").trim();
          const ownerIdentityKey = makeIdentityKey(first, last, addr);
          ownerIdentityKeys.add(ownerIdentityKey);

          const propertyIdentityKeyList = ownerIdentityKeyToPropertyMap.get(ownerIdentityKey) ?? [];
          propertyIdentityKeyList.push(propertyIdentityKey);
          ownerIdentityKeyToPropertyMap.set(ownerIdentityKey, propertyIdentityKeyList);
        }

        const existingProperties = await PropertyData.find({
          identityKey: { $in: [...propertyIdentityKeys] },
        }).lean();

        const existingPropertyMap = new Map(existingProperties.map((doc) => [doc.identityKey, doc]));

        const existingOwner = await Owner.find({
          identityKey: { $in: [...ownerIdentityKeys] },
        }).lean();

        const existingOwnerMap = new Map(existingOwner.map((doc) => [doc.identityKey, doc]));

        for (const row of rows) {
          // property related data
          const propAddr = propAddrIdx !== -1 ? String(row[propAddrIdx] ?? "").trim() : "";
          const propertyCity = propertyCityIdx !== -1 ? String(row[propertyCityIdx] ?? "").trim() : "";
          const propertyState = propertyStateIdx !== -1 ? String(row[propertyStateIdx] ?? "").trim() : "";
          const propertyZip = propertyZipIdx !== -1 ? String(row[propertyZipIdx] ?? "").trim() : "";
          const propertyIdentityKey = makeIdentityKey(propAddr, propertyCity, propertyState, propertyZip);

          //owner related data
          const first = String(row[fnIdx] ?? "").trim();
          const last = String(row[lnIdx] ?? "").trim();
          const addr = String(row[addrIdx] ?? "").trim();
          const ownerIdentityKey = makeIdentityKey(first, last, addr);

          const mailingCity = mailingCityIdx !== -1 ? String(row[mailingCityIdx] ?? "").trim() : "";
          const mailingState = mailingStateIdx !== -1 ? String(row[mailingStateIdx] ?? "").trim() : "";
          const mailingZip = mailingZipIdx !== -1 ? String(row[mailingZipIdx] ?? "").trim() : "";

          // --- Calculate 'clean' ---
          let isPropertyClean = true;
          let isOwnerClean = true;
          if (
            !first ||
            !last ||
            !addr ||
            (mailingCityCol && !mailingCity) ||
            (mailingStateCol && !mailingState) ||
            (mailingZipCol && !mailingZip)
          ) {
            isOwnerClean = false;
          } else {
            const ownerCompleteName = first + last;
            const llcRegex = /\bllc\b/i;
            if (llcRegex.test(ownerCompleteName)) {
              isOwnerClean = false;
            }
          }

          if (
            !propAddr ||
            (propertyCityCol && !propertyCity) ||
            (propertyStateCol && !propertyState) ||
            (propertyZipCol && !propertyZip)
          ) {
            isPropertyClean = false;
          }

          let propertyData: Record<string, any> = { prevList: [], currList: [], clean: isPropertyClean };
          let ownerData: Record<string, any> = {
            propertyIds: [],
            clean: isOwnerClean,
            inPostgres: existingOwnerMap.get(ownerIdentityKey) ?? false,
            jobId: job.jobId,
            botId: job.startedByBotId,
          };

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
              const listNames =
                value === null || value === undefined || String(value).trim() === ""
                  ? []
                  : String(value)
                      .split(",")
                      .map((s: string) => s.trim())
                      .filter((s: string) => s.length > 0);

              propertyData.currList = listNames.map((name: string) => ({
                name: name,
                list_updated_at: job.updatedAt,
              }));
            }

            if (normalizedCol in mongoToPropertyDetailsMap) {
              propertyData[mongoToPropertyDetailsMap[normalizedCol]] = value;
            } else {
              ownerData[normalizedCol] = value;
            }
          });

          if (existingPropertyMap.has(propertyIdentityKey)) {
            propertyData.prevList = existingPropertyMap.get(propertyIdentityKey)?.currList;
          }

          propertyData.isListChanged = false;
          const prevNames = new Set(
            propertyData.prevList.map((item: any) => (typeof item === "string" ? item : item.name)),
          );
          const currNames = new Set(propertyData.currList.map((item: any) => item.name));

          if (prevNames.size !== currNames.size) {
            propertyData.isListChanged = true;
          } else {
            for (const name of prevNames) {
              if (!currNames.has(name)) {
                propertyData.isListChanged = true;
                break;
              }
            }
          }

          updatedPropertyMap.set(propertyIdentityKey, propertyData);
          // updatedOwnerMap.set(ownerIdentityKey, ownerData);
          updatedOwnerMap.set(ownerIdentityKey, {
            ...ownerData,
            inPostgres: ownerData.inPostgres,
            clean: ownerData.clean,
            treasurer_code: ownerData.treasurer_code,
            delinquent_contract: ownerData.delinquent_contract,
            owner_2_first_name: ownerData.owner_2_first_name,
            owner_2_last_name: ownerData.owner_2_last_name,
          });
        }

        const bulkOps = Array.from(updatedPropertyMap.entries()).map(([identityKey, data]) => ({
          updateOne: {
            filter: { identityKey },
            update: {
              $set: {
                ...data,
                updatedAt: job.updatedAt,
                syncedAt: new Date(),
              },
              $setOnInsert: {
                identityKey,
                createdAt: job.createdAt,
              },
            },
            upsert: true,
          },
        }));

        let result = {} as any;
        if (bulkOps.length > 0) {
          result = await PropertyData.bulkWrite(bulkOps, {
            ordered: false, // continues even if one op fails
          });

          console.log("Bulk upsert result:", {
            inserted: result.upsertedCount,
            modified: result.modifiedCount,
            matched: result.matchedCount,
          });
        }
        const extractedPropertyIdentityKeys = Array.from(updatedPropertyMap.keys());

        const properties = await PropertyData.find(
          { identityKey: { $in: extractedPropertyIdentityKeys } },
          { _id: 1, identityKey: 1 },
        ).lean();

        // Create a map of identityKey -> _id
        // Ensure we handle potential duplicate identityKeys in the DB by taking the last one (or handle as needed)
        const insertedMap: Record<string, any> = {};
        properties.forEach((p) => {
          insertedMap[p.identityKey] = p._id;
        });

        console.log(`Resolved ${Object.keys(insertedMap).length} property IDs.`);
        console.log("before", ownerIdentityKeyToPropertyMap);

        for (const [key, values] of ownerIdentityKeyToPropertyMap) {
          const validPropertyIds = values.map((value) => insertedMap[value]).filter((id) => id); // Remove undefined/null values

          ownerIdentityKeyToPropertyMap.set(key, validPropertyIds);

          if (ownerIdentityKeyToPropertyMap.get(key)?.length === 0 && values.length > 0) {
            console.warn(`⚠️ Owner ${key} has ${values.length} property keys but 0 resolved IDs.`);
          }
        }

        const identityKeys = Array.from(updatedOwnerMap.keys());
        const existingOwners = await Owner.find({ identityKey: { $in: identityKeys } }, { identityKey: 1 }).lean();
        console.log("existingOwners", existingOwners);
        const existingKeySet = new Set(existingOwners.map((o) => o.identityKey));

        const updateOps = [];

        const toMongooseObjectIds = (ids: any[]) =>
          ids.filter(Boolean).map((id) => {
            if (id instanceof mongoose.Types.ObjectId) {
              return id; // already correct
            }
            return new mongoose.Types.ObjectId(id);
          });

        for (const [identityKey, data] of updatedOwnerMap.entries()) {
          if (!existingKeySet.has(identityKey)) continue;

          const rawPropertyIds = ownerIdentityKeyToPropertyMap.get(identityKey) ?? [];
          const propertyIds = toMongooseObjectIds(rawPropertyIds);

          const softSafeData = { ...data };
          delete softSafeData._id;
          delete softSafeData.identityKey;
          delete softSafeData.propertyIds;
          delete softSafeData.createdAt;
          delete softSafeData.updatedAt;
          delete softSafeData.__v;

          const safeData = {
            ...softSafeData, // ← convenience
            inPostgres: Boolean(data.inPostgres),
            clean: Boolean(data.clean),
            treasurer_code: data.treasurer_code ?? null,
            delinquent_contract: data.delinquent_contract ?? null,
            ...(data.owner_2_first_name !== undefined && {
              owner_2_first_name: data.owner_2_first_name,
            }),
            ...(data.owner_2_last_name !== undefined && {
              owner_2_last_name: data.owner_2_last_name,
            }),
          };

          const update: any = {
            $set: {
              ...safeData,
              updatedAt: job.updatedAt,
              syncedAt: new Date(),
            },
          };

          if (propertyIds.length > 0) {
            update.$addToSet = {
              propertyIds: { $each: propertyIds },
            };
          }

          updateOps.push({
            updateOne: {
              filter: { identityKey },
              update,
            },
          });
        }

        const newOwners = [];

        for (const [identityKey, data] of updatedOwnerMap.entries()) {
          if (existingKeySet.has(identityKey)) continue;
          const propertyIds = ownerIdentityKeyToPropertyMap.get(identityKey) ?? [];
          newOwners.push({
            identityKey,
            ...data,
            propertyIds,
            createdAt: job.createdAt,
            updatedAt: job.updatedAt,
            syncedAt: new Date(),
          });
        }

        if (updateOps.length > 0) {
          const res = await Owner.bulkWrite(updateOps, { ordered: false });

          res.mongoose?.results?.forEach((result: any, index: number) => {
            if (!result) return;

            console.error(`\n❌ Error in updateOps[${index}]`);

            console.error("name:", result.name);
            console.error("message:", result.message);

            if (result.path) {
              console.error("path:", result.path);
            }

            if (result.value !== undefined) {
              console.error("value:", result.value);
            }

            if (result.reason) {
              console.error("reason:", result.reason?.message);
            }
          });
        }

        if (newOwners.length > 0) {
          console.log("newOwners", newOwners);
          await Owner.insertMany(newOwners, { ordered: false });
        }

        console.log(`Synced identities for job ${job.jobId}`);
      } catch (err) {
        console.error(`Error processing file ${fileName}:`, err);
      }
    }
    console.log("MongoDB sync (Optimized) completed.");
  } catch (error) {
    console.error("Error in syncScrappedDataOptimized:", error);
  }
};
