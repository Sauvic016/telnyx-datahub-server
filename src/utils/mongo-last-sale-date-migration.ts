import { parse, isValid } from "date-fns";
import { PropertyData } from "../models/PropertyData";

export async function migrateLastSaleDateStrings(): Promise<void> {
  console.log("🔄 Starting last_sale_date migration...");

  const count = await PropertyData.countDocuments({
    last_sale_date: { $type: "string" },
  });
  console.log(`Found ${count} documents with string dates`);

  if (count === 0) {
    console.log("✅ No string dates to migrate");
    return;
  }

  const cursor = PropertyData.find({
    last_sale_date: { $type: "string" },
  }).cursor();

  let processed = 0;
  let migrated = 0;
  let skipped = 0;

  for await (const doc of cursor) {
    processed++;

    const raw = String(doc.last_sale_date).trim();
    let parsed: Date | null = null;

    // 1️⃣ ISO date string
    const isoCandidate = new Date(raw);
    if (!isNaN(isoCandidate.getTime())) {
      parsed = isoCandidate;
    } else {
      // 2️⃣ Legacy CSV format (MMM-dd-yyyy)
      const legacy = parse(raw, "MMM-dd-yyyy", new Date());
      if (isValid(legacy)) {
        parsed = legacy;
      }
    }

    if (!parsed) {
      console.warn(`⚠️ Invalid last_sale_date for ${doc._id}:`, raw);
      skipped++;
      continue;
    }

    // Bypass Mongoose schema casting
    await PropertyData.collection.updateOne({ _id: doc._id }, { $set: { last_sale_date: parsed } });
    migrated++;

    if (processed % 1000 === 0) {
      console.log(`  Processed ${processed}/${count}...`);
    }
  }

  console.log("✅ last_sale_date migration complete", {
    processed,
    migrated,
    skipped,
  });
}
