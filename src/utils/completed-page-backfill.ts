import prisma from "../db";
import { PropertyData } from "../models/PropertyData";
import mongoose from "../mongoose";

export const backfillSaleAndCaseDates = async () => {
  // 1. Get propertyId and propertyDetailsId from pipeline
  const pipelineRows = await prisma.pipeline.findMany({
    where: {
      propertyDetailsId: { not: null },
    },
    select: {
      propertyId: true,
      propertyDetailsId: true,
    },
  });

  if (pipelineRows.length === 0) {
    console.log("No pipeline rows with propertyDetailsId found.");
    return;
  }

  // 2. Fetch sale_date and case_date from MongoDB PropertyData
  const validRows = pipelineRows.filter((r) => r.propertyDetailsId);
  const propertyObjectIds = validRows.map(
    (r) => new mongoose.Types.ObjectId(r.propertyId)
  );

  const properties = await PropertyData.find(
    { _id: { $in: propertyObjectIds } },
    { _id: 1, sale_date: 1, case_date: 1 }
  ).lean();

  if (properties.length === 0) {
    console.log("No matching properties found in MongoDB.");
    return;
  }

  // 3. Update each property_details row with the dates
  let updated = 0;
  const skippedNoMongoProp: string[] = [];
  const skippedNoDates: string[] = [];
  const errors: { propertyId: string; error: string }[] = [];

  for (const row of validRows) {
    const prop = properties.find((p) => p._id.toString() === row.propertyId);
    if (!prop) {
      skippedNoMongoProp.push(row.propertyId);
      continue;
    }

    const saleDate = prop.sale_date ?? null;
    const caseDate = prop.case_date ?? null;

    if (!saleDate && !caseDate) {
      skippedNoDates.push(row.propertyId);
      continue;
    }

    try {
      await prisma.property_details.update({
        where: { id: row.propertyDetailsId! },
        data: {
          ...(saleDate && { sale_date: new Date(saleDate) }),
          ...(caseDate && { case_date: new Date(caseDate) }),
        },
      });
      updated++;
    } catch (err: any) {
      errors.push({ propertyId: row.propertyId, error: err.message });
    }
  }

  console.log(`\n--- Backfill Summary ---`);
  console.log(`Total pipeline rows: ${validRows.length}`);
  console.log(`Found in MongoDB: ${properties.length}`);
  console.log(`Updated: ${updated}`);
  console.log(`Skipped (not found in MongoDB): ${skippedNoMongoProp.length}`);
  if (skippedNoMongoProp.length > 0) console.log(`  IDs: ${skippedNoMongoProp.join(", ")}`);
  console.log(`Skipped (no sale_date or case_date): ${skippedNoDates.length}`);
  if (skippedNoDates.length > 0) console.log(`  IDs: ${skippedNoDates.join(", ")}`);
  console.log(`Errors: ${errors.length}`);
  if (errors.length > 0) errors.forEach((e) => console.log(`  ${e.propertyId}: ${e.error}`));
};
