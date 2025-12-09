import axios from "axios";
import { ScrappedData } from "../models/ScrappedData";
import prisma from "../db";
import { ProcessingStage } from "../generated/prisma/enums";
import { makeIdentityKey } from "../utils/helper";
import dotenv from "dotenv";
dotenv.config();

const DIRECTSKIP_SERVER_URL = process.env.DIRECTSKIP_SERVER_URL || "http://localhost:4000";



type DirectSkipRowPayload = {
  row: any;
};

/**
 * Utility function to split array into chunks
 */
function chunk<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

/**
 * Send approved records to DirectSkip server asynchronously (fire-and-forget)
 * Creates batch records in DB and sends without waiting for response
 * @param approvedIdentityKeys - Array of identityKeys that are approved
 * @returns Array of batch IDs created
 */
export async function sendApprovedToDirectSkip(approvedIdentityKeys: string[]): Promise<string[]> {
  const batchIds: string[] = [];

  if (!approvedIdentityKeys || approvedIdentityKeys.length === 0) {
    console.warn("[DirectSkip] No approved identityKeys provided");
    return batchIds;
  }

  console.log(`[DirectSkip] Preparing ${approvedIdentityKeys.length} approved records`);

  // Query MongoDB documents by identityKey
  const docs = await ScrappedData.find({
    identityKey: { $in: approvedIdentityKeys },
  }).lean();

  if (docs.length === 0) {
    console.warn(`[DirectSkip] No matching documents found in MongoDB for approved identityKeys`);
    return batchIds;
  }

  console.log(`[DirectSkip] Found ${docs.length} matching documents in MongoDB`);

  const rowsToSend: DirectSkipRowPayload[] = [];
  const identityKeys: string[] = [];

  for (const doc of docs) {
    // Extract identity fields
    const firstName =
     doc.owner_first_name;
    const lastName = doc.owner_last_name;
    const mailingAddress = doc.mailing_address;

    if (!firstName || !lastName || !mailingAddress) {
      continue; // Skip documents without complete identity
    }

    const identityKey = makeIdentityKey(firstName, lastName, mailingAddress);

    // Verify this document matches one of our approved identityKeys
    if (!approvedIdentityKeys.includes(identityKey)) {
      continue;
    }

    identityKeys.push(identityKey);

    // Pick the relevant fields for DirectSkip
    const row = {
       firstName,
          lastName,
          mailingAddress,
          mailingCity: doc.mailing_city,
          mailingState: doc.mailing_state,
          mailingZip: doc.mailing_zip_code,

          propertyAddress: doc.property_address,
          propertyCity: doc.property_city,
          propertyState: doc.property_state,
          propertyZip: doc.property_zip_code,
      identityKey: doc.identityKey,
    };

    rowsToSend.push({
      row,
    });
  }

  if (rowsToSend.length === 0) {
    console.warn(`[DirectSkip] No valid documents to send after filtering`);
    return batchIds;
  }

  console.log(`[DirectSkip] Prepared ${rowsToSend.length} documents to send to DirectSkip`);

  // Create a batch record in DB
  const batch = await prisma.directSkipBatch.create({
    data: {
      rowCount: rowsToSend.length,
      status: "PENDING",
    },
  });

  batchIds.push(batch.id);
  console.log(`[DirectSkip] Created batch ${batch.id} with ${rowsToSend.length} rows`);

  // Update rows to SENT_TO_DIRECTSKIP using Pipeline table
  if (identityKeys.length > 0) {
    await prisma.pipeline.updateMany({
      where: {
        identityKey: { in: identityKeys },
      },
      data: {
        stage: ProcessingStage.SENT_TO_DIRECTSKIP,
      },
    });
  }

  // Send asynchronously without waiting (fire-and-forget)
  sendBatchAsync(batch.id, rowsToSend).catch((error) => {
    console.error(`[DirectSkip] ❌ Async send failed for batch ${batch.id}:`, error);
  });

  return batchIds;
}

/**
 * Internal function to send batch asynchronously
 * Updates batch status in DB and sends to DirectSkip server
 */
async function sendBatchAsync(batchId: string, rowsToSend: DirectSkipRowPayload[]): Promise<void> {
  try {
    console.log(`[DirectSkip] 🚀 Starting async send for batch ${batchId}`);

    // Update status to SUBMITTED
    await prisma.directSkipBatch.update({
      where: { id: batchId },
      data: { status: "SUBMITTED" },
    });

    // Split into chunks of 500
    const batches = chunk(rowsToSend, 500);

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`[DirectSkip] Sending chunk ${i + 1}/${batches.length} for batch ${batchId}`);

      const response = await axios.post(
        `${DIRECTSKIP_SERVER_URL}/run-directskip`,
        {
          rows: batch,
          batchId, // Send our batch ID for reference
        },
        {
          timeout: 30000, // 30 second timeout
        }
      );

      console.log(`[DirectSkip] ✅ Chunk ${i + 1} sent, response:`, response.data);

      // Store DirectSkip job ID if returned
      if (response.data?.jobId) {
        await prisma.directSkipBatch.update({
          where: { id: batchId },
          data: {
            status: "PROCESSING",
          },
        });
      }
    }

    console.log(`[DirectSkip] ✅ All chunks sent for batch ${batchId}`);
  } catch (error) {
    console.error(`[DirectSkip] ❌ Error sending batch ${batchId}:`, error);

    // Update batch with error
    await prisma.directSkipBatch.update({
      where: { id: batchId },
      data: {
        status: "FAILED",
        error: error instanceof Error ? error.message : String(error),
      },
    });

    throw error;
  }
}
