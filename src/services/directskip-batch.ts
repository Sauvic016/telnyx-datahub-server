import axios from "axios";
import { ScrappedData } from "../models/ScrappedData";
import prisma from "../db";
import { ProcessingStage } from "../generated/prisma/enums";
import dotenv from "dotenv";
dotenv.config();

const DIRECTSKIP_SERVER_URL = process.env.DIRECTSKIP_SERVER_URL || "http://localhost:4000";

export async function sendApprovedToDirectSkip(identityKeyList: string[]): Promise<number> {
  if (!identityKeyList || identityKeyList.length === 0) {
    console.warn("[DirectSkip] No approved identityKeys provided");
    return identityKeyList.length;
  }

  console.log(`[DirectSkip] Preparing ${identityKeyList.length} approved records`);

  // Query MongoDB documents by identityKey
  const docs = await ScrappedData.find({
    identityKey: { $in: identityKeyList },
  }).lean();

  if (docs.length === 0) {
    console.warn(`[DirectSkip] No matching documents found in MongoDB for approved identityKeys`);
    return identityKeyList.length;
  }

  console.log(`[DirectSkip] Found ${docs.length} matching documents in MongoDB`);

  const rowsToSend = docs.map((doc) => ({
    firstName: doc.owner_first_name,
    lastName: doc.owner_last_name,
    mailingAddress: doc.mailing_address,
    mailingCity: doc.mailing_city,
    mailingState: doc.mailing_state,
    mailingZip: doc.mailing_zip_code,
    propertyAddress: doc.property_address,
    propertyCity: doc.property_city,
    propertyState: doc.property_state,
    propertyZip: doc.property_zip_code,
    identityKey: doc.identityKey,
  }));

  // Update rows to SENT_TO_DIRECTSKIP using Pipeline table
  await prisma.pipeline.updateMany({
    where: {
      identityKey: { in: identityKeyList },
    },
    data: {
      stage: ProcessingStage.SENT_TO_DIRECTSKIP,
    },
  });
  await ScrappedData.updateMany(
    { identityKey: { $in: identityKeyList } },
    {
      $set: {
        stage: ProcessingStage.SENT_TO_DIRECTSKIP,
      },
    }
  );

  try {
    await axios.post(`${DIRECTSKIP_SERVER_URL}/run-directskip`, {
      rows: rowsToSend,
    });
  } catch (error) {
    console.error(`[DirectSkip] ❌ Error sending to directskip server}:`, error);
    throw error;
  }

  return rowsToSend.length;
}
