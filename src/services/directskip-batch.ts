import axios from "axios";
import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
import prisma from "../db";
import { ProcessingStage } from "../generated/prisma/enums";
import dotenv from "dotenv";
dotenv.config();

const DIRECTSKIP_SERVER_URL = process.env.DIRECTSKIP_SERVER_URL || "http://localhost:4000";

type TotalKeyListType = {
  identityKey: string;
  ownerId: string;
  propertyId: string;
};

function isPOBox(address?: string | null): boolean {
  if (!address) return false;
  return /\bp\s*\.?\s*o\s*\.?\s*box\b/i.test(address);
}

export async function sendApprovedToDirectSkip(totalKeyList: TotalKeyListType[]): Promise<number> {
  if (!totalKeyList || totalKeyList.length === 0) {
    console.warn("[DirectSkip] No approved identityKeys provided");
    return 0;
  }

  console.log(`[DirectSkip] Preparing ${totalKeyList.length} approved records`);

  const ownerIds = [...new Set(totalKeyList.map((k) => k.ownerId))];
  const propertyIds = [...new Set(totalKeyList.map((k) => k.propertyId))];
  const identityKeys = [...new Set(totalKeyList.map((k) => k.identityKey))];
  console.log(totalKeyList);

  const docs = await Owner.find({
    _id: { $in: ownerIds },
  }).lean();

  const propertyDocs = await PropertyData.find({
    _id: { $in: propertyIds },
  });

  if (docs.length === 0) {
    console.warn(`[DirectSkip] No matching documents found in MongoDB for approved identityKeys`);
    return totalKeyList.length;
  }

  console.log(`[DirectSkip] Found ${docs.length} matching documents in MongoDB`);

  const ownerMap = new Map(docs.map((doc: any) => [doc._id.toString(), doc]));
  const propertyMap = new Map(propertyDocs.map((doc: any) => [doc._id.toString(), doc]));
  console.log("ownerMap", ownerMap);

  const rowsToSend = totalKeyList
    .map((item) => {
      const ownerDetail = ownerMap.get(item.ownerId);

      const propertyForOwnerDetail = propertyMap.get(item.propertyId);
      if (!ownerDetail || !propertyForOwnerDetail) return null;

      return {
        firstName: ownerDetail.owner_first_name,
        lastName: ownerDetail.owner_last_name,
        mailingAddress: isPOBox(ownerDetail.mailing_address)
          ? propertyForOwnerDetail.property_address
          : ownerDetail.mailing_address,
        mailingCity: ownerDetail.mailing_city,
        mailingState: ownerDetail.mailing_state,
        mailingZip: ownerDetail.mailing_zip_code,
        propertyAddress: propertyForOwnerDetail.property_address || "",
        propertyCity: propertyForOwnerDetail.property_city || "",
        propertyState: propertyForOwnerDetail.property_state || "",
        propertyZip: propertyForOwnerDetail.property_zip_code || "",
        identityKey: ownerDetail.identityKey,
        propertyId: item.propertyId,
        ownerId: item.ownerId,
      };
    })
    .filter((row): row is NonNullable<typeof row> => row !== null);
  console.log(rowsToSend);

  // Update rows to SENT_TO_DIRECTSKIP using Pipeline table
  await prisma.pipeline.updateMany({
    where: {
      ownerId: { in: ownerIds },
      propertyId: { in: propertyIds },
    },
    data: {
      stage: ProcessingStage.SENT_TO_DIRECTSKIP,
    },
  });

  await Owner.updateMany(
    { _id: { $in: ownerIds } },
    {
      $set: {
        stage: ProcessingStage.SENT_TO_DIRECTSKIP,
      },
    },
    { strict: false }
  );

  try {
    await axios.post(`${DIRECTSKIP_SERVER_URL}/run-directskip`, {
      rows: rowsToSend,
    });
    console.log("Sent to directSkip");
  } catch (error) {
    console.error(`[DirectSkip] ❌ Error sending to directskip server}:`, error);
    throw error;
  }

  return rowsToSend.length;
}
