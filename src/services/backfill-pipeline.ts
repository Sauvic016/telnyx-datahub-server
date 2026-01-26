import prisma from "../db";
import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";

export async function backfillPipelineLinks(batchSize = 100) {
  const rows = await prisma.pipeline.findMany({
    where: {
      decision: "APPROVED",
      OR: [{ contactId: null }, { propertyDetailsId: null }],
    },
    take: batchSize,
  });

  await Promise.allSettled(rows.map(backfillPipelineRow));
  console.log("Done with the linking");
}

async function backfillPipelineRow(row: { ownerId: string; propertyId: string }) {
  const owner = await Owner.findById(row.ownerId);
  const property = await PropertyData.findById(row.propertyId);

  // Need at least owner to do anything useful
  if (!owner) return;

  const ownerIdentityKey = owner.identityKey;

  let contactId: string | undefined;
  let propertyDetailsId: string | undefined;

  const contact = await resolveContact(ownerIdentityKey);
  if (contact) {
    contactId = contact.id;

    // Only try to resolve property details if we have both property AND contact
    if (property) {
      const propertyIdentityKey = property.identityKey;
      const propertyDetails = await resolvePropertyDetails(propertyIdentityKey, contact.id);

      if (propertyDetails) {
        propertyDetailsId = propertyDetails.id;
      }
    }
  }

  // Only update if we have something to update
  if (!contactId && !propertyDetailsId) return;

  await prisma.pipeline.update({
    where: {
      ownerId_propertyId: {
        ownerId: row.ownerId,
        propertyId: row.propertyId,
      },
    },
    data: {
      ...(contactId ? { contactId } : {}),
      ...(propertyDetailsId ? { propertyDetailsId } : {}),
    },
  });
}

function resolvePropertyDetails(propertyIdentityKey: string, contactId?: string) {
  const [address, city, state, zip] = propertyIdentityKey.split("|");

  return prisma.property_details.findFirst({
    where: {
      contact_id: contactId,
      property_address: { equals: address, mode: "insensitive" },
      property_city: { equals: city, mode: "insensitive" },
      property_state: { equals: state, mode: "insensitive" },
      property_zip: { equals: zip, mode: "insensitive" },
    },
  });
}

function resolveContact(ownerIdentityKey: string) {
  const [firstName, lastName, mailingAddress] = ownerIdentityKey.split("|");

  return prisma.contacts.findFirst({
    where: {
      first_name: { equals: firstName, mode: "insensitive" },
      last_name: { equals: lastName, mode: "insensitive" },
      mailing_address: { equals: mailingAddress, mode: "insensitive" },
    },
  });
}
