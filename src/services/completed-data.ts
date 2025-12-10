import prisma from "../db";

export const getCompletedData = async (filters?: { listName?: string }) => {
  // 1. Fetch all approved pipeline items
  const completedData = await prisma.pipeline.findMany({
    where: {
      decision: "APPROVED",
    },
    select: {
      identityKey: true,
      stage: true,
    },
  });

  // Create a map for quick stage lookup by identityKey
  const stageMap = new Map<string, string>();
  completedData.forEach((item) => {
    stageMap.set(item.identityKey, item.stage);
  });

  const identityKeys = completedData.map((item) => item.identityKey);

  const contactDetails = identityKeys.map((identityKey) => {
    const [firstName, lastName, mailingAddress] = identityKey.split("|");
    return {
      firstName,
      lastName,
      mailingAddress,
    };
  });

  // Build the where clause
  const contactData = await prisma.$transaction(async (tx) => {
    const whereClause: any = {
      OR: contactDetails.map((item) => ({
        first_name: { equals: item.firstName, mode: "insensitive" },
        last_name: { equals: item.lastName, mode: "insensitive" },
        mailing_address: { equals: item.mailingAddress, mode: "insensitive" },
      })),
    };

    if (filters?.listName) {
      whereClause.property_details = {
        some: {
          lists: {
            some: {
              list: {
                name: { equals: filters.listName, mode: "insensitive" },
              },
            },
          },
        },
      };
    }

    const contactData = await tx.contacts.findMany({
      where: whereClause,
      include: {
        directskips: true,
        contact_phones: {
          where: { telynxLookupId: { not: null } },
          include: { telynxLookup: true },
        },
        property_details: { include: { lists: { include: { list: true } } } },
        relationsFrom: {
          include: {
            toContact: {
              select: {
                first_name: true,
                last_name: true,
                contact_phones: {
                  include: { telynxLookup: true },
                },
              },
            },
          },
        },
        relationsTo: {
          include: {
            fromContact: {
              select: {
                first_name: true,
                last_name: true,
                contact_phones: {
                  include: { telynxLookup: true },
                },
              },
            },
          },
        },
      },
    });

    return contactData;
  });

  // Transform the data to include formatted relatives and new fields
  const formattedData = contactData.map((contact) => {
    // Reconstruct identityKey to look up stage
    const identityKey = `${contact.first_name?.toLowerCase()}|${contact.last_name?.toLowerCase()}|${contact.mailing_address?.toLowerCase()}`;
    const stage = stageMap.get(identityKey) || "UNKNOWN";

    // Combine relatives from both directions (relationsFrom and relationsTo)
    const relatives = [
      ...contact.relationsFrom.map((rel) => ({
        first_name: rel.toContact.first_name,
        last_name: rel.toContact.last_name,
        contact_phones: rel.toContact.contact_phones.map((p) => ({
          ...p,
          callerId: p.telynxLookup?.caller_id,
        })),
      })),
      ...contact.relationsTo.map((rel) => ({
        first_name: rel.fromContact.first_name,
        last_name: rel.fromContact.last_name,
        contact_phones: rel.fromContact.contact_phones.map((p) => ({
          ...p,
          callerId: p.telynxLookup?.caller_id,
        })),
      })),
    ];

    return {
      id: contact.id,
      first_name: contact.first_name,
      last_name: contact.last_name,
      mailing_address: contact.mailing_address,
      mailing_city: contact.mailing_city,
      mailing_state: contact.mailing_state,
      mailing_zip: contact.mailing_zip,
      deceased: contact.deceased,
      age: contact.age,
      stage: stage, // Add stage
      confirmedAddress: contact.directskips?.confirmedAddress, // Add confirmedAddress
      contact_phones: contact.contact_phones.map((phone) => ({
        ...phone,
        callerId: phone.telynxLookup?.caller_id, // Add callerId
      })),
      property_details: contact.property_details,
      relatives, // Formatted relatives with names and contact_phones
    };
  });

  return formattedData;
};
