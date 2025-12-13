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
  // Transform the data to include formatted relatives and new fields
  const formattedData = contactData.map((contact) => formatContactData(contact, stageMap));

  return formattedData;
};

const formatContactData = (contact: any, stageMap?: Map<string, string>) => {
  // Reconstruct identityKey to look up stage if map is provided, otherwise use what's available
  let stage = "UNKNOWN";
  if (stageMap) {
    const identityKey = `${contact.first_name?.toLowerCase()}|${contact.last_name?.toLowerCase()}|${contact.mailing_address?.toLowerCase()}`;
    stage = stageMap.get(identityKey) || "UNKNOWN";
  } else if (contact.pipeline_stage) {
    stage = contact.pipeline_stage;
  }

  // Combine relatives from both directions (relationsFrom and relationsTo)
  const relatives = [
    ...contact.relationsFrom.map((rel: any) => ({
      first_name: rel.toContact.first_name,
      last_name: rel.toContact.last_name,
      contact_phones: rel.toContact.contact_phones.map((p: any) => ({
        ...p,
        callerId: p.telynxLookup?.caller_id,
        islookedup: !!p.telynxLookup,
      })),
    })),
    ...contact.relationsTo.map((rel: any) => ({
      first_name: rel.fromContact.first_name,
      last_name: rel.fromContact.last_name,
      contact_phones: rel.fromContact.contact_phones.map((p: any) => ({
        ...p,
        callerId: p.telynxLookup?.caller_id,
        islookedup: !!p.telynxLookup,
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
    stage: stage,
    confirmedAddress: contact.directskips?.confirmedAddress,
    contact_phones: contact.contact_phones.map((phone: any) => ({
      ...phone,
      callerId: phone.telynxLookup?.caller_id,
      islookedup: !!phone.telynxLookup,
    })),
    property_details: contact.property_details,
    relatives,
  };
};

export const getCompletedDataForContactId = async (contactId: string) => {
  const contact = await prisma.contacts.findUnique({
    where: { id: contactId },
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

  if (!contact) return null;

  // Get stage from pipeline
  const identityKey = `${contact.first_name?.toLowerCase()}|${contact.last_name?.toLowerCase()}|${contact.mailing_address?.toLowerCase()}`;
  const pipelineItem = await prisma.pipeline.findUnique({
    where: { identityKey },
    select: { stage: true },
  });

  const stage = pipelineItem?.stage || "UNKNOWN";

  // Pass stage directly via a temporary property or modify helper to accept it
  // Here I'll just pass a map with one entry for simplicity and reuse
  const stageMap = new Map<string, string>();
  stageMap.set(identityKey, stage);

  return formatContactData(contact, stageMap);
};
