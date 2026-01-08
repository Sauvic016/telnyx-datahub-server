import prisma from "../db";
import { Prisma } from "../generated/prisma/client";
import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";

export const getCompletedData = async (filters?: { listName?: string }) => {
  // 1. Fetch all approved pipeline items
  const completedData = await prisma.pipeline.findMany({
    where: {
      decision: "APPROVED",
    },
    select: {
      propertyId: true,
      ownerId: true,
      stage: true,
    },
  });

  const stageMap = new Map<string, string>();

  completedData.forEach(({ ownerId, propertyId, stage }) => {
    stageMap.set(`${ownerId}|${propertyId}`, stage);
  });

  const ownerPropertyMap = new Map<string, Set<string>>();

  for (const { ownerId, propertyId } of completedData) {
    if (!ownerPropertyMap.has(ownerId)) {
      ownerPropertyMap.set(ownerId, new Set());
    }
    ownerPropertyMap.get(ownerId)!.add(propertyId);
  }
  const owners = await Owner.find({
    _id: { $in: [...ownerPropertyMap.keys()] },
  }).populate("propertyIds");

  const result = owners.flatMap((owner) => {
    const allowedPropertyIds = ownerPropertyMap.get(owner._id.toString());

    if (!allowedPropertyIds) return [];

    return owner.propertyIds
      .filter((p: any) => allowedPropertyIds.has(p._id.toString()))
      .map((p: any) => {
        const stage = stageMap.get(`${owner._id.toString()}|${p._id.toString()}`) ?? "UNKNOWN";

        return {
          ...owner.toObject(),
          _id: owner._id,
          property: p,
          stage, // ✅ ATTACHED HERE
        };
      });
  });

  const validResult = result.filter(
    (item) =>
      item.owner_first_name &&
      item.owner_last_name &&
      item.mailing_address &&
      item.property?.property_address &&
      item.property?.property_city &&
      item.property?.property_state &&
      item.property?.property_zip_code
  );

  // Build the where clause
  const contactData = await prisma.$transaction(async (tx) => {
    const whereClause: Prisma.contactsWhereInput = {
      OR: validResult.map(
        (item): Prisma.contactsWhereInput => ({
          AND: [
            {
              first_name: {
                equals: item.owner_first_name!,
                mode: Prisma.QueryMode.insensitive,
              },
            },
            {
              last_name: {
                equals: item.owner_last_name!,
                mode: Prisma.QueryMode.insensitive,
              },
            },
            {
              mailing_address: {
                equals: item.mailing_address!,
                mode: Prisma.QueryMode.insensitive,
              },
            },
            {
              property_details: {
                some: {
                  property_address: {
                    equals: item.property.property_address,
                    mode: Prisma.QueryMode.insensitive,
                  },
                  property_city: {
                    equals: item.property.property_city,
                    mode: Prisma.QueryMode.insensitive,
                  },
                  property_state: {
                    equals: item.property.property_state,
                    mode: Prisma.QueryMode.insensitive,
                  },
                  property_zip: {
                    equals: String(item.property.property_zip_code),
                    mode: Prisma.QueryMode.insensitive,
                  },
                  ...(filters?.listName && {
                    lists: {
                      some: {
                        list: {
                          name: {
                            equals: filters.listName,
                            mode: Prisma.QueryMode.insensitive,
                          },
                        },
                      },
                    },
                  }),
                },
              },
            },
          ],
        })
      ),
    };

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

  const expanded = contactData.flatMap((contact) => {
    let matches = validResult.filter((r) => contact.property_details.some((pd) => isSameProperty(r.property, pd)));

    if (filters?.listName) {
      matches = matches.filter((m) => {
        const pd = contact.property_details.find((d) => isSameProperty(m.property, d));
        return pd?.lists.some((l) => l.list.name === filters.listName);
      });
    }

    return matches.map((m) => formatContactData(contact, m.stage, m.property));
  });
  return expanded;
  // const formatedData = contactData.map((contact) => formatContactData(contact, "NUMBERLOOKUP_COMPLETED"));
  // return
};

// const formatContactData = (contact: any, stage = "UNKNOWN") => {
//   const relatives = [
//     ...contact.relationsFrom.map((rel: any) => ({
//       first_name: rel.toContact.first_name,
//       last_name: rel.toContact.last_name,
//       contact_phones: rel.toContact.contact_phones.map((p: any) => ({
//         ...p,
//         callerId: p.telynxLookup?.caller_id,
//         islookedup: !!p.telynxLookup,
//       })),
//     })),
//     ...contact.relationsTo.map((rel: any) => ({
//       first_name: rel.fromContact.first_name,
//       last_name: rel.fromContact.last_name,
//       contact_phones: rel.fromContact.contact_phones.map((p: any) => ({
//         ...p,
//         callerId: p.telynxLookup?.caller_id,
//         islookedup: !!p.telynxLookup,
//       })),
//     })),
//   ];

//   return {
//     id: contact.id,
//     first_name: contact.first_name,
//     last_name: contact.last_name,
//     mailing_address: contact.mailing_address,
//     mailing_city: contact.mailing_city,
//     mailing_state: contact.mailing_state,
//     mailing_zip: contact.mailing_zip,
//     deceased: contact.deceased,
//     age: contact.age,
//     stage, // ✅ clean
//     confirmedAddress: contact.directskips?.confirmedAddress,
//     contact_phones: contact.contact_phones.map((phone: any) => ({
//       ...phone,
//       callerId: phone.telynxLookup?.caller_id,
//       islookedup: !!phone.telynxLookup,
//     })),
//     property_details: contact.property_details,
//     relatives,
//   };
// };

const formatContactData = (contact: any, stage = "UNKNOWN", matchedProperty?: any) => {
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

  const filteredPropertyDetails = matchedProperty
    ? contact.property_details.filter((pd: any) => isSameProperty(matchedProperty, pd))
    : contact.property_details;

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
    stage,

    confirmedAddress: contact.directskips?.confirmedAddress,

    contact_phones: contact.contact_phones.map((phone: any) => ({
      ...phone,
      callerId: phone.telynxLookup?.caller_id,
      islookedup: !!phone.telynxLookup,
    })),

    // ✅ ONLY the matched property
    property_details: filteredPropertyDetails[0],
    all_properties: contact.property_details,

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

  // need to fix this one aswell
  return formatContactData(contact, "NUMBERLOOKUP_COMPLETED");
};

const normalize = (v?: string | number | null) =>
  String(v ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");

const isSameProperty = (a: any, b: any) =>
  normalize(a.property_address) === normalize(b.property_address) &&
  normalize(a.property_city) === normalize(b.property_city) &&
  normalize(a.property_state) === normalize(b.property_state) &&
  normalize(a.property_zip_code) === normalize(b.property_zip);
