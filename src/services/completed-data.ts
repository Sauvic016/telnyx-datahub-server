import prisma from "../db";
import { Prisma } from "../generated/prisma/client";
import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
import { makeIdentityKey } from "../utils/helper";

export const getCompletedData = async (filters?: { listName?: string; skip: number; take: number }) => {
  // 1. Fetch all approved pipeline items

  const [completedData, total] = await prisma.$transaction([
    prisma.pipeline.findMany({
      where: { decision: "APPROVED" },
      orderBy: { updatedAt: "desc" },
      skip: filters?.skip,
      take: filters?.take,
      select: {
        propertyId: true,
        ownerId: true,
        stage: true,
      },
    }),
    prisma.pipeline.count({
      where: { decision: "APPROVED" },
    }),
  ]);
  // const completedData = await prisma.pipeline.findMany({
  //   where: {
  //     decision: "APPROVED",
  //   },
  //   skip: filters?.skip,
  //   take: filters?.take,
  //   select: {
  //     propertyId: true,
  //     ownerId: true,
  //     stage: true,
  //   },
  // });
  console.log(completedData.length);

  // const stageMap = new Map<string, string>();

  // completedData.forEach(({ ownerId, propertyId, stage }) => {
  //   stageMap.set(`${ownerId}|${propertyId}`, stage);
  // });

  const ownerPropertyMap = new Map<string, Set<string>>();

  for (const { ownerId, propertyId } of completedData) {
    if (!ownerPropertyMap.has(ownerId)) {
      ownerPropertyMap.set(ownerId, new Set());
    }
    ownerPropertyMap.get(ownerId)!.add(propertyId);
  }

  const ownerIds = [...new Set(completedData.map((k) => k.ownerId))];
  const propertyIds = [...new Set(completedData.map((k) => k.propertyId))];
  const ownersDetails = await Owner.find({
    _id: { $in: ownerIds },
  });

  const propertiesDetails = await PropertyData.find({
    _id: { $in: propertyIds },
  });

  const ownerIdToIdentityKeyMap = new Map(ownersDetails.map((o) => [o._id.toString(), o.identityKey]));
  const propertyIdToIdentityKeyMap = new Map(propertiesDetails.map((p) => [p._id.toString(), p.identityKey]));

  const ownerIdentityToPropertyIdentityMap = new Map<string, Set<string>>();

  for (const { ownerId, propertyId } of completedData) {
    const ownerIdentityKey = ownerIdToIdentityKeyMap.get(ownerId);
    const propertyIdentityKey = propertyIdToIdentityKeyMap.get(propertyId);

    if (!ownerIdentityKey || !propertyIdentityKey) continue;

    let propertySet = ownerIdentityToPropertyIdentityMap.get(ownerIdentityKey);

    if (!propertySet) {
      propertySet = new Set<string>();
      ownerIdentityToPropertyIdentityMap.set(ownerIdentityKey, propertySet);
    }

    propertySet.add(propertyIdentityKey);
  }

  const ownerIdentityKeys = ownersDetails.map((item) => item.identityKey);

  const contactDetails = ownerIdentityKeys.map((identityKey) => {
    const [firstName, lastName, mailingAddress] = identityKey.split("|");
    return {
      firstName,
      lastName,
      mailingAddress,
    };
  });

  // const owners = await Owner.find({
  //   _id: { $in: [...ownerPropertyMap.keys()] },
  // }).populate("propertyIds");

  // const result = owners.flatMap((owner) => {
  //   const allowedPropertyIds = ownerPropertyMap.get(owner._id.toString());

  //   if (!allowedPropertyIds) return [];

  //   return owner.propertyIds
  //     .filter((p: any) => allowedPropertyIds.has(p._id.toString()))
  //     .map((p: any) => {
  //       // const stage = stageMap.get(`${owner._id.toString()}|${p._id.toString()}`) ?? "UNKNOWN";

  //       return {
  //         ...owner.toObject(),
  //         _id: owner._id,
  //         property: p,
  //         // stage, // ✅ ATTACHED HERE
  //       };
  //     });
  // });

  // const validResult = result.filter(
  //   (item) =>
  //     item.owner_first_name &&
  //     item.owner_last_name &&
  //     item.mailing_address &&
  //     item.property?.property_address &&
  //     item.property?.property_city &&
  //     item.property?.property_state &&
  //     item.property?.property_zip_code
  // );

  // Build the where clause
  const contactData = await prisma.$transaction(async (tx) => {
    const whereClause: any = {
      OR: contactDetails.map((item) => ({
        first_name: { equals: item.firstName, mode: "insensitive" },
        last_name: { equals: item.lastName, mode: "insensitive" },
        mailing_address: { equals: item.mailingAddress, mode: "insensitive" },
      })),
    };

    return await tx.contacts.findMany({
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
  });
  const contactDataList: any[] = [];
  for (const contact of contactData) {
    const IdentityKey = makeIdentityKey(
      contact.first_name || "",
      contact.last_name || "",
      contact.mailing_address || ""
    );
    for (const property of contact.property_details || []) {
      const propertyIdentityKey = makeIdentityKey(
        property.property_address || "",
        property.property_city || "",
        property.property_state || "",
        property.property_zip || ""
      );
      if (ownerIdentityToPropertyIdentityMap.get(IdentityKey)?.has(propertyIdentityKey)) {
        if (filters?.listName) {
          const properytListNameSet = new Set(property.lists.map((list) => list.list.name.toLowerCase()));

          if (properytListNameSet.has(filters?.listName.toLowerCase())) {
            contactDataList.push({
              ...contact,
              property_details: property,
            });
          }
        } else {
          contactDataList.push({
            ...contact,
            property_details: property,
          });
        }
      }
    }
  }

  return { contactDataList: contactDataList.map((contact) => formatContactData(contact)), totalItems: total };
};

const formatContactData = (contact: any, matchedProperty?: any) => {
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

  // const filteredPropertyDetails = matchedProperty
  //   ? contact.property_details.filter((pd: any) => isSameProperty(matchedProperty, pd))
  //   : contact.property_details;

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

    confirmedAddress: contact.directskips?.confirmedAddress,

    contact_phones:
      contact.contact_phones?.map((phone: any) => ({
        ...phone,
        callerId: phone.telynxLookup?.caller_id,
        islookedup: !!phone.telynxLookup,
      })) || [],

    // ✅ ONLY the matched property
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

  // need to fix this one aswell
  return formatContactData(contact);
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
