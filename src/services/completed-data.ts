import prisma from "../db";
import { Prisma } from "../generated/prisma/client";
import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
import { formatRowData } from "../utils/completed-formatter";
import { makeIdentityKey, resolveDateRange } from "../utils/helper";

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

function matchesActivityFilter(activity: Activity, filter: { smsSent?: boolean; smsDelivered?: boolean }): boolean {
  // Case 1: user filters by smsDelivered
  if (filter.smsDelivered === true) {
    return activity.smsDelivered === true;
  }

  if (filter.smsDelivered === false) {
    return activity.smsDelivered === false;
  }

  // Case 2: user filters by smsSent (and did not specify delivered)
  if (filter.smsSent === true) {
    return activity.smsSent === true;
  }

  if (filter.smsSent === false) {
    return activity.smsSent === false;
  }

  // No activity filter
  return true;
}

interface DateRangeFilter {
  type?: string;
  startDate?: string;
  endDate?: string;
}
export interface Activity {
  smsSent: boolean;
  smsDelivered: boolean;
}

export interface CompletedRecordsFilters {
  listName?: string;
  propertyStatusId?: string;
  dateRange?: DateRangeFilter;
  sortBy?: "updatedAt" | "lastSold";
  sortOrder?: "asc" | "desc";
  activity?: Partial<Activity>;
}

interface PaginationParams {
  skip: number;
  take: number;
}

// Helper function for dynamic sorting
function buildOrderBy(
  sortBy?: "updatedAt" | "lastSold",
  sortOrder: "asc" | "desc" = "desc",
): Prisma.PipelineOrderByWithRelationInput {
  switch (sortBy) {
    case "lastSold":
      return {
        propertyDetails: {
          last_sold: {
            sort: sortOrder === "asc" ? Prisma.SortOrder.asc : Prisma.SortOrder.desc,
            nulls: Prisma.NullsOrder.last,
          },
        },
      };
    case "updatedAt":
    default:
      return { updatedAt: sortOrder };
  }
}

export const fetchCompletedRecords = async (
  filters?: CompletedRecordsFilters,
  pagination?: PaginationParams,
  searchQuery?: string,
) => {
  const sortOrder = filters?.sortOrder || "desc";

  const andConditions: Prisma.PipelineWhereInput[] = [{ decision: "APPROVED" }];
  if (filters?.listName && filters.listName !== "all-lists") {
    andConditions.push({
      propertyDetails: {
        lists: {
          some: {
            list: {
              name: { equals: filters.listName, mode: "insensitive" },
            },
          },
        },
      },
    });
  }

  // Date range filter (on Pipeline.updatedAt)
  if (filters?.dateRange?.startDate || filters?.dateRange?.endDate) {
    const { startDate, endDate } = resolveDateRange({
      filterDateType: filters.dateRange.type,
      startDate: filters.dateRange.startDate,
      endDate: filters.dateRange.endDate,
    });

    if (startDate) {
      andConditions.push({ updatedAt: { gte: startDate } });
    }
    if (endDate) {
      andConditions.push({ updatedAt: { lte: endDate } });
    }
  }

  // Property status filter
  if (filters?.propertyStatusId) {
    andConditions.push({
      propertyDetails: {
        property_status: {
          some: {
            propertyStatusId: filters.propertyStatusId,
          },
        },
      },
    });
  }

  if (searchQuery) {
    // Search query filter
    const searchTerm = searchQuery.trim();
    andConditions.push({
      OR: [
        {
          propertyDetails: {
            property_address: { contains: searchTerm, mode: "insensitive" },
          },
        },
        {
          contacts: {
            mailing_address: { contains: searchTerm, mode: "insensitive" },
          },
        },
        {
          contacts: {
            first_name: { contains: searchTerm, mode: "insensitive" },
          },
        },
        {
          contacts: {
            last_name: { contains: searchTerm, mode: "insensitive" },
          },
        },
        {
          propertyDetails: {
            lists: {
              some: {
                list: {
                  name: { contains: searchTerm, mode: "insensitive" },
                },
              },
            },
          },
        },
      ],
    });
  }
  // if (filters?.activity) {
  //   const { smsSent, smsDelivered } = filters.activity;

  //   if (smsDelivered) {
  //   }
  // }

  // Combine all conditions
  const where: Prisma.PipelineWhereInput = {
    AND: andConditions,
  };

  // Build dynamic orderBy
  const orderBy = buildOrderBy(filters?.sortBy, sortOrder);

  // Include configuration
  const include = {
    contacts: {
      include: {
        directskips: true,
        contact_phones: true,
        relationsFrom: {
          include: {
            toContact: {
              select: {
                first_name: true,
                last_name: true,
                contact_phones: true,
              },
            },
          },
        },
      },
    },
    propertyDetails: {
      include: {
        lists: { include: { list: true } },
        property_status: { include: { PropertyStatus: true } },
      },
    },
  };

  // Execute queries
  const [rows, total] = await prisma.$transaction([
    prisma.pipeline.findMany({
      where,
      orderBy,
      // skip: pagination?.skip,
      // take: pagination?.take,
      include,
    }),
    prisma.pipeline.count({ where }),
  ]);

  let updatedRows: any = rows;
  const phoneNumbersMap = rows.map((row) => getPropIdPhoneNumbers(row));
  const matchedPropertyId = await findContactsWithMatchingOutboundMessages(phoneNumbersMap);

  updatedRows = updatedRows.map((row: any) => {
    const propertyDetailsId = row.propertyDetailsId!;
    if (Object.keys(matchedPropertyId).includes(propertyDetailsId)) {
      if (matchedPropertyId[propertyDetailsId].status === "delivered") {
        return { ...row, ["contacts"]: { ...row.contacts, smsSent: true, smsDelivered: true } };
      } else {
        return { ...row, ["contacts"]: { ...row.contacts, smsSent: true, smsDelivered: false } };
      }
    }
    return { ...row, ["contacts"]: { ...row.contacts, smsSent: false, smsDelivered: false } };
  });

  if (filters?.activity) {
    const { smsSent, smsDelivered } = filters.activity;

    console.log("Filter values:", {
      smsSent,
      smsDelivered,
      typeOfSmsSent: typeof smsSent,
      typeofSmsDelivered: typeof smsDelivered,
    });

    // console.log(
    //   "All rows smsSent values:",
    //   updatedRows.map((row: any) => ({
    //     propertyDetailsId: row.propertyDetailsId,
    //     smsSent: row.contacts.smsSent,
    //     smsDelivered: row.contacts.smsDelivered,
    //   })),
    // );
    // console.log("Before filter count:", updatedRows.length);

    updatedRows = updatedRows.filter((row: any) => {
      if (smsDelivered !== undefined && row.contacts.smsDelivered !== smsDelivered) {
        return false;
      }
      if (smsSent !== undefined && row.contacts.smsSent !== smsSent) {
        return false;
      }
      return true;
    });

    // console.log("After filter count:", updatedRows.length);
    // console.log(
    //   "Filtered rows:",
    //   updatedRows.map((r: any) => ({
    //     id: r.propertyDetailsId,
    //     smsSent: r.contacts.smsSent,
    //     smsDelivered: r.contacts.smsDelivered,
    //   })),
    // );
  }
  updatedRows = await formatRowData(updatedRows);

  const start = pagination?.skip ?? 0;
  const end = start + (pagination?.take ?? updatedRows.length);
  const paginatedRows = updatedRows.slice(start, end);

  return { rows: paginatedRows, total: updatedRows.length };
};

async function findContactsWithMatchingOutboundMessages(
  phoneNumbersMap: Record<string, { property_address: string; phones: string[] }>[],
): Promise<Record<string, { status: string }>> {
  const matched: Record<string, { status: string }> = {};

  for (const entry of phoneNumbersMap) {
    const propertyId = Object.keys(entry)[0];
    const { property_address, phones } = entry[propertyId];

    // Skip if no phones or no property address
    if (!phones.length || !property_address) continue;

    const matchingMessages = await prisma.outbound_messages.findFirst({
      where: {
        receiver_phone: { in: phones },
        text: { contains: property_address, mode: "insensitive" },
      },
      select: { id: true, status: true },
    });

    if (matchingMessages) {
      matched[propertyId] = { status: matchingMessages.status! };
    }
  }

  return matched;
}

function getPropIdPhoneNumbers(data: any): Record<string, any> {
  const propId = data.propertyDetailsId;
  const phoneSet = new Set<string>();

  // Add main contact's phone numbers
  for (const phone of data.contacts.contact_phones) {
    if (phone.phone_number) {
      phoneSet.add(phone.phone_number);
    }
  }

  // Add relatives' phone numbers from relationsFrom
  for (const relation of data.contacts.relationsFrom) {
    // Skip self-referential relations

    for (const phone of relation.toContact.contact_phones) {
      if (phone.phone_number) {
        phoneSet.add(phone.phone_number);
      }
    }
  }

  return {
    [propId]: {
      property_address: data?.propertyDetails?.property_address?.toLowerCase(),
      phones: Array.from(phoneSet),
    },
  };
}
