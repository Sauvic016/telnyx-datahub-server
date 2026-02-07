import prisma from "../db";
import { Prisma } from "../generated/prisma/client";
import { formatRowData } from "../utils/completed-formatter";
import { resolveDateRange } from "../utils/helper";

export interface Activity {
  smsSent: boolean;
  smsDelivered: boolean;
}

export interface CompletedRecordsFilters {
  listName?: string;
  propertyStatusId?: string;
  dateRange?: DateRangeFilter;
  sortBy?: "updatedAt" | "lastSold" | "case_date" | "sale_date";
  sortOrder?: "asc" | "desc";
  activity?: Partial<Activity>;
}

interface DateRangeFilter {
  type?: string;
  startDate?: string;
  endDate?: string;
}

interface PaginationParams {
  skip: number;
  take: number;
}

function buildOrderBy(
  sortBy?: "updatedAt" | "lastSold" | "case_date" | "sale_date",
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
    case "case_date":
      return {
        propertyDetails: {
          case_date: {
            sort: sortOrder === "asc" ? Prisma.SortOrder.asc : Prisma.SortOrder.desc,
            nulls: Prisma.NullsOrder.last,
          },
        },
      };
    case "sale_date":
      return {
        propertyDetails: {
          sale_date: {
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

  // List filter
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

  // Date range filter
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

  // Search query filter
  if (searchQuery) {
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

  // Pre-filter using database query to get matching pipeline IDs
  if (filters?.activity) {
    const { smsSent, smsDelivered } = filters.activity;

    if (smsDelivered !== undefined) {
      // Find pipelines that have delivered outbound messages
      const deliveredPipelineIds = await getFilteredPipelineIds(true);

      if (smsDelivered === true) {
        // Include only pipelines with delivered messages
        if (deliveredPipelineIds.length > 0) {
          andConditions.push({
            OR: deliveredPipelineIds.map((id) => ({
              ownerId: id.ownerId,
              propertyId: id.propertyId,
            })),
          });
        } else {
          andConditions.push({ ownerId: "impossible-match-xyz" });
        }
      } else {
        // smsDelivered === false: Exclude pipelines with delivered messages
        // This correctly includes both "sent but not delivered" AND "never sent"
        if (deliveredPipelineIds.length > 0) {
          andConditions.push({
            NOT: {
              OR: deliveredPipelineIds.map((id) => ({
                ownerId: id.ownerId,
                propertyId: id.propertyId,
              })),
            },
          });
        }
        // If no delivered pipelines exist, all records match — no condition needed
      }
    } else if (smsSent !== undefined) {
      // Find pipelines that have ANY outbound messages
      const sentPipelineIds = await getFilteredPipelineIds(false);

      if (smsSent === true) {
        // Include only pipelines with sent messages
        if (sentPipelineIds.length > 0) {
          andConditions.push({
            OR: sentPipelineIds.map((id) => ({
              ownerId: id.ownerId,
              propertyId: id.propertyId,
            })),
          });
        } else {
          andConditions.push({ ownerId: "impossible-match-xyz" });
        }
      } else {
        // smsSent === false: Exclude pipelines with any sent messages
        if (sentPipelineIds.length > 0) {
          andConditions.push({
            NOT: {
              OR: sentPipelineIds.map((id) => ({
                ownerId: id.ownerId,
                propertyId: id.propertyId,
              })),
            },
          });
        }
        // If no sent pipelines exist, all records match — no condition needed
      }
    }
  }

  const where: Prisma.PipelineWhereInput = {
    AND: andConditions,
  };

  const orderBy = buildOrderBy(filters?.sortBy, sortOrder);

  const include = {
    contacts: {
      include: {
        directskips: true,
        contact_phones: {
          include: { telynxLookup: true },
        },
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
      },
    },
    propertyDetails: {
      include: {
        lists: { include: { list: true } },
        property_status: { include: { PropertyStatus: true } },
        owners: {
          include: {
            contact: {
              select: {
                first_name: true,
                last_name: true,
              },
            },
          },
        },
      },
    },
  };

  // Execute queries with pagination at the DB level
  const [rows, total] = await prisma.$transaction([
    prisma.pipeline.findMany({
      where,
      orderBy,
      skip: pagination?.skip,
      take: pagination?.take,
      include,
    }),
    prisma.pipeline.count({ where }),
  ]);

  // Enrich rows with SMS status (only for the paginated subset)
  const enrichedRows = await enrichWithSmsStatus(rows);
  const formattedRows = await formatRowData(enrichedRows);

  return { rows: formattedRows, total };
};

// Get pipeline IDs that have matching outbound messages
// onlyDelivered = true  → only match messages with status "delivered"
// onlyDelivered = false → match any outbound message
async function getFilteredPipelineIds(onlyDelivered: boolean): Promise<Array<{ ownerId: string; propertyId: string }>> {
  // Get all approved pipelines with their phone numbers in a single query
  const pipelines = await prisma.pipeline.findMany({
    where: {
      decision: "APPROVED",
      propertyDetails: {
        property_address: { not: null },
      },
    },
    select: {
      ownerId: true,
      propertyId: true,
      propertyDetailsId: true,
      propertyDetails: {
        select: {
          property_address: true,
        },
      },
      contacts: {
        select: {
          contact_phones: {
            select: {
              phone_number: true,
            },
          },
          relationsFrom: {
            select: {
              toContact: {
                select: {
                  contact_phones: {
                    select: {
                      phone_number: true,
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
  });

  // Build a map of phone numbers to pipeline IDs
  const phoneMap = new Map<string, Array<{ ownerId: string; propertyId: string; propertyAddress: string }>>();

  for (const pipeline of pipelines) {
    if (!pipeline.propertyDetails?.property_address) continue;

    const phones = new Set<string>();

    // Collect main contact's phone numbers
    pipeline.contacts?.contact_phones?.forEach((phone) => {
      if (phone.phone_number) phones.add(phone.phone_number);
    });

    // Collect relatives' phone numbers (relationsFrom only, matching original)
    pipeline.contacts?.relationsFrom?.forEach((rel) => {
      rel.toContact.contact_phones?.forEach((phone) => {
        if (phone.phone_number) phones.add(phone.phone_number);
      });
    });

    // Map each phone to this pipeline
    phones.forEach((phone) => {
      if (!phoneMap.has(phone)) {
        phoneMap.set(phone, []);
      }
      phoneMap.get(phone)!.push({
        ownerId: pipeline.ownerId,
        propertyId: pipeline.propertyId,
        propertyAddress: pipeline.propertyDetails!.property_address!.toLowerCase(),
      });
    });
  }

  if (phoneMap.size === 0) {
    return [];
  }

  // Query outbound_messages for all phone numbers in one batch
  const allPhones = Array.from(phoneMap.keys());

  const statusFilter: any = onlyDelivered ? { status: "delivered" } : {};

  const outboundMessages = await prisma.outbound_messages.findMany({
    where: {
      receiver_phone: { in: allPhones },
      ...statusFilter,
    },
    select: {
      receiver_phone: true,
      text: true,
      status: true,
    },
  });

  // Match messages with pipelines based on property address in text
  const matchedPipelineIds = new Set<string>();

  for (const message of outboundMessages) {
    const pipelinesForPhone = phoneMap.get(message.receiver_phone);
    if (!pipelinesForPhone) continue;

    const messageText = message.text?.toLowerCase() || "";

    for (const pipeline of pipelinesForPhone) {
      if (messageText.includes(pipeline.propertyAddress)) {
        matchedPipelineIds.add(`${pipeline.ownerId}:${pipeline.propertyId}`);
      }
    }
  }

  // Convert back to array of { ownerId, propertyId }
  return Array.from(matchedPipelineIds).map((id) => {
    const [ownerId, propertyId] = id.split(":");
    return { ownerId, propertyId };
  });
}

// Helper function to enrich rows with SMS status
async function enrichWithSmsStatus(rows: any[]): Promise<any[]> {
  if (rows.length === 0) return rows;

  // Only enrich rows that have contacts and propertyDetails
  const rowsWithContacts = rows.filter((row) => row.contacts && row.propertyDetailsId);
  const phoneNumbersMap = rowsWithContacts.map((row) => getPropIdPhoneNumbers(row));
  const matchedPropertyId = await findContactsWithMatchingOutboundMessages(phoneNumbersMap);

  return rows.map((row) => {
    // Skip enrichment for rows without contacts (e.g. "sent to Directskip" stage)
    if (!row.contacts) return row;

    const propertyDetailsId = row.propertyDetailsId;
    const smsStatus = propertyDetailsId ? matchedPropertyId[propertyDetailsId] : undefined;

    if (smsStatus) {
      return {
        ...row,
        contacts: {
          ...row.contacts,
          smsSent: true,
          smsDelivered: smsStatus.status === "delivered",
        },
      };
    }

    return {
      ...row,
      contacts: {
        ...row.contacts,
        smsSent: false,
        smsDelivered: false,
      },
    };
  });
}

async function findContactsWithMatchingOutboundMessages(
  phoneNumbersMap: Record<string, { property_address: string; phones: string[] }>[],
): Promise<Record<string, { status: string }>> {
  const matched: Record<string, { status: string }> = {};

  // Collect all unique phone numbers across all entries
  const allPhones = new Set<string>();
  for (const entry of phoneNumbersMap) {
    const propertyId = Object.keys(entry)[0];
    const { phones } = entry[propertyId];
    for (const phone of phones) {
      allPhones.add(phone);
    }
  }

  if (allPhones.size === 0) return matched;

  // Single batched query instead of N sequential queries
  const allMessages = await prisma.outbound_messages.findMany({
    where: {
      receiver_phone: { in: Array.from(allPhones) },
    },
    select: { receiver_phone: true, text: true, status: true },
  });

  // Build a lookup: phone -> messages
  const messagesByPhone = new Map<string, { text: string | null; status: string | null }[]>();
  for (const msg of allMessages) {
    const existing = messagesByPhone.get(msg.receiver_phone) || [];
    existing.push({ text: msg.text, status: msg.status });
    messagesByPhone.set(msg.receiver_phone, existing);
  }

  // Match in-memory by property_address
  for (const entry of phoneNumbersMap) {
    const propertyId = Object.keys(entry)[0];
    const { property_address, phones } = entry[propertyId];

    if (!phones.length || !property_address) continue;

    const addressLower = property_address.toLowerCase();
    let found = false;

    for (const phone of phones) {
      if (found) break;
      const messages = messagesByPhone.get(phone);
      if (!messages) continue;

      for (const msg of messages) {
        if (msg.text && msg.text.toLowerCase().includes(addressLower)) {
          matched[propertyId] = { status: msg.status! };
          found = true;
          break;
        }
      }
    }
  }

  return matched;
}

function getPropIdPhoneNumbers(data: any): Record<string, any> {
  const propId = data.propertyDetailsId;
  const phoneSet = new Set<string>();

  // Add main contact's phone numbers
  for (const phone of data.contacts?.contact_phones || []) {
    if (phone.phone_number) {
      phoneSet.add(phone.phone_number);
    }
  }

  // Add relatives' phone numbers (relationsFrom only, matching original)
  for (const relation of data.contacts?.relationsFrom || []) {
    for (const phone of relation?.toContact?.contact_phones || []) {
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
