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

interface DateRangeFilter {
  type?: string;
  startDate?: string;
  endDate?: string;
}

export interface CompletedRecordsFilters {
  listName?: string;
  propertyStatusId?: string;
  dateRange?: DateRangeFilter;
  sortBy?: "updatedAt" | "lastSold";
  sortOrder?: "asc" | "desc";
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
        contact_phones: {
          where: { telynxLookupId: { not: null } },
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
      skip: pagination?.skip,
      take: pagination?.take,
      include,
    }),
    prisma.pipeline.count({ where }),
  ]);

  const updatedRows = await formatRowData(rows);
  return { rows: updatedRows, total };
};
