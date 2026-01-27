import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
import prisma from "../db";
// ============ TYPES ============

interface FormattedContactPhone {
  id?: string;
  phone_number: string | null;
  phone_type: string | null;
  phone_status: string | null;
  phone_tags: string | null;
  callerId: string | null;
  isLookedUp: boolean;
  telynxLookup: any | null;
  smsSent: boolean;
  smsDelivered: boolean;
  propertyAddressFound: boolean;
}

interface FormattedRelative {
  id?: string;
  first_name: string | null;
  last_name: string | null;
  relationType: string | null;
  contact_phones: FormattedContactPhone[];
  smsSent: boolean;
  smsDelivered: boolean;
  propertyAddressFound: boolean;
}

interface FormattedContact {
  id?: string;
  first_name: string | null;
  last_name: string | null;
  company_name?: string | null;
  mailing_address: string | null;
  mailing_city: string | null;
  mailing_state: string | null;
  mailing_zip: string | null;
  deceased?: string | null;
  age?: string | null;
  owner_2_first_name?: string | null;
  owner_2_last_name?: string | null;
  bankruptcy?: string | null;
  foreclosure?: string | null;
  relatives: FormattedRelative[];
  contact_phones: FormattedContactPhone[];
  directskips?: any | null;
  smsSent: boolean;
  smsDelivered: boolean;
  propertyAddressFound: boolean;
}

interface FormattedList {
  listId?: number;
  name: string;
}

interface FormattedPropertyStatus {
  id: string;
  name: string;
  color: string | null;
}

interface FormattedPropertyDetails {
  id?: string;
  property_address: string | null;
  property_city: string | null;
  property_state: string | null;
  property_zip: string | null;
  parcel?: string | null;
  land_use_code?: string | null;
  bedrooms: string | number | null;
  bathrooms: string | number | null;
  sqft: string | null;
  year_built: string | number | null;
  cdu?: string | null;
  heat?: string | null;
  estimated_value?: string | null;
  last_sale_price: string | null;
  last_sold: string | null;
  previous_sale_price?: string | null;
  previous_sale_date?: string | null;
  last_buyer?: string | null;
  last_seller?: string | null;
  tax_lien?: string | null;
  tax_delinquent_amount?: string | null;
  years_delinquent?: number | null;
  lists: FormattedList[];
  currList?: string[];
  prevList?: string[];
  // Segregated property status
  primaryPropertyStatus: FormattedPropertyStatus | null;
  secondaryPropertyStatus: FormattedPropertyStatus | null;
}

interface FormattedRow {
  ownerId: string;
  propertyId: string;
  contactId: string | null;
  propertyDetailsId: string | null;
  decision: string;
  stage: string;
  updatedAt: Date;
  createdAt: Date;
  source: "prisma" | "mongodb";
  contacts: FormattedContact | null;
  propertyDetails: FormattedPropertyDetails | null;
}

// ============ SMS STATUS BATCH QUERY ============

interface ContactSmsStatus {
  contactId: string;
  smsSent: boolean;
  smsDelivered: boolean;
  propertyAddressFound: boolean;
}

interface PhoneSmsStatus {
  phoneId: string;
  smsSent: boolean;
  smsDelivered: boolean;
  propertyAddressFound: boolean;
}

interface PhonePropertyMap {
  phoneId: string;
  phoneNumber: string;
  contactId: string;
  propertyAddress: string;
}

const fetchSmsStatusBatch = async (
  contactIds: string[],
  propertyAddressMap: Map<string, string>,
  phonePropertyMapList: PhonePropertyMap[],
): Promise<{
  contactStatusMap: Map<string, ContactSmsStatus>;
  phoneStatusMap: Map<string, PhoneSmsStatus>;
}> => {
  if (contactIds.length === 0) {
    return {
      contactStatusMap: new Map(),
      phoneStatusMap: new Map(),
    };
  }

  const contactStatusMap = new Map<string, ContactSmsStatus>();
  const phoneStatusMap = new Map<string, PhoneSmsStatus>();

  // Build property address CTE for contacts
  const propertyAddressEntries = Array.from(propertyAddressMap.entries());
  const contactPropertyMapCTE =
    propertyAddressEntries.length > 0
      ? propertyAddressEntries
          .map(
            ([contactId, address]) =>
              `SELECT '${contactId.replace(/'/g, "''")}' AS contact_id, '${address.toLowerCase().replace(/'/g, "''")}' AS property_address`,
          )
          .join(" UNION ALL ")
      : "SELECT NULL::varchar AS contact_id, NULL::varchar AS property_address WHERE FALSE";

  // Build property address CTE for phones
  const phonePropertyMapCTE =
    phonePropertyMapList.length > 0
      ? phonePropertyMapList
          .map(
            (p) =>
              `SELECT '${p.phoneId.replace(/'/g, "''")}' AS phone_id, '${p.phoneNumber.replace(/'/g, "''")}' AS phone_number, '${p.propertyAddress.toLowerCase().replace(/'/g, "''")}' AS property_address`,
          )
          .join(" UNION ALL ")
      : "SELECT NULL::varchar AS phone_id, NULL::varchar AS phone_number, NULL::varchar AS property_address WHERE FALSE";

  // Query for contact-level status
  const contactResults = await prisma.$queryRawUnsafe<
    {
      contact_id: string;
      sms_sent: boolean;
      sms_delivered: boolean;
      property_address_found: boolean;
    }[]
  >(
    `
    WITH contact_ids AS (
      SELECT unnest(ARRAY[${contactIds.map((id) => `'${id.replace(/'/g, "''")}'`).join(", ")}]) AS contact_id
    ),
    contact_property_map AS (
      ${contactPropertyMapCTE}
    )
    SELECT
      ci.contact_id,
      EXISTS (
        SELECT 1
        FROM contact_phones cp
        JOIN outbound_messages om ON LOWER(om.receiver_phone) = LOWER(cp.phone_number)
        WHERE cp.contact_id = ci.contact_id
        LIMIT 1
      ) AS sms_sent,
      EXISTS (
        SELECT 1
        FROM contact_phones cp
        JOIN outbound_messages om ON LOWER(om.receiver_phone) = LOWER(cp.phone_number)
        WHERE cp.contact_id = ci.contact_id
          AND om.status = 'delivered'
        LIMIT 1
      ) AS sms_delivered,
      EXISTS (
        SELECT 1
        FROM contact_phones cp
        JOIN outbound_messages om ON LOWER(om.receiver_phone) = LOWER(cp.phone_number)
        JOIN contact_property_map cpm ON cpm.contact_id = ci.contact_id
        WHERE cp.contact_id = ci.contact_id
          AND LOWER(om.text) LIKE '%' || cpm.property_address || '%'
        ORDER BY om.created_at DESC
        LIMIT 5
      ) AS property_address_found
    FROM contact_ids ci
    `,
  );

  contactResults.forEach((result) => {
    contactStatusMap.set(result.contact_id, {
      contactId: result.contact_id,
      smsSent: result.sms_sent,
      smsDelivered: result.sms_delivered,
      propertyAddressFound: result.property_address_found,
    });
  });

  // Query for phone-level status
  if (phonePropertyMapList.length > 0) {
    const phoneResults = await prisma.$queryRawUnsafe<
      {
        phone_id: string;
        sms_sent: boolean;
        sms_delivered: boolean;
        property_address_found: boolean;
      }[]
    >(
      `
      WITH phone_property_map AS (
        ${phonePropertyMapCTE}
      )
      SELECT
        ppm.phone_id,
        EXISTS (
          SELECT 1
          FROM outbound_messages om
          WHERE LOWER(om.receiver_phone) = LOWER(ppm.phone_number)
          LIMIT 1
        ) AS sms_sent,
        EXISTS (
          SELECT 1
          FROM outbound_messages om
          WHERE LOWER(om.receiver_phone) = LOWER(ppm.phone_number)
            AND om.status = 'delivered'
          LIMIT 1
        ) AS sms_delivered,
        EXISTS (
          SELECT 1
          FROM outbound_messages om
          WHERE LOWER(om.receiver_phone) = LOWER(ppm.phone_number)
            AND LOWER(om.text) LIKE '%' || ppm.property_address || '%'
          ORDER BY om.created_at DESC
          LIMIT 5
        ) AS property_address_found
      FROM phone_property_map ppm
      `,
    );

    phoneResults.forEach((result) => {
      phoneStatusMap.set(result.phone_id, {
        phoneId: result.phone_id,
        smsSent: result.sms_sent,
        smsDelivered: result.sms_delivered,
        propertyAddressFound: result.property_address_found,
      });
    });
  }

  return { contactStatusMap, phoneStatusMap };
};

// ============ MAIN FORMATTER ============

const formatRowData = async (rows: any[]): Promise<FormattedRow[]> => {
  // Collect all contact IDs, phone IDs, and build property address maps
  const contactIdsWithPhones = new Set<string>();
  const contactIdsWithoutPhones = new Set<string>();
  const propertyAddressMap = new Map<string, string>();
  const phonePropertyMapList: PhonePropertyMap[] = [];

  rows.forEach((row) => {
    const propertyAddress = row.propertyDetails?.property_address;

    if (row.contactId) {
      // Map contact to their property address
      if (propertyAddress) {
        propertyAddressMap.set(row.contactId, propertyAddress);
      }

      // Collect phone information for the main contact
      if (row.contacts?.contact_phones && row.contacts.contact_phones.length > 0) {
        contactIdsWithPhones.add(row.contactId);
        if (propertyAddress) {
          row.contacts.contact_phones.forEach((phone: any) => {
            if (phone.id && phone.phone_number) {
              phonePropertyMapList.push({
                phoneId: phone.id,
                phoneNumber: phone.phone_number,
                contactId: row.contactId,
                propertyAddress: propertyAddress,
              });
            }
          });
        }
      } else {
        contactIdsWithoutPhones.add(row.contactId);
      }
    }

    // Also collect relative IDs and their phones
    if (row.contacts?.relationsFrom) {
      row.contacts.relationsFrom.forEach((rel: any) => {
        if (rel.toContact?.id) {
          // Relatives share the same property address from the pipeline
          if (propertyAddress) {
            propertyAddressMap.set(rel.toContact.id, propertyAddress);
          }

          // Collect phone information for relatives
          if (rel.toContact.contact_phones && rel.toContact.contact_phones.length > 0) {
            contactIdsWithPhones.add(rel.toContact.id);
            if (propertyAddress) {
              rel.toContact.contact_phones.forEach((phone: any) => {
                if (phone.id && phone.phone_number) {
                  phonePropertyMapList.push({
                    phoneId: phone.id,
                    phoneNumber: phone.phone_number,
                    contactId: rel.toContact.id,
                    propertyAddress: propertyAddress,
                  });
                }
              });
            }
          } else {
            contactIdsWithoutPhones.add(rel.toContact.id);
          }
        }
      });
    }

    if (row.contacts?.relationsTo) {
      row.contacts.relationsTo.forEach((rel: any) => {
        if (rel.fromContact?.id) {
          // Relatives share the same property address from the pipeline
          if (propertyAddress) {
            propertyAddressMap.set(rel.fromContact.id, propertyAddress);
          }

          // Collect phone information for relatives
          if (rel.fromContact.contact_phones && rel.fromContact.contact_phones.length > 0) {
            contactIdsWithPhones.add(rel.fromContact.id);
            if (propertyAddress) {
              rel.fromContact.contact_phones.forEach((phone: any) => {
                if (phone.id && phone.phone_number) {
                  phonePropertyMapList.push({
                    phoneId: phone.id,
                    phoneNumber: phone.phone_number,
                    contactId: rel.fromContact.id,
                    propertyAddress: propertyAddress,
                  });
                }
              });
            }
          } else {
            contactIdsWithoutPhones.add(rel.fromContact.id);
          }
        }
      });
    }
  });

  // Fetch SMS status for contacts with phones only
  const contactIdsToQuery = Array.from(contactIdsWithPhones);
  const { contactStatusMap, phoneStatusMap } = await fetchSmsStatusBatch(
    contactIdsToQuery,
    propertyAddressMap,
    phonePropertyMapList,
  );

  // Add false status for contacts without phones (early return optimization)
  contactIdsWithoutPhones.forEach((contactId) => {
    contactStatusMap.set(contactId, {
      contactId,
      smsSent: false,
      smsDelivered: false,
      propertyAddressFound: false,
    });
  });

  // Format rows with SMS status
  const updatedRows = await Promise.all(
    rows.map(async (row: any) => {
      const hasPrismaData = row.contactId && row.propertyDetailsId;

      if (hasPrismaData) {
        return formatPrismaRow(row, contactStatusMap, phoneStatusMap);
      } else {
        return await formatMongoRow(row);
      }
    }),
  );

  return updatedRows;
};

// ============ PRISMA ROW FORMATTER ============

const formatPrismaRow = (
  row: any,
  contactStatusMap: Map<string, ContactSmsStatus>,
  phoneStatusMap: Map<string, PhoneSmsStatus>,
): FormattedRow => {
  return {
    ownerId: row.ownerId,
    propertyId: row.propertyId,
    contactId: row.contactId,
    propertyDetailsId: row.propertyDetailsId,
    decision: row.decision,
    stage: row.stage,
    updatedAt: row.updatedAt,
    createdAt: row.createdAt,
    source: "prisma",
    contacts: row.contacts ? formatPrismaContact(row.contacts, contactStatusMap, phoneStatusMap) : null,
    propertyDetails: row.propertyDetails ? formatPrismaPropertyDetails(row.propertyDetails) : null,
  };
};

const formatPrismaContact = (
  contact: any,
  contactStatusMap: Map<string, ContactSmsStatus>,
  phoneStatusMap: Map<string, PhoneSmsStatus>,
): FormattedContact => {
  if (!contact || Object.keys(contact).length === 0) {
    return {
      first_name: null,
      last_name: null,
      mailing_address: null,
      mailing_city: null,
      mailing_state: null,
      mailing_zip: null,
      relatives: [],
      contact_phones: [],
      smsSent: false,
      smsDelivered: false,
      propertyAddressFound: false,
    };
  }

  const smsStatus = contactStatusMap.get(contact.id) || {
    contactId: contact.id,
    smsSent: false,
    smsDelivered: false,
    propertyAddressFound: false,
  };

  return {
    id: contact.id,
    first_name: contact.first_name ?? null,
    last_name: contact.last_name ?? null,
    mailing_address: contact.mailing_address ?? null,
    mailing_city: contact.mailing_city ?? null,
    mailing_state: contact.mailing_state ?? null,
    mailing_zip: contact.mailing_zip ?? null,
    deceased: contact.deceased ?? null,
    age: contact.age ?? null,
    relatives: getRelativesFromPrisma(contact, contactStatusMap, phoneStatusMap),
    contact_phones: formatPrismaContactPhones(contact.contact_phones, phoneStatusMap),
    directskips: contact.directskips ?? null,
    smsSent: smsStatus.smsSent,
    smsDelivered: smsStatus.smsDelivered,
    propertyAddressFound: smsStatus.propertyAddressFound,
  };
};

const getRelativesFromPrisma = (
  contact: any,
  contactStatusMap: Map<string, ContactSmsStatus>,
  phoneStatusMap: Map<string, PhoneSmsStatus>,
): FormattedRelative[] => {
  if (!contact) return [];

  const relationsFrom = contact.relationsFrom ?? [];
  const relationsTo = contact.relationsTo ?? [];

  if (relationsFrom.length === 0 && relationsTo.length === 0) {
    return [];
  }

  return [
    ...relationsFrom
      .filter((rel: any) => rel.toContact)
      .map((rel: any) => {
        const smsStatus = contactStatusMap.get(rel.toContact.id) || {
          contactId: rel.toContact.id,
          smsSent: false,
          smsDelivered: false,
          propertyAddressFound: false,
        };

        return {
          id: rel.toContact.id,
          first_name: rel.toContact.first_name ?? null,
          last_name: rel.toContact.last_name ?? null,
          relationType: rel.relationType ?? null,
          contact_phones: formatPrismaContactPhones(rel.toContact.contact_phones, phoneStatusMap),
          smsSent: smsStatus.smsSent,
          smsDelivered: smsStatus.smsDelivered,
          propertyAddressFound: smsStatus.propertyAddressFound,
        };
      }),
    ...relationsTo
      .filter((rel: any) => rel.fromContact)
      .map((rel: any) => {
        const smsStatus = contactStatusMap.get(rel.fromContact.id) || {
          contactId: rel.fromContact.id,
          smsSent: false,
          smsDelivered: false,
          propertyAddressFound: false,
        };

        return {
          id: rel.fromContact.id,
          first_name: rel.fromContact.first_name ?? null,
          last_name: rel.fromContact.last_name ?? null,
          relationType: rel.relationType ?? null,
          contact_phones: formatPrismaContactPhones(rel.fromContact.contact_phones, phoneStatusMap),
          smsSent: smsStatus.smsSent,
          smsDelivered: smsStatus.smsDelivered,
          propertyAddressFound: smsStatus.propertyAddressFound,
        };
      }),
  ];
};

const formatPrismaContactPhones = (
  phones: any[] | null | undefined,
  phoneStatusMap: Map<string, PhoneSmsStatus>,
): FormattedContactPhone[] => {
  if (!phones || !Array.isArray(phones)) return [];

  return phones.map((phone: any) => {
    const phoneStatus = phoneStatusMap.get(phone.id) || {
      phoneId: phone.id,
      smsSent: false,
      smsDelivered: false,
      propertyAddressFound: false,
    };

    return {
      id: phone.id,
      phone_number: phone.phone_number ?? null,
      phone_type: phone.phone_type ?? null,
      phone_status: phone.phone_status ?? null,
      phone_tags: phone.phone_tags ?? null,
      callerId: phone.telynxLookup?.caller_id ?? null,
      isLookedUp: !!phone.telynxLookup,
      telynxLookup: phone.telynxLookup ?? null,
      smsSent: phoneStatus.smsSent,
      smsDelivered: phoneStatus.smsDelivered,
      propertyAddressFound: phoneStatus.propertyAddressFound,
    };
  });
};

const formatPrismaPropertyDetails = (propertyDetails: any): FormattedPropertyDetails => {
  if (!propertyDetails || Object.keys(propertyDetails).length === 0) {
    return {
      property_address: null,
      property_city: null,
      property_state: null,
      property_zip: null,
      bedrooms: null,
      bathrooms: null,
      sqft: null,
      year_built: null,
      last_sale_price: null,
      last_sold: null,
      lists: [],
      primaryPropertyStatus: null,
      secondaryPropertyStatus: null,
    };
  }

  // Segregate property status by type
  const { primaryPropertyStatus, secondaryPropertyStatus } = segregatePropertyStatus(propertyDetails.property_status);

  return {
    id: propertyDetails.id,
    property_address: propertyDetails.property_address ?? null,
    property_city: propertyDetails.property_city ?? null,
    property_state: propertyDetails.property_state ?? null,
    property_zip: propertyDetails.property_zip ?? null,
    parcel: propertyDetails.apn ?? propertyDetails.parcel_id ?? null,
    bedrooms: propertyDetails.bedrooms ?? null,
    bathrooms: propertyDetails.bathrooms ?? null,
    sqft: propertyDetails.sqft ?? null,
    year_built: propertyDetails.year ?? null,
    cdu: propertyDetails.cdu ?? null,
    estimated_value: propertyDetails.estimated_value ?? null,
    last_sale_price: propertyDetails.last_sale_price ?? null,
    last_sold: propertyDetails.last_sold ?? null,
    lists: formatPrismaLists(propertyDetails.lists),
    primaryPropertyStatus,
    secondaryPropertyStatus,
  };
};

const segregatePropertyStatus = (
  statusAssociations: any[] | null | undefined,
): {
  primaryPropertyStatus: FormattedPropertyStatus | null;
  secondaryPropertyStatus: FormattedPropertyStatus | null;
} => {
  if (!statusAssociations || !Array.isArray(statusAssociations)) {
    return {
      primaryPropertyStatus: null,
      secondaryPropertyStatus: null,
    };
  }

  let primaryPropertyStatus: FormattedPropertyStatus | null = null;
  let secondaryPropertyStatus: FormattedPropertyStatus | null = null;

  for (const item of statusAssociations) {
    if (!item.PropertyStatus) continue;

    const formattedStatus: FormattedPropertyStatus = {
      id: item.PropertyStatus.id,
      name: item.PropertyStatus.name,
      color: item.PropertyStatus.color ?? null,
    };

    if (item.PropertyStatus.statusType === "PRIMARY") {
      primaryPropertyStatus = formattedStatus;
    } else if (item.PropertyStatus.statusType === "SECONDARY") {
      secondaryPropertyStatus = formattedStatus;
    }
  }

  return {
    primaryPropertyStatus,
    secondaryPropertyStatus,
  };
};

const formatPrismaLists = (lists: any[] | null | undefined): FormattedList[] => {
  if (!lists || !Array.isArray(lists)) return [];

  return lists
    .filter((item: any) => item.list)
    .map((item: any) => ({
      listId: item.list.id,
      name: item.list.name,
    }));
};

// ============ MONGODB ROW FORMATTER ============

const formatMongoRow = async (row: any): Promise<FormattedRow> => {
  const [ownerDetails, propertyDetails] = await Promise.all([
    Owner.findById(row.ownerId).lean(),
    PropertyData.findById(row.propertyId).lean(),
  ]);

  return {
    ownerId: row.ownerId,
    propertyId: row.propertyId,
    contactId: null,
    propertyDetailsId: null,
    decision: row.decision,
    stage: row.stage,
    updatedAt: row.updatedAt,
    createdAt: row.createdAt,
    source: "mongodb",
    contacts: ownerDetails ? formatMongoContact(ownerDetails) : null,
    propertyDetails: propertyDetails ? formatMongoPropertyDetails(propertyDetails) : null,
  };
};

const formatMongoContact = (owner: any): FormattedContact => {
  if (!owner) {
    return {
      first_name: null,
      last_name: null,
      mailing_address: null,
      mailing_city: null,
      mailing_state: null,
      mailing_zip: null,
      relatives: [],
      contact_phones: [],
      smsSent: false,
      smsDelivered: false,
      propertyAddressFound: false,
    };
  }

  return {
    id: owner._id?.toString(),
    first_name: owner.owner_first_name ?? null,
    last_name: owner.owner_last_name ?? null,
    company_name: owner.company_name_full_name ?? null,
    mailing_address: owner.mailing_address ?? null,
    mailing_city: owner.mailing_city ?? null,
    mailing_state: owner.mailing_state ?? null,
    mailing_zip: owner.mailing_zip_code ?? null,
    owner_2_first_name: owner.owner_2_first_name ?? null,
    owner_2_last_name: owner.owner_2_last_name ?? null,
    bankruptcy: owner.bankruptcy ?? null,
    foreclosure: owner.foreclosure ?? null,
    relatives: [],
    contact_phones: [],
    directskips: null,
    smsSent: false,
    smsDelivered: false,
    propertyAddressFound: false,
  };
};

const formatMongoPropertyDetails = (property: any): FormattedPropertyDetails => {
  if (!property) {
    return {
      property_address: null,
      property_city: null,
      property_state: null,
      property_zip: null,
      bedrooms: null,
      bathrooms: null,
      sqft: null,
      year_built: null,
      last_sale_price: null,
      last_sold: null,
      lists: [],
      primaryPropertyStatus: null,
      secondaryPropertyStatus: null,
    };
  }

  const formattedLists: FormattedList[] = (property.currList ?? [])
    .filter((name: any) => typeof name === "string")
    .map((name: string) => ({ name }));

  return {
    id: property._id?.toString(),
    property_address: property.property_address ?? null,
    property_city: property.property_city ?? null,
    property_state: property.property_state ?? null,
    property_zip: property.property_zip_code?.toString() ?? null,
    parcel: property.parcel ?? null,
    land_use_code: property.land_use_code ?? null,
    bedrooms: property.bedrooms ?? null,
    bathrooms: property.bathrooms ?? null,
    sqft: property.square_feet ?? null,
    year_built: property.year_built ?? null,
    cdu: property.cdu ?? null,
    heat: property.heat ?? null,
    last_sale_price: property.last_sale_price ?? null,
    last_sold: property.last_sale_date ?? null,
    previous_sale_price: property.previous_sale_price ?? null,
    previous_sale_date: property.previous_sale_date ?? null,
    last_buyer: property.last_buyer ?? null,
    last_seller: property.last_seller ?? null,
    tax_lien: property.tax_lien ?? null,
    tax_delinquent_amount: property.tax_delinquent_amount ?? null,
    years_delinquent: property.years_delinquent ?? null,
    lists: formattedLists,
    currList: property.currList ?? [],
    prevList: property.prevList ?? [],
    // MongoDB doesn't have property status
    primaryPropertyStatus: null,
    secondaryPropertyStatus: null,
  };
};

export { formatRowData };
