import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
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
}

interface FormattedRelative {
  id?: string;
  first_name: string | null;
  last_name: string | null;
  relationType: string | null;
  contact_phones: FormattedContactPhone[];
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

// ============ MAIN FORMATTER ============

const formatRowData = async (rows: any[]): Promise<FormattedRow[]> => {
  const updatedRows = await Promise.all(
    rows.map(async (row: any) => {
      const hasPrismaData = row.contactId && row.propertyDetailsId;

      if (hasPrismaData) {
        return formatPrismaRow(row);
      } else {
        return await formatMongoRow(row);
      }
    }),
  );

  return updatedRows;
};

// ============ PRISMA ROW FORMATTER ============

const formatPrismaRow = (row: any): FormattedRow => {
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
    contacts: row.contacts ? formatPrismaContact(row.contacts) : null,
    propertyDetails: row.propertyDetails ? formatPrismaPropertyDetails(row.propertyDetails) : null,
  };
};

const formatPrismaContact = (contact: any): FormattedContact => {
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
    };
  }

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
    relatives: getRelativesFromPrisma(contact),
    contact_phones: formatPrismaContactPhones(contact.contact_phones),
    directskips: contact.directskips ?? null,
  };
};

const getRelativesFromPrisma = (contact: any): FormattedRelative[] => {
  if (!contact) return [];

  const relationsFrom = contact.relationsFrom ?? [];
  const relationsTo = contact.relationsTo ?? [];

  if (relationsFrom.length === 0 && relationsTo.length === 0) {
    return [];
  }

  return [
    ...relationsFrom
      .filter((rel: any) => rel.toContact)
      .map((rel: any) => ({
        id: rel.toContact.id,
        first_name: rel.toContact.first_name ?? null,
        last_name: rel.toContact.last_name ?? null,
        relationType: rel.relationType ?? null,
        contact_phones: formatPrismaContactPhones(rel.toContact.contact_phones),
      })),
    ...relationsTo
      .filter((rel: any) => rel.fromContact)
      .map((rel: any) => ({
        id: rel.fromContact.id,
        first_name: rel.fromContact.first_name ?? null,
        last_name: rel.fromContact.last_name ?? null,
        relationType: rel.relationType ?? null,
        contact_phones: formatPrismaContactPhones(rel.fromContact.contact_phones),
      })),
  ];
};

const formatPrismaContactPhones = (phones: any[] | null | undefined): FormattedContactPhone[] => {
  if (!phones || !Array.isArray(phones)) return [];

  return phones.map((phone: any) => ({
    id: phone.id,
    phone_number: phone.phone_number ?? null,
    phone_type: phone.phone_type ?? null,
    phone_status: phone.phone_status ?? null,
    phone_tags: phone.phone_tags ?? null,
    callerId: phone.telynxLookup?.caller_id ?? null,
    isLookedUp: !!phone.telynxLookup,
    telynxLookup: phone.telynxLookup ?? null,
  }));
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
