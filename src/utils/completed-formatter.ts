import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
import prisma from "../db";
import { Pipeline, Prisma } from "../generated/prisma/client";
import { Activity } from "../services/completed-data";

// ============ HELPERS ============

export const sortContactPhonesByTag = (phones: any[]): any[] => {
  if (!phones || phones.length === 0) return phones;

  // Check if any phone has phone_tags with DS pattern
  const hasAnyDSTags = phones.some((phone) => phone.phone_tags && /^DS\d+$/i.test(phone.phone_tags));

  // If no DS tags found, return as-is
  if (!hasAnyDSTags) return phones;

  return [...phones].sort((a, b) => {
    const tagA = a.phone_tags || "";
    const tagB = b.phone_tags || "";

    const matchA = tagA.match(/^DS(\d+)$/i);
    const matchB = tagB.match(/^DS(\d+)$/i);

    // If both have DS tags, sort by number
    if (matchA && matchB) {
      return parseInt(matchA[1]) - parseInt(matchB[1]);
    }

    // DS tags come before non-DS tags
    if (matchA) return -1;
    if (matchB) return 1;

    // Both don't have DS tags, keep original order
    return 0;
  });
};

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
  // smsSent: boolean;
  // smsDelivered: boolean;
  // propertyAddressFound: boolean;
}

interface FormattedRelative {
  id?: string;
  first_name: string | null;
  last_name: string | null;
  relationType: string | null;
  contact_phones: FormattedContactPhone[];
  // smsSent: boolean;
  // smsDelivered: boolean;
  // propertyAddressFound: boolean;
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
  // propertyAddressFound: boolean;
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
  propertyOwners: any;
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
  contacts: FormattedContact | [];
  propertyDetails: FormattedPropertyDetails | [];
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

const formatRowData = async (rows: any[]): Promise<FormattedRow[]> => {
  return rows.map((row) => ({
    ownerId: row.ownerId,
    propertyId: row.propertyId,
    contactId: row.contactId,
    propertyDetailsId: row.propertyDetailsId,
    decision: row.decision,
    stage: row.stage,
    updatedAt: row.updatedAt,
    createdAt: row.createdAt,
    source: "prisma",
    contacts: row.contacts ? formatPrismaContact(row.contacts) : [],
    propertyDetails: row.propertyDetails ? formatPrismaPropertyDetails(row.propertyDetails) : [],
  }));
};

const formatPrismaContact = (contact: any): FormattedContact => {
  const phones = formatPhones(contact.contact_phones);

  // const smsSent = phones.some((p) => p.smsSent);
  // const smsDelivered = phones.some((p) => p.smsDelivered);

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
    relatives: formatRelatives(contact),
    directskips: contact.directskips ?? null,
    contact_phones: phones,
    smsSent: contact.smsSent,
    smsDelivered: contact.smsDelivered,
  };
};

const formatRelatives = (contact: any): FormattedRelative[] => {
  const rels: FormattedRelative[] = [];

  for (const r of contact.relationsFrom ?? []) {
    if (!r.toContact) continue;

    const phones = formatPhones(r.toContact.contact_phones);

    rels.push({
      id: r.toContactId,
      first_name: r.toContact.first_name ?? null,
      last_name: r.toContact.last_name ?? null,
      relationType: r.relationType ?? null,
      contact_phones: phones,

      // smsSent: phones.some((p) => p.smsSent),
      // smsDelivered: phones.some((p) => p.smsDelivered),
    });
  }

  return rels;
};

const formatPhones = (phones: any[] | null | undefined): FormattedContactPhone[] =>
  sortContactPhonesByTag(
    (phones ?? []).map((phone) => {
      // const activity = phone.phone_number ? phoneActivityMap.get(phone.phone_number) : undefined;

      return {
        id: phone.id,
        phone_number: phone.phone_number ?? null,
        phone_type: phone.phone_type ?? null,
        phone_status: phone.phone_status ?? null,
        phone_tags: phone.phone_tags ?? null,
        callerId: phone.telynxLookup?.caller_id ?? null,
        isLookedUp: !!phone.telynxLookup,
        telynxLookup: phone.telynxLookup ?? null,
      };
    }),
  );

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
      propertyOwners: null,
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
    propertyOwners: propertyDetails.owners ?? null,
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

export { formatRowData };
