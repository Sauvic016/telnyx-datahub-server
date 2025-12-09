interface DirectSkipSearchOptions {
  apiKey: string;

  // Owner name (recommended)
  lastName?: string;
  firstName?: string;

  // Mailing address (recommended)
  mailingAddress?: string;
  mailingCity?: string;
  mailingState?: string;
  mailingZip?: string;

  // Property address (also recommended)
  propertyAddress?: string;
  propertyCity?: string;
  propertyState?: string;
  propertyZip?: string;

  // Optional custom fields
  customField1?: string;
  customField2?: string;
  customField3?: string;

  // Optional flags
  autoMatchBoost?: number; // default 1
  dncScrub?: number; // 0 or 1
  ownerFix?: number; // 0 or 1
}

/**
 * Types for the DirectSkip API response, based on sample output.
 */

export interface DirectSkipStatus {
  error: string;
}

export interface DirectSkipInput {
  lastname?: string;
  firstname?: string;
  address?: string;
  city?: string;
  state?: string;
  zip?: string;
  property_address?: string;
  property_city?: string;
  property_state?: string;
  property_zip?: string;
  custom_field1?: string;
  custom_field2?: string;
  custom_field3?: string;
}

export interface DirectSkipResultCode {
  result_code: string;
}

export interface DirectSkipNameRecord {
  firstname?: string;
  lastname?: string;
  age?: string;
  deceased?: string;
}

export interface DirectSkipPhoneRecord {
  phonenumber?: string;
  phonetype?: string;
}

export interface DirectSkipEmailRecord {
  email?: string;
}

export interface DirectSkipConfirmedAddress {
  street?: string;
  city?: string;
  state?: string;
  zip?: string;
}

export interface DirectSkipRelative {
  name?: string;
  age?: string;
  phones?: DirectSkipPhoneRecord[];
}

export interface DirectSkipContact {
  names?: DirectSkipNameRecord[];
  phones?: DirectSkipPhoneRecord[];
  emails?: DirectSkipEmailRecord[];
  confirmed_address?: DirectSkipConfirmedAddress[];
  relatives?: DirectSkipRelative[];
}

/**
 * Full DirectSkip response shape.
 */
export interface DirectSkipSearchResponse {
  status: DirectSkipStatus;
  input: DirectSkipInput;
  result_code: DirectSkipResultCode;
  contacts: DirectSkipContact[];
}
