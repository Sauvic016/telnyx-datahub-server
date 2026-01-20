export interface PropertyAddressUpdate {
  newPropertyAddress?: string;
  newPropertyCity?: string;
  newPropertyState?: string;
  newPropertyZip?: string;
}

export interface MailingAddressUpdate {
  newMailingAddress?: string;
  newMailingCity?: string;
  newMailingState?: string;
  newMailingZip?: string;
}

export type AddressUpdate = PropertyAddressUpdate & MailingAddressUpdate;

export interface EditDetailsResult {
  success: true;
  summary: string;
  message: string;
  modifiedFields: string[];
}

export interface EditDetailsError {
  success: false;
  error: string;
  statusCode: number;
}

export type EditDetailsResponse = EditDetailsResult | EditDetailsError;
