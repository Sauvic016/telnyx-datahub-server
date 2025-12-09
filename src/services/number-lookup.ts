import { Telnyx } from "telnyx";
import { toE164 } from "../utils/phone";

const TELNYX_API_KEY = process.env.TELNYX_API_KEY || "";
const telnyx = new Telnyx({ apiKey: TELNYX_API_KEY });

interface ILookupNumbers {
  phone_number: string;
}

export interface TelnyxLookupResult {
  success: boolean;
  data?: {
    caller_name?: {
      caller_name?: string;
      error_code?: string;
    };
    carrier?: {
      error_code?: string | null;
      mobile_country_code?: string;
      mobile_network_code?: string | number;
      name?: string;
      type?: string;
    };
    country_code?: string;
    fraud?: any;
    national_format?: string;
    phone_number?: string;
    portability?: {
      altspid?: string;
      altspid_carrier_name?: string;
      altspid_carrier_type?: string;
      city?: string;
      line_type?: string;
      lrn?: string;
      ocn?: string;
      ported_date?: string;
      ported_status?: string;
      spid?: string;
      spid_carrier_name?: string;
      spid_carrier_type?: string;
      state?: string;
    };
    record_type?: string;
  };
  error?: string;
}

/**
 * Perform Telnyx number lookup for a single phone number
 * @param normalizedPhone - Phone number in normalized format (e.g., "13307605034")
 * @returns TelnyxLookupResult with success status and data
 */
export async function lookupSingleNumber(normalizedPhone: string): Promise<TelnyxLookupResult> {
  try {
    const e164Phone = toE164(normalizedPhone);
    console.log(`[TelnyxLookup] Looking up ${e164Phone}...`);

    const { data: numberInfo } = await telnyx.numberLookup.retrieve(e164Phone);


    console.log(`[TelnyxLookup] Success for ${e164Phone}`);
    console.log(numberInfo);
    return {
      success: true,
      data: numberInfo,
    };
  } catch (error: any) {
    console.error(`[TelnyxLookup] Failed for ${normalizedPhone}:`, error.message);
    return {
      success: false,
      error: error.message || "Unknown error",
    };
  }
}

/**
 * Batch lookup for multiple phone numbers (legacy function)
 */
const numberLookup = async (phones: ILookupNumbers[]) => {
  console.log(
    "[NumberLookup] Batch lookup for:",
    phones.map((phone) => phone.phone_number)
  );

  const results = await Promise.all(phones.map((phone) => lookupSingleNumber(phone.phone_number)));

  return results;
};
