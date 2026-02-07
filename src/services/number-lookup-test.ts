import axios from "axios";
import { toE164 } from "../utils/phone";

const TELNYX_API_KEY = process.env.TELNYX_API_KEY || "";

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

    console.log(`[TelnyxLookup] Looking up ${e164Phone} via Telnyx REST API using axios...`);

    const url = `https://api.telnyx.com/v2/number_lookup/${encodeURIComponent(
      e164Phone,
    )}?type=carrier&type=caller-name`;
    const testurl = `http://localhost:3000/numberLookup`;

    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${TELNYX_API_KEY}`,
        Accept: "application/json",
        "Content-Type": "application/json",
      },
    });

    // Log rate limit headers for monitoring
    const rateLimitLimit = response.headers["x-ratelimit-limit"];
    const rateLimitRemaining = response.headers["x-ratelimit-remaining"];
    const rateLimitReset = response.headers["x-ratelimit-reset"];

    if (rateLimitRemaining !== undefined) {
      console.log(
        `[TelnyxLookup] Rate limit: ${rateLimitRemaining}/${rateLimitLimit} remaining (resets at ${rateLimitReset})`,
      );

      // Warn if we're getting close to the limit
      if (parseInt(rateLimitRemaining) < 10) {
        console.warn(`[TelnyxLookup] WARNING: Only ${rateLimitRemaining} requests remaining before rate limit!`);
      }
    }

    console.log(`[TelnyxLookup] Success for ${e164Phone}`);
    console.log(response.data);

    return {
      success: true,
      data: response.data?.data,
    };
  } catch (error: any) {
    console.error(`[TelnyxLookup] Failed for ${normalizedPhone}:`, error.response?.data || error.message);

    return {
      success: false,
      error: error.response?.data?.errors?.[0]?.detail || error.message,
    };
  }
}

/**
 * Batch lookup for multiple phone numbers (legacy function)
 */
const numberLookup = async (phones: ILookupNumbers[]) => {
  console.log(
    "[NumberLookup] Batch lookup for:",
    phones.map((phone) => phone.phone_number),
  );

  const results = await Promise.all(phones.map((phone) => lookupSingleNumber(phone.phone_number)));

  return results;
};
