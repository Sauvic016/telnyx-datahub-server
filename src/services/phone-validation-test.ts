import prisma from "../db";
import { lookupSingleNumber, TelnyxLookupResult } from "./number-lookup-test";
import { normalizePhone, toE164 } from "../utils/phone";
import { us_state } from "../utils/constants";
import crypto from "crypto";

export interface PhoneValidationParams {
  contactId: string;
  phoneNumber: string;
  phoneType: string;
  phoneTag: string;
}

export interface PhoneValidationResult {
  success: boolean;
  reason?: "duplicate" | "lookup_failed" | "validation_error";
  error?: string;
  phoneId?: string;
}

function classifyCallerId(callerName: string | null | undefined, firstName: string, lastName: string): string {
  const callerNameArr = (callerName || "")
    .replace(/,/g, " ")
    .split(" ")
    .map((name) => name.trim().toLowerCase())
    .filter(Boolean);

  const firstNameLower = firstName.toLowerCase();
  const lastNameLower = lastName.toLowerCase();

  if (callerNameArr.includes(firstNameLower) || callerNameArr.includes(lastNameLower)) {
    return "IDMATCH";
  } else if (callerNameArr.includes("wireless") && callerNameArr.includes("caller")) {
    return "WC";
  } else if (callerNameArr.length === 0 || callerNameArr.some((n) => us_state.includes(n))) {
    return "NoID";
  } else {
    return "Wrong Number";
  }
}

export async function validateAndStorePhone(params: PhoneValidationParams): Promise<PhoneValidationResult> {
  const { contactId, phoneNumber, phoneType, phoneTag } = params;

  try {
    const normalized = normalizePhone(phoneNumber);
    if (!normalized) {
      return {
        success: false,
        reason: "validation_error",
        error: "Invalid phone number format",
      };
    }

    const e164 = toE164(normalized);

    console.log(`[PhoneValidation] Running Telnyx lookup for ${e164}`);

    // Retry logic for rate limiting
    let telnyxResult: TelnyxLookupResult;
    let retryCount = 0;
    const maxRetries = 3;
    const baseDelay = 2000; // 2 seconds

    // Use do-while to ensure telnyxResult is always assigned
    do {
      telnyxResult = await lookupSingleNumber(e164);

      // Check if it's a rate limit error
      const isRateLimitError = telnyxResult.error?.includes("Too many requests") || 
                               telnyxResult.error?.includes("exceeded the maximum");

      if (telnyxResult.success || !isRateLimitError) {
        break; // Success or non-rate-limit error, exit retry loop
      }

      retryCount++;
      if (retryCount <= maxRetries) {
        const delay = baseDelay * Math.pow(2, retryCount - 1); // Exponential backoff: 2s, 4s, 8s
        console.log(`[PhoneValidation] Rate limit hit for ${e164}, retrying in ${delay}ms (attempt ${retryCount}/${maxRetries})`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    } while (retryCount <= maxRetries);

    console.log(`[PhoneValidation] Telnyx lookup raw response:`, JSON.stringify(telnyxResult, null, 2));

    if (!telnyxResult.success || !telnyxResult.data) {
      console.log(`[PhoneValidation] Telnyx lookup failed for ${e164} after ${retryCount} retries: ${telnyxResult.error}`);
      return {
        success: false,
        reason: "lookup_failed",
        error: telnyxResult.error,
      };
    }

    console.log(`[PhoneValidation] Telnyx lookup success for ${e164}`);

    const contact = await prisma.contacts.findUnique({
      where: { id: contactId },
      select: { first_name: true, last_name: true },
    });

    const firstName = contact?.first_name || "";
    const lastName = contact?.last_name || "";
    const callerName = telnyxResult.data!.caller_name?.caller_name;

    const callerId = classifyCallerId(callerName, firstName, lastName);

    const candidatePhoneId = crypto.randomUUID();

    let phoneId!: string;

    await prisma.$transaction(async (tx) => {
      // Use last 10 digits for flexible matching (same approach as saveDirectSkipContacts)
      const last10 = normalized.slice(-10);
      const existingForContact = await tx.contact_phones.findFirst({
        where: {
          contact_id: contactId,
          phone_number: {
            endsWith: last10,
          },
        },
      });

      phoneId = existingForContact?.id ?? candidatePhoneId;

      // Upsert the Telnyx lookup record
      await tx.telynxLookup.upsert({
        where: { phone_number: e164 },
        update: {
          updatedAt: new Date(),
          caller_id: callerId,

          country_code: telnyxResult.data!.country_code,
          national_format: telnyxResult.data!.national_format,
          record_type: telnyxResult.data!.record_type,

          caller_name_caller_name: telnyxResult.data!.caller_name?.caller_name,
          caller_name_error_code: telnyxResult.data!.caller_name?.error_code,

          carrier_error_code: telnyxResult.data!.carrier?.error_code,
          carrier_mobile_country_code: telnyxResult.data!.carrier?.mobile_country_code,
          carrier_mobile_network_code: telnyxResult.data!.carrier?.mobile_network_code
            ? Number(telnyxResult.data!.carrier.mobile_network_code)
            : null,
          carrier_name: telnyxResult.data!.carrier?.name,
          carrier_type: telnyxResult.data!.carrier?.type,

          portability_altspid: telnyxResult.data!.portability?.altspid,
          portability_altspid_carrier_name: telnyxResult.data!.portability?.altspid_carrier_name,
          portability_altspid_carrier_type: telnyxResult.data!.portability?.altspid_carrier_type,
          portability_city: telnyxResult.data!.portability?.city,
          portability_line_type: telnyxResult.data!.portability?.line_type,
          portability_lrn: telnyxResult.data!.portability?.lrn,
          portability_ocn: telnyxResult.data!.portability?.ocn,
          portability_ported_date: telnyxResult.data!.portability?.ported_date
            ? new Date(telnyxResult.data!.portability.ported_date)
            : null,
          portability_ported_status: telnyxResult.data!.portability?.ported_status,
          portability_spid: telnyxResult.data!.portability?.spid,
          portability_spid_carrier_name: telnyxResult.data!.portability?.spid_carrier_name,
          portability_spid_carrier_type: telnyxResult.data!.portability?.spid_carrier_type,
          portability_state: telnyxResult.data!.portability?.state,
        },
        create: {
          phone_number: e164,

          country_code: telnyxResult.data!.country_code,
          national_format: telnyxResult.data!.national_format,
          record_type: telnyxResult.data!.record_type,

          caller_name_caller_name: telnyxResult.data!.caller_name?.caller_name,
          caller_name_error_code: telnyxResult.data!.caller_name?.error_code,

          caller_id: callerId,

          carrier_error_code: telnyxResult.data!.carrier?.error_code,
          carrier_mobile_country_code: telnyxResult.data!.carrier?.mobile_country_code,
          carrier_mobile_network_code: telnyxResult.data!.carrier?.mobile_network_code
            ? Number(telnyxResult.data!.carrier.mobile_network_code)
            : null,
          carrier_name: telnyxResult.data!.carrier?.name,
          carrier_type: telnyxResult.data!.carrier?.type,

          portability_altspid: telnyxResult.data!.portability?.altspid,
          portability_altspid_carrier_name: telnyxResult.data!.portability?.altspid_carrier_name,
          portability_altspid_carrier_type: telnyxResult.data!.portability?.altspid_carrier_type,
          portability_city: telnyxResult.data!.portability?.city,
          portability_line_type: telnyxResult.data!.portability?.line_type,
          portability_lrn: telnyxResult.data!.portability?.lrn,
          portability_ocn: telnyxResult.data!.portability?.ocn,
          portability_ported_date: telnyxResult.data!.portability?.ported_date
            ? new Date(telnyxResult.data!.portability.ported_date)
            : null,
          portability_ported_status: telnyxResult.data!.portability?.ported_status,
          portability_spid: telnyxResult.data!.portability?.spid,
          portability_spid_carrier_name: telnyxResult.data!.portability?.spid_carrier_name,
          portability_spid_carrier_type: telnyxResult.data!.portability?.spid_carrier_type,
          portability_state: telnyxResult.data!.portability?.state,
        },
      });

      // Explicitly fetch the record to get the ID (needed because upsert doesn't always return ID reliably on update)
      const lookupRecord = await tx.telynxLookup.findUnique({
        where: { phone_number: e164 },
        select: { id: true },
      });

      if (!lookupRecord) {
        throw new Error(`Failed to find TelynxLookup record for ${e164} after upsert`);
      }

      console.log(`[PhoneValidation] TelynxLookup record ID: ${lookupRecord.id} for phone ${e164}`);
      console.log(`[PhoneValidation] About to ${existingForContact ? 'UPDATE' : 'CREATE'} contact_phones with telynxLookupId: ${lookupRecord.id}`);

      if (existingForContact) {
        const updated = await tx.contact_phones.update({
          where: { id: existingForContact.id },
          data: {
            phone_number: e164,
            phone_status: "active",
            phone_tags: phoneTag,
            telynxLookupId: lookupRecord.id,
          },
        });
        console.log(`[PhoneValidation] Updated contact_phones ID: ${updated.id}, telynxLookupId set to: ${updated.telynxLookupId}`);
      } else {
        const created = await tx.contact_phones.create({
          data: {
            id: phoneId,
            contact_id: contactId,
            phone_number: e164,
            phone_type: phoneType,
            phone_status: "active",
            phone_tags: phoneTag,
            telynxLookupId: lookupRecord.id,
          },
        });
        console.log(`[PhoneValidation] Created contact_phones ID: ${created.id}, telynxLookupId set to: ${created.telynxLookupId}`);
      }
    });

    console.log(`[PhoneValidation] Successfully validated and stored ${normalized} for contact ${contactId}`);

    return {
      success: true,
      phoneId,
    };
  } catch (error: any) {
    console.error("[PhoneValidation] Unexpected error:", error);
    return {
      success: false,
      reason: "validation_error",
      error: error.message || "Unknown error",
    };
  }
}
