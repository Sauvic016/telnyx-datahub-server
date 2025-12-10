import prisma from "../db";
import { lookupSingleNumber } from "./number-lookup";
import { normalizePhone, toE164 } from "../utils/phone";
import { us_state } from "../utils/constants";
import crypto from "crypto";

export interface PhoneValidationParams {
  contactId: string;
  phoneNumber: string;
  phoneType: string;
  isMainContact: boolean;
  count: number;
}

export interface PhoneValidationResult {
  success: boolean;
  reason?: "duplicate" | "lookup_failed" | "validation_error";
  error?: string;
  phoneId?: string;
}

/**
 * Classifies the caller ID based on caller name matching with contact's name.
 * Returns: IDMATCH, WC, NoID, or Wrong Number
 */
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

/**
 * Validates a phone number via Telnyx and stores it in both
 * contact_phones and TelynxLookup tables.
 *
 * If the phone number has already been validated (TelynxLookup exists),
 * it reuses the existing lookup data and only creates a new contact_phones entry.
 * This allows multiple contacts to share the same phone number while only
 * performing Telnyx lookup once.
 *
 * Note: TelynxLookup and contact_phones are linked via matching phone_number values,
 * not via foreign key constraint (to handle existing orphaned data).
 *
 * @param params - Phone validation parameters
 * @returns PhoneValidationResult indicating success/failure and reason
 */
export async function validateAndStorePhone(params: PhoneValidationParams): Promise<PhoneValidationResult> {
  const { contactId, phoneNumber, phoneType, isMainContact, count } = params;

  try {
    // Normalize phone number (digits only)
    const normalized = normalizePhone(phoneNumber);

    if (!normalized) {
      return {
        success: false,
        reason: "validation_error",
        error: "Invalid phone number format",
      };
    }

    // Convert to E.164 for storage (e.g. +1330...)
    const e164 = toE164(normalized);

    // Check if this contact already has this phone number
    // Check BOTH formats to catch legacy data (normalized) and new data (e164)
    const existingForContact = await prisma.contact_phones.findFirst({
      where: {
        contact_id: contactId,
        OR: [{ phone_number: e164 }, { phone_number: normalized }],
      },
    });

    if (existingForContact) {
      console.log(`[PhoneValidation] Contact ${contactId} already has phone ${e164} (or legacy ${normalized})`);
      return {
        success: false,
        reason: "duplicate",
      };
    }

    // Check if TelynxLookup already exists for this phone number
    // Check BOTH formats
    const existingLookup = await prisma.telynxLookup.findFirst({
      where: {
        OR: [{ phone_number: e164 }, { phone_number: normalized }],
      },
    });

    if (existingLookup) {
      // Reuse existing lookup data, just create contact_phones entry
      console.log(`[PhoneValidation] Reusing existing Telnyx lookup for ${e164}`);

      const phoneId = crypto.randomUUID();
      await prisma.contact_phones.create({
        data: {
          id: phoneId,
          contact_id: contactId,
          phone_number: e164, // Always store as E.164
          phone_type: phoneType,
          phone_status: "active",
          phone_tags: isMainContact ? `DS${count}` : "",
          telynxLookupId: existingLookup.id, // Link to existing lookup
        },
      });

      console.log(`[PhoneValidation] Created contact_phones for ${e164} (reused lookup)`);
      return {
        success: true,
        phoneId,
      };
    }

    // No existing lookup - perform new Telnyx lookup
    // Lookup using E.164 to be safe with Telnyx API
    const telnyxResult = await lookupSingleNumber(e164);

    if (!telnyxResult.success || !telnyxResult.data) {
      console.log(`[PhoneValidation] Telnyx lookup failed for ${e164}: ${telnyxResult.error}`);
      return {
        success: false,
        reason: "lookup_failed",
        error: telnyxResult.error,
      };
    }

    // Store both in a transaction (atomic!)
    const phoneId = crypto.randomUUID();

    // Fetch contact to get first_name and last_name for caller ID classification
    const contact = await prisma.contacts.findUnique({
      where: { id: contactId },
      select: { first_name: true, last_name: true },
    });

    const firstName = contact?.first_name || "";
    const lastName = contact?.last_name || "";

    // Calculate caller_id classification
    const callerName = telnyxResult.data!.caller_name?.caller_name;
    const callerId = classifyCallerId(callerName, firstName, lastName);
    console.log(
      `[PhoneValidation] Caller ID classification: ${callerId} (callerName: ${callerName}, firstName: ${firstName}, lastName: ${lastName})`
    );

    await prisma.$transaction(async (tx) => {
      // 1. Upsert TelynxLookup record (handles race condition)
      const lookupRecord = await tx.telynxLookup.upsert({
        where: {
          phone_number: e164,
        },
        update: {
          // Update timestamp if it already exists
          updatedAt: new Date(),
          caller_id: callerId, // Update caller_id on re-validation
        },
        create: {
          country_code: telnyxResult.data!.country_code,
          national_format: telnyxResult.data!.national_format,
          phone_number: telnyxResult.data!.phone_number,
          record_type: telnyxResult.data!.record_type,

          // caller_name
          caller_name_caller_name: telnyxResult.data!.caller_name?.caller_name,
          caller_name_error_code: telnyxResult.data!.caller_name?.error_code,

          // caller_id classification
          caller_id: callerId,

          // carrier
          carrier_error_code: telnyxResult.data!.carrier?.error_code,
          carrier_mobile_country_code: telnyxResult.data!.carrier?.mobile_country_code,
          carrier_mobile_network_code: telnyxResult.data!.carrier?.mobile_network_code
            ? Number(telnyxResult.data!.carrier.mobile_network_code)
            : null,
          carrier_name: telnyxResult.data!.carrier?.name,
          carrier_type: telnyxResult.data!.carrier?.type,

          // portability
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

      console.log(`[PhoneValidation] Upserted TelynxLookup record ${lookupRecord.id}`);

      // 2. Create contact_phones record (links via phone_number AND foreign key)
      await tx.contact_phones.create({
        data: {
          id: phoneId,
          contact_id: contactId,
          phone_number: e164, // Always store as E.164
          phone_type: phoneType,
          phone_status: "active",
          phone_tags: isMainContact ? `DS${count}` : "",
          telynxLookupId: lookupRecord.id, // Link to lookup record
        },
      });
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
