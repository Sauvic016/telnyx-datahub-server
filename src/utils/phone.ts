/**
 * Normalize phone number to a consistent format for storage and matching
 * Removes all non-numeric characters and adds US country code if needed
 *
 * Examples:
 * - "3307605034" -> "13307605034"
 * - "(330) 760-5034" -> "13307605034"
 * - "+1 330-760-5034" -> "13307605034"
 * - "1-330-760-5034" -> "13307605034"
 */
export function normalizePhone(phone: string | null | undefined): string {
  if (!phone) return "";

  // Remove all non-numeric characters
  const digits = phone.replace(/\D/g, "");

  // If empty after cleaning, return empty
  if (!digits) return "";

  // If it's a 10-digit US number without country code, add it
  if (digits.length === 10) {
    return `1${digits}`;
  }

  // If it already has country code (11 digits starting with 1)
  if (digits.length === 11 && digits.startsWith("1")) {
    return digits;
  }

  // For other formats, return digits as-is
  return digits;
}

/**
 * Format normalized phone to E.164 format for Telnyx API
 * Example: "13307605034" -> "+13307605034"
 */
export function toE164(normalizedPhone: string): string {
  if (!normalizedPhone) return "";
  return normalizedPhone.startsWith("+") ? normalizedPhone : `+${normalizedPhone}`;
}

export function normalizeUSPhoneNumber(phone: string): string {
  const digits = phone.replace(/\D/g, "");

  // Already normalized
  if (/^\+1\d{10}$/.test(phone)) {
    return phone;
  }

  // Handle 1XXXXXXXXXX
  if (digits.length === 11 && digits.startsWith("1")) {
    return `+${digits}`;
  }

  // Validate 10-digit NANP number (area code must start 2–9)
  if (/^[2-9]\d{9}$/.test(digits)) {
    return `+1${digits}`;
  }

  return phone;
}
