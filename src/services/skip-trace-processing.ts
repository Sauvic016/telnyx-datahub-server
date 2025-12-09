import { DirectSkipSearchResponse } from "../types/directskip";
import prisma from "../db";
import { ProcessingStage, DirectSkipStatus } from "../generated/prisma/enums";
import { validateAndStorePhone } from "./phone-validation";
import crypto from "crypto";
import { ScrappedData } from "../models/ScrappedData";
import { makeIdentityKey } from "../utils/helper";

interface IWebhookResult {
  identityKey: string;
  response: DirectSkipSearchResponse;
}

interface PhoneToValidate {
  contactId: string;
  phoneNumber: string;
  phoneType: string;
}

export const processSkipTraceResponse = async (results: IWebhookResult[]) => {
  if (!results || !Array.isArray(results)) {
    console.warn("[SkipTraceProcessing] No valid results to process");
    return;
  }
  console.log(`[SkipTraceProcessing] Processing ${results.length} results...`);

  // Process in chunks to avoid overwhelming the DB or event loop if results are huge
  const CHUNK_SIZE = 50;
  for (let i = 0; i < results.length; i += CHUNK_SIZE) {
    const chunk = results.slice(i, i + CHUNK_SIZE);

    await Promise.all(
      chunk.map(async (result) => {
        try {
          const { identityKey, response } = result;

          // 1. Update Pipeline to DIRECTSKIP_COMPLETED
          await prisma.pipeline.update({
            where: {
              identityKey,
            },
            data: {
              stage: ProcessingStage.DIRECTSKIP_COMPLETED,
              updatedAt: new Date(),
            },
          });

          // 2. Save DirectSkip contacts (NO phone validation yet)
          const contactMap = await saveDirectSkipContacts(response);
          console.log(contactMap);

          // 2.5 Save Property Details and Lists from MongoDB
          if (contactMap.mainContactId) {
            await savePropertyDetails(contactMap.mainContactId, identityKey);
          }

          // 3. Determine which phones to lookup based on deceased status
          const phonesToValidate: PhoneToValidate[] = [];

          if (response.contacts && response.contacts.length > 0) {
            // Check if the first contact (main contact) is deceased
            const firstContact = response.contacts[0];
            const isMainContactDeceased = firstContact.names?.some(
              (name) => name.deceased === "Y"
            ) ?? false;

            if (isMainContactDeceased) {
              // Main contact is deceased: Get first relative's phone
              const firstRelative = firstContact.relatives?.[0];

              if (firstRelative && firstRelative.phones && firstRelative.phones.length > 0) {
                const relativeId = contactMap.relatives.get(firstRelative.name || "");
                if (relativeId && firstRelative.phones[0].phonenumber) {
                  phonesToValidate.push({
                    contactId: relativeId,
                    phoneNumber: firstRelative.phones[0].phonenumber,
                    phoneType: firstRelative.phones[0].phonetype || "Unknown",
                  });
                }
              }
            } else {
              // Main contact is not deceased: Get first 3 phones from first contact
              const mainContactId = contactMap.mainContactId;

              if (mainContactId && firstContact.phones) {
                const phonesToAdd = firstContact.phones.slice(0, 3);
                for (const phone of phonesToAdd) {
                  if (phone.phonenumber) {
                    phonesToValidate.push({
                      contactId: mainContactId,
                      phoneNumber: phone.phonenumber,
                      phoneType: phone.phonetype || "Unknown",
                    });
                  }
                }
              }
            }
          }

          // 4. Validate and store ONLY the selected phones
          if (phonesToValidate.length > 0) {
            await prisma.pipeline.update({
              where: {
                identityKey,
              },
              data: {
                stage: ProcessingStage.NUMBERLOOKUP_PROCESSING,
                updatedAt: new Date(),
              },
            });

            console.log(`[SkipTraceProcessing] Validating ${phonesToValidate.length} selected phones...`);

            // Validate and store each selected phone
            for (const phone of phonesToValidate) {
              const result = await validateAndStorePhone({
                contactId: phone.contactId,
                phoneNumber: phone.phoneNumber,
                phoneType: phone.phoneType,
              });

              if (result.success) {
                console.log(`[SkipTraceProcessing] ✓ Validated and stored phone ${phone.phoneNumber}`);
              } else {
                console.log(`[SkipTraceProcessing] ✗ Skipped phone ${phone.phoneNumber}: ${result.reason}`);
              }
            }

            await prisma.pipeline.update({
              where: {
                identityKey,
              },
              data: {
                stage: ProcessingStage.NUMBERLOOKUP_COMPLETED,
                updatedAt: new Date(),
              },
            });
          }
        } catch (err) {
          console.error(`[SkipTraceProcessing] Error processing row ${result.identityKey}:`, err);
          // Continue with other rows
        }
      })
    );
  }
  console.log(`[SkipTraceProcessing] Completed processing ${results.length} results.`);
};

interface ContactMap {
  mainContactId: string;
  relatives: Map<string, string>; // relative name -> contact ID
}

const saveDirectSkipContacts = async (response: DirectSkipSearchResponse): Promise<ContactMap> => {
  const contacts = response.contacts || [];
  const contactMap: ContactMap = {
    mainContactId: "",
    relatives: new Map(),
  };

  if (contacts.length === 0) return contactMap;

  const processedContactIds = new Set<string>();

  for (const contactData of contacts) {
    try {
      // Get the primary contact name (first name in the list)
      const primaryName = contactData.names?.[0];
      if (!primaryName) continue;

      const firstName = (primaryName.firstname || "").toLowerCase();
      const lastName = (primaryName.lastname || "").toLowerCase();

      // Get mailing address from input or confirmed_address
      const confirmedAddr = contactData.confirmed_address?.[0];
      const mailingAddress = (response.input.address || confirmedAddr?.street || "").toLowerCase();
      const mailingCity = (response.input.city || confirmedAddr?.city || "").toLowerCase();
      const mailingState = (response.input.state || confirmedAddr?.state || "").toLowerCase();
      const mailingZip = response.input.zip || confirmedAddr?.zip || "";

      if (!firstName || !lastName || !mailingAddress) {
        console.warn("[SaveContacts] Skipping contact - missing identity fields");
        continue;
      }

      // Check if contact exists (case-insensitive)
      let contact = await prisma.contacts.findFirst({
        where: {
          first_name: { equals: firstName, mode: "insensitive" },
          last_name: { equals: lastName, mode: "insensitive" },
          mailing_address: { equals: mailingAddress, mode: "insensitive" },
        },
      });

      // If we haven't processed this contact ID yet in this batch, update/create it.
      // If we HAVE processed it, we skip updating it to prevent lower-priority results (e.g. deceased records)
      // from overwriting the main result.
      const isDuplicateInBatch = contact && processedContactIds.has(contact.id);

      if (!isDuplicateInBatch) {
        if (contact) {
          // Update existing contact - only update fields if we have values
          const updateData: any = {};
          if (primaryName.deceased) updateData.deceased = primaryName.deceased;
          if (primaryName.age) updateData.age = primaryName.age;
          if (mailingCity) updateData.mailing_city = mailingCity;
          if (mailingState) updateData.mailing_state = mailingState;
          if (mailingZip) updateData.mailing_zip = mailingZip;

          if (Object.keys(updateData).length > 0) {
            contact = await prisma.contacts.update({
              where: { id: contact.id },
              data: updateData,
            });
            console.log(`[SaveContacts] Updated existing contact ${contact.id}: ${firstName} ${lastName} with ${JSON.stringify(updateData)}`);
          } else {
             console.log(`[SaveContacts] No updates needed for existing contact ${contact.id}`);
          }
        } else {
          // Create new contact
          contact = await prisma.contacts.create({
            data: {
              id: crypto.randomUUID(),
              first_name: firstName,
              last_name: lastName,
              mailing_address: mailingAddress,
              mailing_city: mailingCity,
              mailing_state: mailingState,
              mailing_zip: mailingZip,
              deceased: primaryName.deceased || "N",
              age: primaryName.age,
              user_id: "1",
            },
          });
          console.log(`[SaveContacts] Created new contact ${contact.id}: ${firstName} ${lastName}`);
        }

        // Upsert DirectSkip status
        await prisma.directSkip.upsert({
          where: { contactId: contact.id },
          update: {
            status: DirectSkipStatus.COMPLETED,
            skipTracedAt: new Date(),
            confirmedAddress: confirmedAddr as any,
          },
          create: {
            contactId: contact.id,
            status: DirectSkipStatus.COMPLETED,
            skipTracedAt: new Date(),
            confirmedAddress: confirmedAddr as any,
          },
        });

        processedContactIds.add(contact.id);
      } else {
        console.log(`[SaveContacts] Skipping update for duplicate contact ${contact!.id} in batch`);
      }

      // Set mainContactId if it's the first valid contact we encountered
      if (!contactMap.mainContactId && contact) {
        contactMap.mainContactId = contact.id;
      }

      // Always process relatives, even if the main contact update was skipped.
      // This ensures we capture all potential relatives found in the search results.
      if (contact) {
        const relatives = contactData.relatives || [];
        for (const relative of relatives) {
          try {
            const relName = relative.name?.split(" ") || [];
            const relFirstName = (relName[0] || "").toLowerCase();
            const relLastName = (relName.slice(1).join(" ") || "").toLowerCase();

            if (!relFirstName || !relLastName) continue;

            // Try to find the relative contact
            let relativeContact = await prisma.contacts.findFirst({
              where: {
                first_name: { equals: relFirstName, mode: "insensitive" },
                last_name: { equals: relLastName, mode: "insensitive" },
              },
            });

            // If relative doesn't exist, create using primary contact's mailing address
            if (!relativeContact) {
              relativeContact = await prisma.contacts.create({
                data: {
                  id: crypto.randomUUID(),
                  first_name: relFirstName,
                  last_name: relLastName,
                  age: relative.age,
                  mailing_address: mailingAddress,
                  mailing_city: mailingCity,
                  mailing_state: mailingState,
                  mailing_zip: mailingZip,
                  user_id: "1",
                },
              });
              console.log(`[SaveContacts] Created relative ${relativeContact.id}: ${relFirstName} ${relLastName}`);
            }

            // Store relative mapping
            contactMap.relatives.set(relative.name || "", relativeContact.id);

            // Check if relationship already exists in either direction
            const existingRelation = await prisma.contactRelation.findFirst({
              where: {
                OR: [
                  { fromContactId: contact.id, toContactId: relativeContact.id },
                  { fromContactId: relativeContact.id, toContactId: contact.id },
                ],
              },
            });

            if (existingRelation) {
              // Update existing relation
              await prisma.contactRelation.update({
                where: {
                  fromContactId_toContactId: {
                    fromContactId: existingRelation.fromContactId,
                    toContactId: existingRelation.toContactId,
                  },
                },
                data: {
                  confirmationCount: { increment: 1 },
                  confirmedBidirectional: true,
                  lastConfirmedAt: new Date(),
                },
              });
              console.log(`[SaveContacts] Updated existing relation (confirmed bidirectional)`);
            } else {
              // Create new relation
              await prisma.contactRelation.create({
                data: {
                  fromContactId: contact.id,
                  toContactId: relativeContact.id,
                  relationType: "relative",
                  confirmationCount: 1,
                },
              });
              console.log(`[SaveContacts] Created new relation`);
            }
          } catch (relErr) {
            console.error("[SaveContacts] Error processing relative:", relErr);
          }
        }
      }
    } catch (contactErr) {
      console.error("[SaveContacts] Error processing contact:", contactErr);
    }
  }

  return contactMap;
};

const pickField = (doc: any, candidates: string[]): string => {
  const normalize = (s: string) => s.trim().toLowerCase().replace(/^"|"$/g, "");

  for (const cand of candidates) {
    const candNorm = normalize(cand);

    // Check for exact match first
    if (doc[cand]) return String(doc[cand]);

    // Check for normalized match
    for (const key of Object.keys(doc)) {
      if (normalize(key) === candNorm) {
        return String(doc[key]);
      }
    }
  }
  return "";
};

const savePropertyDetails = async (contactId: string, identityKey: string) => {
  try {
    const parts = identityKey.split("|");
    if (parts.length < 3) {
      console.warn(`[SavePropertyDetails] Invalid identityKey: ${identityKey}`);
      return;
    }

    const [firstName, lastName, mailingAddress] = parts;

    // Find matching documents in MongoDB
    // We need to match loosely because identityKey is normalized
    // But MongoDB query needs exact fields or regex.
    // Since we don't know the exact field names in MongoDB (flexible schema),
    // we might need to fetch by something else or iterate?
    // Actually, contacts-check.ts fetches ALL records for a job. Here we don't have jobId easily.
    // But we can try to match on normalized fields if we assume standard field names or try a few.
    // OR, we can rely on the fact that we probably have these fields.

    // Let's try to find by standard fields first.
    // Since ScrappedData is flexible, we can't easily do a perfect query without knowing the schema.
    // However, we can try to match on the most common fields.

    // BETTER APPROACH:
    // We can't efficiently query MongoDB with "normalized" values on unknown keys.
    // But we know the identityKey was constructed FROM the MongoDB data in contacts-check.ts.
    // So the data MUST exist.
    // The issue is we don't know which record it is without scanning.
    // But wait, `contacts-check.ts` used `jobId`. We don't have `jobId` here in the webhook response directly?
    // Actually, we don't.

    // Let's try to query by standard fields.
    const candidatesFirst = ["first_name", "First Name", "Owner First Name"];
    const candidatesLast = ["last_name", "Last Name", "Owner Last Name"];
    const candidatesAddr = ["mailing_address", "Mailing Address"];

    // We will try to find records that match ANY of these combinations.
    // This is expensive if we do it for every row.
    // But we have no choice if we don't store the MongoDB ID.

    // Construct a query that looks for matching first/last/addr in known fields
    const orConditions: any[] = [];

    // This is getting complicated to construct a perfect MongoDB query for flexible schema.
    // Let's try a simpler approach:
    // We assume the data has "first_name", "last_name", "mailing_address" or similar.
    // We will fetch records that match the normalized values.
    // Since we can't do normalized match easily in Mongo without aggregation or regex,
    // we will use regex for case-insensitive match on the most likely fields.

    const regexFirst = new RegExp(`^${firstName}$`, "i");
    const regexLast = new RegExp(`^${lastName}$`, "i");
    const regexAddr = new RegExp(`^${mailingAddress}$`, "i");

    const query = {
      $or: [
        { first_name: regexFirst, last_name: regexLast, mailing_address: regexAddr },
        { "First Name": regexFirst, "Last Name": regexLast, "Mailing Address": regexAddr },
        { "Owner First Name": regexFirst, "Owner Last Name": regexLast, "Mailing Address": regexAddr },
      ],
    };

    const mongoRecords = await ScrappedData.find(query).lean();

    if (!mongoRecords.length) {
      console.warn(`[SavePropertyDetails] No MongoDB records found for ${identityKey}`);
      return;
    }

    console.log(`[SavePropertyDetails] Found ${mongoRecords.length} records for ${identityKey}`);

    for (const doc of mongoRecords) {
      // Extract property details
      const propertyAddr = pickField(doc, ["property_address", "Property Address"]);
      const propertyCity = pickField(doc, ["property_city", "Property City", "City"]);
      const propertyState = pickField(doc, ["property_state", "Property State", "State"]);
      const propertyZip = pickField(doc, ["property_zip", "Property Zip Code", "Zip"]);

      // Extract other details
      const bedrooms = pickField(doc, ["bedrooms", "Bedrooms", "Beds"]);
      const bathrooms = pickField(doc, ["bathrooms", "Bathrooms", "Baths"]);
      const sqft = pickField(doc, ["sqft", "Square Feet", "Sqft"]);
      const year = pickField(doc, ["year", "Year Built"]);
      const heatingType = pickField(doc, ["Heat"]);
      const airConditioner = pickField(doc, ["air_conditioner", "Air Conditioner"]); // Or infer from Heat if it contains "AIR CONDITION"
      const buildingUseCode = pickField(doc, ["building_use_code", "Land Use Code"]);
      const parcelId = pickField(doc, ["parcel_id", "Parcel", "APN"]);
      const lastSalePrice = pickField(doc, ["last_sale_price", "Last Sale Price"]);
      const lastSold = pickField(doc, ["last_sold", "Last Sale Date"]);
      const taxDelinquentValue = pickField(doc, ["tax_delinquent_value", "Tax Delinquent Amount"]);
      const yearBehindOnTaxes = pickField(doc, ["year_behind_on_taxes", "Years Delinquent"]);
      const foreclosureDate = pickField(doc, ["foreclosure_date", "Foreclosure"]);
      const bankruptcyDate = pickField(doc, ["bankruptcy_recording_date", "Bankruptcy"]);
      const lienType = pickField(doc, ["lien_type", "Tax Lien"]);

      // Infer air conditioner from Heat if explicit field missing
      let finalAirConditioner = airConditioner;
      if (!finalAirConditioner && heatingType.toUpperCase().includes("AIR CONDITION")) {
        finalAirConditioner = "Yes";
      }

      // Upsert Property Details
      // We use a composite key or just generate a new ID?
      // The schema has `id` as primary key.
      // We should check if this property already exists for this contact.

      let propertyDetails = await prisma.property_details.findFirst({
        where: {
          contact_id: contactId,
          property_address: { equals: propertyAddr, mode: "insensitive" },
        },
      });

      if (!propertyDetails) {
        propertyDetails = await prisma.property_details.create({
          data: {
            id: crypto.randomUUID(),
            contact_id: contactId,
            property_address: propertyAddr,
            property_city: propertyCity,
            property_state: propertyState,
            property_zip: propertyZip,
            bedrooms,
            bathrooms,
            sqft,
            year,
            heating_type: heatingType,
            air_conditioner: finalAirConditioner,
            building_use_code: buildingUseCode,
            parcel_id: parcelId,
            apn: parcelId, // Map Parcel to APN as well if APN is missing
            last_sale_price: lastSalePrice,
            last_sold: lastSold,
            tax_delinquent_value: taxDelinquentValue,
            year_behind_on_taxes: yearBehindOnTaxes,
            foreclosure_date: foreclosureDate,
            bankruptcy_recording_date: bankruptcyDate,
            lien_type: lienType,
            // Add other fields as needed
          },
        });
        console.log(`[SavePropertyDetails] Created property details ${propertyDetails.id}`);
      } else {
        // Update existing property details - only update fields if we have values
        const updateData: any = {};
        if (propertyCity) updateData.property_city = propertyCity;
        if (propertyState) updateData.property_state = propertyState;
        if (propertyZip) updateData.property_zip = propertyZip;
        if (bedrooms) updateData.bedrooms = bedrooms;
        if (bathrooms) updateData.bathrooms = bathrooms;
        if (sqft) updateData.sqft = sqft;
        if (year) updateData.year = year;
        if (heatingType) updateData.heating_type = heatingType;
        if (finalAirConditioner) updateData.air_conditioner = finalAirConditioner;
        if (buildingUseCode) updateData.building_use_code = buildingUseCode;
        if (parcelId) updateData.parcel_id = parcelId;
        if (parcelId) updateData.apn = parcelId; // Assuming APN logic is same
        if (lastSalePrice) updateData.last_sale_price = lastSalePrice;
        if (lastSold) updateData.last_sold = lastSold;
        if (taxDelinquentValue) updateData.tax_delinquent_value = taxDelinquentValue;
        if (yearBehindOnTaxes) updateData.year_behind_on_taxes = yearBehindOnTaxes;
        if (foreclosureDate) updateData.foreclosure_date = foreclosureDate;
        if (bankruptcyDate) updateData.bankruptcy_recording_date = bankruptcyDate;
        if (lienType) updateData.lien_type = lienType;

        if (Object.keys(updateData).length > 0) {
          propertyDetails = await prisma.property_details.update({
            where: { id: propertyDetails.id },
            data: updateData,
          });
          console.log(`[SavePropertyDetails] Updated property details ${propertyDetails.id} with partial data`);
        } else {
          console.log(`[SavePropertyDetails] No updates needed for property details ${propertyDetails.id}`);
        }
      }

      // Handle Lists
      const listStr = pickField(doc, ["list", "lists"]);
      if (listStr) {
        const listNames = listStr
          .split(",")
          .map((s) => s.trim().replace(/^"|"$/g, ""))
          .filter((s) => s.length > 0);

        for (const listName of listNames) {
          // Upsert List
          const list = await prisma.list.upsert({
            where: { name: listName },
            update: {},
            create: { name: listName },
          });

          // Upsert PropertyList association
          await prisma.propertyList.upsert({
            where: {
              propertyId_listId: {
                propertyId: propertyDetails.id,
                listId: list.id,
              },
            },
            update: {},
            create: {
              propertyId: propertyDetails.id,
              listId: list.id,
            },
          });
          console.log(`[SavePropertyDetails] Linked property ${propertyDetails.id} to list ${listName}`);
        }
      }
    }
  } catch (err) {
    console.error(`[SavePropertyDetails] Error processing ${identityKey}:`, err);
  }
};
