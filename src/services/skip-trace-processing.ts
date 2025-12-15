import { DirectSkipSearchResponse } from "../types/directskip";
import prisma from "../db";
import { ProcessingStage, DirectSkipStatus } from "../generated/prisma/enums";
import { validateAndStorePhone } from "./phone-validation";
import crypto from "crypto";
import { ScrappedData } from "../models/ScrappedData";
import { makeIdentityKey } from "../utils/helper";
import { toE164 } from "../utils/phone";

interface IWebhookResult {
  identityKey: string;
  response: DirectSkipSearchResponse;
}

interface PhoneToValidate {
  contactId: string;
  phoneNumber: string;
  phoneType: string;
  isMainContact: boolean;
}

export const processSkipTraceResponse = async (results: IWebhookResult[]) => {
  if (!results || !Array.isArray(results)) {
    console.warn("[SkipTraceProcessing] No valid results to process");
    return;
  }
  console.log(`[SkipTraceProcessing] Processing ${results.length} results...`);

  // Process in chunks to avoid overwhelming the DB or event loop if results are huge

  await Promise.all(
    results.map(async (result) => {
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
        await ScrappedData.updateOne(
          { identityKey },
          {
            $set: {
              stage: ProcessingStage.DIRECTSKIP_COMPLETED,
            },
          }
        );

        // 2. Save DirectSkip contacts (NO phone validation yet)
        const contactMap = await saveDirectSkipContacts(response);
        console.log("contactMap", contactMap);

        // 2.5 Save Property Details and Lists from MongoDB
        if (contactMap.mainContactId) {
          await savePropertyDetails(contactMap.mainContactId, identityKey);
        }

        // 3. Determine which phones to lookup based on deceased status
        const phonesToValidate: PhoneToValidate[] = [];

        if (response.contacts && response.contacts.length > 0) {
          // Check if the first contact (main contact) is deceased
          const firstContact = response.contacts[0];
          const isMainContactDeceased = firstContact.names?.some((name) => name.deceased === "Y") ?? false;

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
                  isMainContact: false,
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
                    isMainContact: true,
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
          await ScrappedData.updateOne(
            { identityKey },
            {
              $set: {
                stage: ProcessingStage.NUMBERLOOKUP_PROCESSING,
              },
            }
          );

          console.log(`[SkipTraceProcessing] Validating ${phonesToValidate.length} selected phones...`);

          await Promise.all(
            phonesToValidate.map(async (phone, index) => {
              const result = await validateAndStorePhone({
                contactId: phone.contactId,
                phoneNumber: phone.phoneNumber,
                phoneType: phone.phoneType,
                isMainContact: phone.isMainContact,
                count: index + 1,
              });

              if (result.success) {
                console.log(
                  `[SkipTraceProcessing] ✓ Validated and stored phone ${phone.phoneNumber} (index: ${index})`
                );
              } else {
                console.log(
                  `[SkipTraceProcessing] ✗ Skipped phone ${phone.phoneNumber} (index: ${index}): ${result.reason}`
                );
              }
            })
          );

          await prisma.pipeline.update({
            where: {
              identityKey,
            },
            data: {
              stage: ProcessingStage.NUMBERLOOKUP_COMPLETED,
              updatedAt: new Date(),
            },
          });
          await ScrappedData.updateOne(
            { identityKey },
            {
              $set: {
                stage: ProcessingStage.NUMBERLOOKUP_COMPLETED,
              },
            }
          );
        }
      } catch (err) {
        console.error(`[SkipTraceProcessing] Error processing row ${result.identityKey}:`, err);
        // Continue with other rows
      }
    })
  );
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

      if (!firstName && !lastName && !mailingAddress) {
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
            console.log(
              `[SaveContacts] Updated existing contact ${contact.id}: ${firstName} ${lastName} with ${JSON.stringify(
                updateData
              )}`
            );
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
            const relPhoneNumbers = relative.phones || [];

            if (!relFirstName && !relLastName) continue;

            // Try to find the relative contact
            let relativeContact = await prisma.contacts.findFirst({
              where: {
                first_name: { equals: relFirstName, mode: "insensitive" },
                last_name: { equals: relLastName, mode: "insensitive" },
                mailing_address: { equals: mailingAddress, mode: "insensitive" },
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
              await prisma.$transaction(async (tx) => {
                for (const phone of relPhoneNumbers) {
                  if (!phone.phonenumber) continue;
                  const formattedNumber = toE164(phone.phonenumber);

                  const existing = await tx.contact_phones.findFirst({
                    where: {
                      contact_id: relativeContact!.id ?? null,
                      phone_number: formattedNumber,
                    },
                  });

                  if (existing) {
                    await tx.contact_phones.update({
                      where: { id: existing.id },
                      data: { phone_type: phone.phonetype },
                    });
                  } else {
                    const phoneId = crypto.randomUUID();
                    await tx.contact_phones.create({
                      data: {
                        id: phoneId,
                        contact_id: relativeContact!.id ?? null,
                        phone_number: formattedNumber,
                        phone_type: phone.phonetype,
                      },
                    });
                  }
                }
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
    const mongoRecord = await ScrappedData.findOne({ identityKey }).lean();

    if (!mongoRecord) {
      console.warn(`[SavePropertyDetails] No MongoDB record found for ${identityKey}`);
      return;
    }

    // Get property_datas array, default to empty array if not present
    const propertyDatas: Record<string, any>[] = mongoRecord.property_datas || [];
    
    if (propertyDatas.length === 0) {
      console.warn(`[SavePropertyDetails] No property_datas found for ${identityKey}`);
      return;
    }

    // Get currList from mongoRecord for list associations
    const listStrArray: string[] = mongoRecord.currList || [];

    // Process each property in property_datas
    for (const propData of propertyDatas) {
      const propertyAddr = propData.property_address || "";
      const propertyCity = propData.property_city || "";
      const propertyState = propData.property_state || "";
      const propertyZip = propData.property_zip_code || "";

      const bedrooms = propData.bedrooms ? String(propData.bedrooms) : "";
      const bathrooms = propData.bathrooms ? String(propData.bathrooms) : "";
      const sqft = propData.square_feet ? String(propData.square_feet) : "";
      const year = propData.year_built ? String(propData.year_built) : "";
      const heatingType = propData.heat || "";
      const airConditioner = propData.air_conditioner || "";
      const buildingUseCode = propData.land_use_code || "";
      const parcelId = propData.parcel || propData.apn || "";
      const lastSalePrice = propData.last_sale_price ? String(propData.last_sale_price) : "";
      const lastSold = propData.last_sale_date || "";
      const taxDelinquentValue = propData.tax_delinquent_amount ? String(propData.tax_delinquent_amount) : "";
      const yearBehindOnTaxes = propData.years_delinquent ? String(propData.years_delinquent) : "";
      const foreclosureDate = propData.foreclosure || propData.foreclosure_date || "";
      const bankruptcyDate = propData.bankruptcy || propData.bankruptcy_recording_date || "";
      const lienType = propData.tax_lien || "";
      const cdu = propData.cdu || "";

      let finalAirConditioner = airConditioner;
      if (!finalAirConditioner && heatingType?.toUpperCase().includes("AIR CONDITION")) {
        finalAirConditioner = "Yes";
      }

      // Skip if no property address
      if (!propertyAddr) {
        console.warn(`[SavePropertyDetails] Skipping property with no address for ${identityKey}`);
        continue;
      }

      // Check if property already exists
      let propertyDetails = await prisma.property_details.findFirst({
        where: {
          contact_id: contactId,
          property_address: { equals: propertyAddr, mode: "insensitive" },
        },
      });

      // Create or update property
      if (!propertyDetails) {
        propertyDetails = await prisma.property_details.create({
          data: {
            id: crypto.randomUUID(),
            contact_id: contactId,
            property_address: propertyAddr,
            property_city: propertyCity,
            property_state: propertyState,
            property_zip: String(propertyZip),
            bedrooms,
            bathrooms,
            sqft,
            year,
            heating_type: heatingType,
            air_conditioner: finalAirConditioner,
            building_use_code: buildingUseCode,
            parcel_id: String(parcelId),
            apn: String(parcelId),
            last_sale_price: lastSalePrice,
            last_sold: lastSold,
            tax_delinquent_value: taxDelinquentValue,
            year_behind_on_taxes: yearBehindOnTaxes,
            foreclosure_date: foreclosureDate,
            bankruptcy_recording_date: bankruptcyDate,
            lien_type: lienType,
            cdu,
          },
        });
        console.log(`[SavePropertyDetails] Created property details ${propertyDetails.id}`);
      } else {
        const updateData: any = {};

        if (propertyCity) updateData.property_city = propertyCity;
        if (propertyState) updateData.property_state = propertyState;
        if (propertyZip) updateData.property_zip = String(propertyZip);
        if (bedrooms) updateData.bedrooms = bedrooms;
        if (bathrooms) updateData.bathrooms = bathrooms;
        if (sqft) updateData.sqft = sqft;
        if (year) updateData.year = year;
        if (heatingType) updateData.heating_type = heatingType;
        if (finalAirConditioner) updateData.air_conditioner = finalAirConditioner;
        if (buildingUseCode) updateData.building_use_code = buildingUseCode;
        if (parcelId) updateData.parcel_id = String(parcelId);
        if (parcelId) updateData.apn = String(parcelId);
        if (lastSalePrice) updateData.last_sale_price = lastSalePrice;
        if (lastSold) updateData.last_sold = lastSold;
        if (taxDelinquentValue) updateData.tax_delinquent_value = taxDelinquentValue;
        if (yearBehindOnTaxes) updateData.year_behind_on_taxes = yearBehindOnTaxes;
        if (foreclosureDate) updateData.foreclosure_date = foreclosureDate;
        if (bankruptcyDate) updateData.bankruptcy_recording_date = bankruptcyDate;
        if (lienType) updateData.lien_type = lienType;
        if (cdu) updateData.cdu = cdu;

        if (Object.keys(updateData).length > 0) {
          await prisma.property_details.update({
            where: { id: propertyDetails.id },
            data: updateData,
          });
          console.log(`[SavePropertyDetails] Updated property details ${propertyDetails.id}`);
        }
      }

      // Link property to lists
      for (const listName of listStrArray) {
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
  } catch (err) {
    console.error(`[SavePropertyDetails] Error processing ${identityKey}:`, err);
  }
};
