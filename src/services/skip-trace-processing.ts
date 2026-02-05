import { DirectSkipSearchResponse, DirectSkipContact, DirectSkipNameRecord } from "../types/directskip";
import prisma from "../db";
import { ProcessingStage, DirectSkipStatus } from "../generated/prisma/enums";
import { validateAndStorePhone } from "./phone-validation-test";
import crypto from "crypto";
import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
import { makeIdentityKey } from "../utils/helper";
import { normalizeUSPhoneNumber, toE164 } from "../utils/phone";
import { parse, isValid } from "date-fns";

interface IWebhookResult {
  identityKey: string;
  response: DirectSkipSearchResponse;
  propertyId: string;
  ownerId: string;
  poBoxAddress: string;
}

interface PhoneToValidate {
  contactId: string;
  phoneNumber: string;
  phoneType: string;
  phoneTag: string;
}

export const processSkipTraceResponse = async (results: IWebhookResult[]) => {
  if (!results || !Array.isArray(results)) {
    console.warn("[SkipTraceProcessing] No valid results to process");
    return;
  }
  console.log(`[SkipTraceProcessing] Processing ${results.length} results...`);

  await Promise.all(
    results.map(async (result) => {
      try {
        const { identityKey, response, propertyId, ownerId, poBoxAddress } = result;

        // 1. Update Pipeline to DIRECTSKIP_COMPLETED
        await prisma.pipeline.update({
          where: {
            ownerId_propertyId: {
              ownerId,
              propertyId,
            },
          },
          data: {
            stage: response.contacts.length ? ProcessingStage.DIRECTSKIP_COMPLETED : ProcessingStage.DIRECTSKIP_FAILED,
            updatedAt: new Date(),
          },
        });

        await Owner.updateOne(
          { _id: ownerId },
          {
            $set: {
              stage: response.contacts.length
                ? ProcessingStage.DIRECTSKIP_COMPLETED
                : ProcessingStage.DIRECTSKIP_FAILED,
            },
          },
          { strict: false },
        );

        // 2. Save DirectSkip contacts (NO phone validation yet)
        const contactMap = await saveDirectSkipContacts(response, ownerId, poBoxAddress);

        // 2.5 Resolve ownership (check for owner2 from MongoDB)
        const ownershipResult = await resolveOwnershipContacts(ownerId, contactMap.mainContactId, response);

        // 2.6 Save Property Details and Lists from MongoDB
        let propertyDetailsId;
        if (contactMap.mainContactId) {
          propertyDetailsId = await savePropertyDetails(
            contactMap.mainContactId,
            identityKey,
            propertyId,
            ownerId,
            response,
          );
        }

        // 2.7 Create PropertyOwnership records if we have a property
        if (propertyDetailsId && ownershipResult.owners.length > 0) {
          await createPropertyOwnerships(propertyDetailsId, ownershipResult.owners);
        }

        // 3. Determine which phones to lookup based on deceased status
        const phonesToValidate: PhoneToValidate[] = [];

        if (response.contacts && response.contacts.length > 0) {
          const resolved = resolveMainContactOptimized(response.contacts, response.input);
          const mainContact = resolved?.contact;
          const isMainContactDeceased = mainContact?.names?.some((name) => name.deceased === "Y") ?? false;

          let dsCount = 0; // Counter for DS tags

          if (isMainContactDeceased) {
            console.log(`[SkipTraceProcessing] Main contact is deceased`);

            if (ownershipResult.owner2HasPhones) {
              console.log(`[SkipTraceProcessing] Adding owner2 phones for validation (owner deceased)`);
              // Re-map owner2 phones to assign DS tags starting from 1 (separate from owner1)
              let owner2DsCount = 0;
              for (const phone of ownershipResult.owner2PhonesToValidate) {
                owner2DsCount++;
                phonesToValidate.push({
                  ...phone,
                  phoneTag: `DS${owner2DsCount}`,
                });
              }
            }

            console.log(`[SkipTraceProcessing] Checking first relative's phone (owner deceased)`);
            const firstRelative = mainContact?.relatives?.[0];

            if (firstRelative && firstRelative.phones && firstRelative.phones.length > 0) {
              const relativeId = contactMap.relatives.get(firstRelative.name || "");
              if (relativeId && firstRelative.phones[0].phonenumber) {
                phonesToValidate.push({
                  contactId: relativeId,
                  phoneNumber: firstRelative.phones[0].phonenumber,
                  phoneType: firstRelative.phones[0].phonetype || "Unknown",
                  phoneTag: "R1",
                });
              }
            }
          } else {
            // Main contact is alive: validate first 3 owner phones
            const mainContactId = contactMap.mainContactId;

            if (mainContactId && mainContact?.phones) {
              const phonesToAdd = mainContact.phones.slice(0, 3);
              for (const phone of phonesToAdd) {
                if (phone.phonenumber) {
                  dsCount++;
                  phonesToValidate.push({
                    contactId: mainContactId,
                    phoneNumber: phone.phonenumber,
                    phoneType: phone.phonetype || "Unknown",
                    phoneTag: `DS${dsCount}`,
                  });
                }
              }
            }

            // Also add owner2's first 2 phones for validation (if present)
            if (ownershipResult.owner2PhonesToValidate.length > 0) {
              console.log(`[SkipTraceProcessing] Adding owner2 phones to validation queue (owner alive)`);
              // Re-map owner2 phones to assign DS tags starting from 1 (separate from owner1)
              let owner2DsCount = 0;
              for (const phone of ownershipResult.owner2PhonesToValidate) {
                owner2DsCount++;
                phonesToValidate.push({
                  ...phone,
                  phoneTag: `DS${owner2DsCount}`,
                });
              }
            }
          }
        }

        // 4. Validate and store ONLY the selected phones
        if (phonesToValidate.length > 0) {
          await prisma.pipeline.update({
            where: {
              ownerId_propertyId: {
                ownerId,
                propertyId,
              },
            },
            data: {
              stage: ProcessingStage.NUMBERLOOKUP_PROCESSING,
              updatedAt: new Date(),
            },
          });
          await Owner.updateOne(
            { _id: ownerId },
            {
              $set: {
                stage: ProcessingStage.NUMBERLOOKUP_PROCESSING,
              },
            },
            { strict: false },
          );

          console.log(`[SkipTraceProcessing] Validating ${phonesToValidate.length} selected phones...`);

          // Process phones sequentially with delay to avoid rate limiting
          for (let index = 0; index < phonesToValidate.length; index++) {
            const phone = phonesToValidate[index];

            console.log(
              `[SkipTraceProcessing] Validating phone ${index + 1}/${phonesToValidate.length}: ${phone.phoneNumber} (tag: ${phone.phoneTag}, contactId: ${phone.contactId})`,
            );

            const result = await validateAndStorePhone({
              contactId: phone.contactId,
              phoneNumber: phone.phoneNumber,
              phoneType: phone.phoneType,
              phoneTag: phone.phoneTag,
            });

            if (result.success) {
              console.log(
                `[SkipTraceProcessing] ✓ Validated and stored phone ${phone.phoneNumber} (tag: ${phone.phoneTag})`,
              );
            } else {
              console.log(
                `[SkipTraceProcessing] ✗ Skipped phone ${phone.phoneNumber} (tag: ${phone.phoneTag}): ${result.reason}`,
              );
            }

            // Add delay between requests to avoid rate limiting (except after the last one)
            if (index < phonesToValidate.length - 1) {
              const delay = 200; // 200ms delay between requests
              console.log(`[SkipTraceProcessing] Waiting ${delay}ms before next validation...`);
              await new Promise((resolve) => setTimeout(resolve, delay));
            }
          }

          await prisma.pipeline.update({
            where: {
              ownerId_propertyId: {
                ownerId,
                propertyId,
              },
            },
            data: {
              stage: ProcessingStage.NUMBERLOOKUP_COMPLETED,
              contactId: contactMap.mainContactId,
              propertyDetailsId,
              updatedAt: new Date(),
            },
          });
          await Owner.updateOne(
            { _id: ownerId },
            {
              $set: {
                stage: ProcessingStage.NUMBERLOOKUP_COMPLETED,
              },
            },
            { strict: false },
          );
        } else {
          // No phones to validate - still update pipeline with contactId and propertyDetailsId
          console.log(`[SkipTraceProcessing] No phones to validate, updating pipeline with contact and property IDs`);
          await prisma.pipeline.update({
            where: {
              ownerId_propertyId: {
                ownerId,
                propertyId,
              },
            },
            data: {
              contactId: contactMap.mainContactId,
              propertyDetailsId,
              updatedAt: new Date(),
            },
          });
        }
      } catch (err) {
        console.error(`[SkipTraceProcessing] Error processing row ${result.identityKey}:`, err);
        // Continue with other rows
      }
    }),
  );
};

interface ContactMap {
  mainContactId: string;
  relatives: Map<string, string>; // relative name -> contact ID
}

const saveDirectSkipContacts = async (
  response: DirectSkipSearchResponse,
  ownerId: string,
  poBoxAddress: string,
): Promise<ContactMap> => {
  const contactMap: ContactMap = {
    mainContactId: "",
    relatives: new Map(),
  };

  const OwnerDetails = await Owner.findOne({ _id: ownerId });

  const processedContactIds = new Set<string>();

  // ======================================================
  // CASE 1: DirectSkip returned contacts (your existing logic)
  // ======================================================
  if (response.contacts && response.contacts.length > 0) {
    const contacts = response.contacts;
    // Resolve main contact deterministically
    // const resolvedMainContact = resolveMainContact(contacts, response.input);
    const resolved = resolveMainContactOptimized(response.contacts, response.input);

    const resolvedMainContact = resolved?.contact;
    const resolvedPrimaryName = resolved?.name;

    for (const contactData of contacts) {
      try {
        const primaryName =
          contactData === resolvedMainContact && resolvedPrimaryName ? resolvedPrimaryName : contactData.names?.[0];

        if (!primaryName) continue;

        const firstName = (primaryName.firstname || "").toLowerCase();
        const lastName = (primaryName.lastname || "").toLowerCase();

        const confirmedAddr = contactData.confirmed_address?.[0];

        const mailingAddress = (OwnerDetails?.mailing_address || confirmedAddr?.street || "").toLowerCase();

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
                  updateData,
                )}`,
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

          await prisma.directSkip.upsert({
            where: { contactId: contact!.id },
            update: {
              status: DirectSkipStatus.COMPLETED,
              skipTracedAt: new Date(),
              confirmedAddress: confirmedAddr as any,
            },
            create: {
              contactId: contact!.id,
              status: DirectSkipStatus.COMPLETED,
              skipTracedAt: new Date(),
              confirmedAddress: confirmedAddr as any,
            },
          });

          processedContactIds.add(contact!.id);

          // Save main contact's phones (without validation) so they exist in the database
          // Limit to first 3 phones
          const contactPhones = (contactData.phones || []).slice(0, 3);
          if (contactPhones.length > 0) {
            const contactId = contact!.id;
            await prisma.$transaction(async (tx) => {
              for (const phone of contactPhones) {
                if (!phone.phonenumber) continue;
                const formattedNumber = normalizeUSPhoneNumber(phone.phonenumber);
                const last10 = formattedNumber.slice(-10);
                const existing = await tx.contact_phones.findFirst({
                  where: {
                    contact_id: contactId,
                    phone_number: {
                      endsWith: last10,
                    },
                  },
                });

                if (existing) {
                  await tx.contact_phones.update({
                    where: { id: existing.id },
                    data: { phone_type: phone.phonetype, phone_number: formattedNumber },
                  });
                } else {
                  const phoneId = crypto.randomUUID();
                  await tx.contact_phones.create({
                    data: {
                      id: phoneId,
                      contact_id: contactId,
                      phone_number: formattedNumber,
                      phone_type: phone.phonetype,
                    },
                  });
                }
              }
            });
            console.log(`[SaveContacts] Saved ${contactPhones.length} phones for contact ${contactId}`);
          }
        } else {
          console.log(`[SaveContacts] Skipping update for duplicate contact ${contact!.id} in batch`);
        }

        // Set mainContactId if it's the first valid contact we encountered
        // if (!contactMap.mainContactId && contact) {
        //   contactMap.mainContactId = contact.id;
        // }
        if (contact && resolvedMainContact === contactData && !contactMap.mainContactId) {
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
              // Limit to first 3 phones
              const relPhoneNumbers = (relative.phones || []).slice(0, 3);

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
                console.log(`[SaveContacts] Created relative ${relativeContact.id}: ${relFirstName} ${relLastName}`);
              }

              // Save phones for relative (both new and existing relatives)
              if (relPhoneNumbers.length > 0) {
                await prisma.$transaction(async (tx) => {
                  for (const phone of relPhoneNumbers) {
                    if (!phone.phonenumber) continue;
                    const formattedNumber = normalizeUSPhoneNumber(phone.phonenumber);
                    const last10 = formattedNumber.slice(-10);
                    const existing = await tx.contact_phones.findFirst({
                      where: {
                        contact_id: relativeContact!.id,
                        phone_number: {
                          endsWith: last10,
                        },
                      },
                    });

                    if (existing) {
                      await tx.contact_phones.update({
                        where: { id: existing.id },
                        data: { phone_type: phone.phonetype, phone_number: formattedNumber },
                      });
                    } else {
                      const phoneId = crypto.randomUUID();
                      await tx.contact_phones.create({
                        data: {
                          id: phoneId,
                          contact_id: relativeContact!.id,
                          phone_number: formattedNumber,
                          phone_type: phone.phonetype,
                        },
                      });
                    }
                  }
                });
                console.log(`[SaveContacts] Saved ${relPhoneNumbers.length} phones for relative ${relativeContact.id}`);
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
  }

  // ======================================================
  // CASE 2: No contacts returned → create from input only
  // ======================================================
  const input = response.input;

  if (!input) return contactMap;

  const firstName = (input.firstname || "").toLowerCase();
  const lastName = (input.lastname || "").toLowerCase();

  const mailingAddress = (poBoxAddress && poBoxAddress.length ? poBoxAddress : input.address || "").toLowerCase();
  const mailingCity = (input.city || "").toLowerCase();
  const mailingState = (input.state || "").toLowerCase();
  const mailingZip = input.zip || "";

  if (!firstName && !lastName && !mailingAddress) return contactMap;

  let contact = await prisma.contacts.findFirst({
    where: {
      first_name: { equals: firstName, mode: "insensitive" },
      last_name: { equals: lastName, mode: "insensitive" },
      mailing_address: { equals: mailingAddress, mode: "insensitive" },
    },
  });
  console.log("contact in skip trace processing", contact);

  if (!contact) {
    contact = await prisma.contacts.create({
      data: {
        id: crypto.randomUUID(),
        first_name: firstName,
        last_name: lastName,
        mailing_address: mailingAddress,
        mailing_city: mailingCity,
        mailing_state: mailingState,
        mailing_zip: mailingZip,
        user_id: "1",
      },
    });
  } else {
    contact = await prisma.contacts.update({
      where: { id: contact.id },
      data: {
        first_name: firstName,
        last_name: lastName,
        mailing_address: mailingAddress,
        mailing_city: mailingCity,
        mailing_state: mailingState,
        mailing_zip: mailingZip,
        user_id: "1",
      },
    });
  }

  await prisma.directSkip.upsert({
    where: { contactId: contact.id },
    update: {
      status: DirectSkipStatus.FAILED,
      skipTracedAt: new Date(),
    },
    create: {
      contactId: contact.id,
      status: DirectSkipStatus.FAILED,
      skipTracedAt: new Date(),
    },
  });

  contactMap.mainContactId = contact.id;

  return contactMap;
};

const savePropertyDetails = async (
  contactId: string,
  identityKey: string,
  propertyId: string,
  ownerId: string,
  response: DirectSkipSearchResponse,
) => {
  try {
    // const propertyId = response.propertyId;
    // const propertyAddr = response.input.property_address;
    // if (!propertyAddr) {
    //   console.warn(`[SavePropertyDetails] No property address in response for ${identityKey}`);
    //   return;
    // }

    // Query PropertyData directly
    const propData: any = await PropertyData.findOne({
      _id: propertyId,
    }).lean();

    if (!propData) {
      console.warn(`[SavePropertyDetails] No PropertyData found for ${identityKey} and property_id ${propertyId}`);
      return;
    }
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
    const rawDate = propData.last_sale_date;

    const lastSold: Date | null = rawDate ? new Date(rawDate) : null;
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
    // Use currList from the single matched property
    const currListRaw = propData.currList || [];
    const listStrArray: string[] = Array.isArray(currListRaw)
      ? currListRaw.map((l: any) => (typeof l === "string" ? l : l.name))
      : [];

    for (const listName of listStrArray) {
      if (!listName) continue;

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
    return propertyDetails?.id ?? null;
  } catch (err) {
    console.error(`[SavePropertyDetails] Error processing ${identityKey}:`, err);
  }
};

const normalize = (v?: string) => v?.toLowerCase().trim() || "";

export function resolveMainContactOptimized(
  contacts: DirectSkipContact[],
  input: { firstname?: string; lastname?: string },
): { contact: DirectSkipContact; name: DirectSkipNameRecord } | null {
  if (!contacts || contacts.length === 0) return null;

  const inputFirst = normalize(input.firstname);
  const inputLast = normalize(input.lastname);

  // 1. Try matching logic first
  for (const contact of contacts) {
    for (const name of contact.names || []) {
      const first = normalize(name.firstname);
      const last = normalize(name.lastname);

      // ✅ Full name match → keep API name
      if (first === inputFirst && last === inputLast) {
        return {
          contact,
          name,
        };
      }

      // ✅ First name match only → override last name in-place
      if (first === inputFirst && last !== inputLast) {
        name.lastname = input.lastname;
        return {
          contact,
          name,
        };
      }
    }
  }

  // 2. No match found → mutate first contact in-place
  const firstContact = contacts[0];

  if (!firstContact.names || firstContact.names.length === 0) {
    firstContact.names = [
      {
        firstname: input.firstname,
        lastname: input.lastname,
        deceased: undefined,
      },
    ];
  } else {
    const primaryName = firstContact.names[0];
    primaryName.firstname = input.firstname;
    primaryName.lastname = input.lastname;
    // deceased preserved automatically
  }

  return {
    contact: firstContact,
    name: firstContact.names[0],
  };
}

// ======================================================
// OWNERSHIP LOGIC
// ======================================================

interface OwnershipContact {
  contactId: string;
  isPrimary: boolean;
  ownershipType: string;
}

interface OwnershipResult {
  owners: OwnershipContact[];
  owner2PhonesToValidate: PhoneToValidate[]; // Up to first 2 phones
  owner2HasPhones: boolean;
}

/**
 * Resolves ownership contacts by checking owner1 and owner2 from MongoDB Owner document.
 * - If owner2 is empty or same as owner1 → single owner
 * - If owner2 is different → find/create contact for owner2
 */
export const resolveOwnershipContacts = async (
  ownerId: string,
  mainContactId: string,
  response: DirectSkipSearchResponse,
): Promise<OwnershipResult> => {
  const result: OwnershipResult = {
    owners: [],
    owner2PhonesToValidate: [],
    owner2HasPhones: false,
  };

  // Always add main contact as primary owner
  if (mainContactId) {
    result.owners.push({
      contactId: mainContactId,
      isPrimary: true,
      ownershipType: "owner",
    });
  }

  // Fetch Owner document from MongoDB
  const ownerDoc: any = await Owner.findOne({ _id: ownerId });
  if (!ownerDoc) {
    console.log(`[Ownership] No Owner document found for ${ownerId}`);
    return result;
  }

  const owner1First = normalize(ownerDoc.owner_first_name);
  const owner1Last = normalize(ownerDoc.owner_last_name);
  const owner2First = normalize(ownerDoc.owner_2_first_name);
  const owner2Last = normalize(ownerDoc.owner_2_last_name);

  // Check if owner2 exists
  if (!owner2First && !owner2Last) {
    console.log(`[Ownership] No owner2 found for ${ownerId}`);
    return result;
  }

  // Check if owner2 is same as owner1
  if (owner1First === owner2First && owner1Last === owner2Last) {
    console.log(`[Ownership] Owner2 is same as owner1 for ${ownerId}`);
    return result;
  }

  console.log(`[Ownership] Found different owner2: ${owner2First} ${owner2Last}`);

  // Get mailing address from owner doc or response input
  const mailingAddress = normalize(ownerDoc.mailing_address || response.input?.address);
  const mailingCity = normalize(ownerDoc.mailing_city || response.input?.city);
  const mailingState = normalize(ownerDoc.mailing_state || response.input?.state);
  const mailingZip = ownerDoc.mailing_zip_code || response.input?.zip || "";

  // Find or create contact for owner2
  let owner2Contact = await prisma.contacts.findFirst({
    where: {
      first_name: { equals: owner2First, mode: "insensitive" },
      last_name: { equals: owner2Last, mode: "insensitive" },
      mailing_address: { equals: mailingAddress, mode: "insensitive" },
    },
  });

  if (!owner2Contact) {
    owner2Contact = await prisma.contacts.create({
      data: {
        id: crypto.randomUUID(),
        first_name: owner2First,
        last_name: owner2Last,
        mailing_address: mailingAddress,
        mailing_city: mailingCity,
        mailing_state: mailingState,
        mailing_zip: mailingZip,
        user_id: "1",
      },
    });
    console.log(`[Ownership] Created owner2 contact ${owner2Contact.id}: ${owner2First} ${owner2Last}`);
  } else {
    console.log(`[Ownership] Found existing owner2 contact ${owner2Contact.id}`);
  }

  // Add owner2 to owners list
  result.owners.push({
    contactId: owner2Contact.id,
    isPrimary: false,
    ownershipType: "co-owner",
  });

  // Check if owner2 has phones in DirectSkip response
  // Look for owner2 in BOTH the contacts array AND in relatives, and collect ALL phones
  let owner2Phones: { phonenumber?: string; phonetype?: string }[] = [];

  console.log(`[Ownership] Searching for owner2 in DirectSkip response: "${owner2First}" "${owner2Last}"`);

  if (response.contacts) {
    console.log(`[Ownership] Checking ${response.contacts.length} contacts from DirectSkip...`);

    // First, check top-level contacts array
    for (let i = 0; i < response.contacts.length; i++) {
      const contact = response.contacts[i];

      for (const name of contact.names || []) {
        const first = normalize(name.firstname);
        const last = normalize(name.lastname);

        console.log(
          `[Ownership] Contact ${i + 1}: Checking "${first}" "${last}" (original: "${name.firstname}" "${name.lastname}")`,
        );

        if (first === owner2First && last === owner2Last) {
          console.log(`[Ownership] ✓ MATCH FOUND in contacts! Owner2 found in DirectSkip response`);

          if (contact.phones && contact.phones.length > 0) {
            // Add phones from contacts array
            owner2Phones.push(...contact.phones);
            console.log(
              `[Ownership] Added ${contact.phones.length} phones from contacts array (total: ${owner2Phones.length})`,
            );
          } else {
            console.log(`[Ownership] Owner2 match found in contacts but no phones available`);
          }
          break; // Found in this contact's names, move to next contact
        }
      }
    }

    // Also check relatives array (don't stop even if found in contacts)
    console.log(`[Ownership] Checking relatives arrays for additional Owner2 phones...`);

    for (let i = 0; i < response.contacts.length; i++) {
      const contact = response.contacts[i];

      if (contact.relatives && contact.relatives.length > 0) {
        console.log(`[Ownership] Contact ${i + 1} has ${contact.relatives.length} relatives`);

        for (let j = 0; j < contact.relatives.length; j++) {
          const relative = contact.relatives[j];
          const relativeName = normalize(relative.name);

          // Parse the relative name (format: "FIRSTNAME LASTNAME")
          const nameParts = relativeName.split(" ").filter(Boolean);
          const relativeFirst = nameParts[0] || "";
          const relativeLast = nameParts.slice(1).join(" ") || "";

          console.log(
            `[Ownership]   Relative ${j + 1}: Checking "${relativeFirst}" "${relativeLast}" (original: "${relative.name}")`,
          );

          if (relativeFirst === owner2First && relativeLast === owner2Last) {
            console.log(`[Ownership] ✓ MATCH FOUND in relatives! Owner2 found as relative`);

            if (relative.phones && relative.phones.length > 0) {
              // Add phones from relatives array
              owner2Phones.push(...relative.phones);
              console.log(
                `[Ownership] Added ${relative.phones.length} phones from relatives array (total: ${owner2Phones.length})`,
              );
            } else {
              console.log(`[Ownership] Owner2 match found in relatives but no phones available`);
            }
            break; // Found in this contact's relatives, move to next contact
          }
        }
      }
    }

    if (owner2Phones.length === 0) {
      console.log(`[Ownership] ✗ Owner2 NOT found in DirectSkip response (checked both contacts and relatives)`);
      console.log(`[Ownership] Expected: "${owner2First}" "${owner2Last}"`);
    } else {
      console.log(`[Ownership] Total phones collected for Owner2: ${owner2Phones.length}`);
    }
  }

  // Save first 3 owner2 phones to database (regardless of validation)
  const owner2PhonesToSave = owner2Phones.slice(0, 3);
  if (owner2PhonesToSave.length > 0) {
    result.owner2HasPhones = true;
    await prisma.$transaction(async (tx) => {
      for (const phone of owner2PhonesToSave) {
        if (!phone.phonenumber) continue;
        const formattedNumber = normalizeUSPhoneNumber(phone.phonenumber);
        const last10 = formattedNumber.slice(-10);
        const existing = await tx.contact_phones.findFirst({
          where: {
            contact_id: owner2Contact!.id,
            phone_number: {
              endsWith: last10,
            },
          },
        });

        if (existing) {
          await tx.contact_phones.update({
            where: { id: existing.id },
            data: { phone_type: phone.phonetype, phone_number: formattedNumber },
          });
        } else {
          const phoneId = crypto.randomUUID();
          await tx.contact_phones.create({
            data: {
              id: phoneId,
              contact_id: owner2Contact!.id,
              phone_number: formattedNumber,
              phone_type: phone.phonetype,
            },
          });
        }
      }
    });
    console.log(`[Ownership] Saved ${owner2PhonesToSave.length} phones for owner2 ${owner2Contact.id}`);

    // Add first 2 phones for validation
    const phonesToValidate = owner2Phones.slice(0, 2);
    for (const phone of phonesToValidate) {
      if (phone.phonenumber) {
        result.owner2PhonesToValidate.push({
          contactId: owner2Contact.id,
          phoneNumber: phone.phonenumber,
          phoneType: phone.phonetype || "Unknown",
          phoneTag: "", // Will be assigned in main loop
        });
      }
    }
    console.log(`[Ownership] Owner2 phones to validate: ${result.owner2PhonesToValidate.length}`);
  }

  return result;
};

/**
 * Creates PropertyOwnership records linking a property to its owners
 */
export const createPropertyOwnerships = async (propertyId: string, owners: OwnershipContact[]): Promise<void> => {
  for (const owner of owners) {
    try {
      await prisma.propertyOwnership.upsert({
        where: {
          propertyId_contactId: {
            propertyId,
            contactId: owner.contactId,
          },
        },
        update: {
          isPrimary: owner.isPrimary,
          ownershipType: owner.ownershipType,
          updatedAt: new Date(),
        },
        create: {
          propertyId,
          contactId: owner.contactId,
          isPrimary: owner.isPrimary,
          ownershipType: owner.ownershipType,
        },
      });
      console.log(
        `[Ownership] Created/updated ownership: property ${propertyId} → contact ${owner.contactId} (${owner.ownershipType}, primary: ${owner.isPrimary})`,
      );
    } catch (err) {
      console.error(`[Ownership] Error creating ownership for ${owner.contactId}:`, err);
    }
  }
};
