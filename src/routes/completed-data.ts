import { Router } from "express";
import { randomUUID } from "crypto";
import { CompletedRecordsFilters, fetchCompletedRecords } from "../services/completed-data-fast";
import { getValidParam } from "../utils/query-params";
import { parseAmPmTime, isInTimeSlot } from "../utils/helper";
import { sortContactPhonesByTag } from "../utils/completed-formatter";
import prisma from "../db";

const router = Router();

interface DeleteRequestBody {
  isBulk: boolean;
  // For manual selection
  ids?: string[];
  // For bulk selection
  filter?: {
    listName?: string;
    dateRange?: { start: string; end: string };
    propertyStatus?: string;
    activity?: string;
  };
  limit?: number | "all";
  startIndex?: number;
  excludedIds?: string[];
}

router.get("/", async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page as string) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string) || 10));
    const skip = (page - 1) * limit;

    const filters: CompletedRecordsFilters = {};

    if (req.query.listName) {
      filters.listName = req.query.listName as string;
    }

    filters.propertyStatusId = getValidParam(req.query.propertyStatusId);

    if (req.query.startDate || req.query.endDate || req.query.filterDateType) {
      filters.dateRange = {
        type: req.query.filterDateType as string | undefined,
        startDate: req.query.startDate as string | undefined,
        endDate: req.query.endDate as string | undefined,
      };
    }

    const sortBy = req.query.sortBy as string | undefined;
    if (sortBy === "updatedAt" || sortBy === "lastSold" || sortBy === "case_date" || sortBy === "sale_date") {
      filters.sortBy = sortBy;
    }

    const sortOrder = req.query.sortOrder as string | undefined;
    if (sortOrder === "asc" || sortOrder === "desc") {
      filters.sortOrder = sortOrder;
    }

    const ACTIVITY_KEYS = new Set(["smsSent", "smsDelivered"]);
    function parseYesNo(value?: string): boolean | undefined {
      if (value === "yes") return true;
      if (value === "no") return false;
      return undefined;
    }
    const activityType = req.query.activityType as "smsSent" | "smsDelivered" | undefined;
    const activityValue = req.query.activityValue as string | undefined;

    if (activityType && ACTIVITY_KEYS.has(activityType)) {
      const parsed = parseYesNo(activityValue);

      if (parsed !== undefined) {
        filters.activity = {
          [activityType]: parsed,
        };
      }
    }

    const searchQuery = req.query.search as string | undefined;

    const result = await fetchCompletedRecords(
      Object.keys(filters).length > 0 ? filters : undefined,
      { skip, take: limit },
      searchQuery,
    );
    const lists = await prisma.list.findMany({
      select: { id: true, name: true },
      orderBy: { name: "asc" },
    });
    const propertyStatuses = await prisma.propertyStatus.findMany();
    res.json({
      data: result.rows,
      lists,
      page,
      limit,
      totalItems: result.total,
      totalPages: Math.ceil(result.total / limit),
      propertyStatuses,
    });
  } catch (error) {
    console.error("Error in /completed-data:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/delete", async (req, res) => {
  try {
    const body: DeleteRequestBody = req.body;

    let deletedCount = 0;

    if (body.isBulk) {
      // ============ BULK DELETE ============
      const { filter, limit, startIndex = 0, excludedIds = [] } = body;

      // Parse excluded IDs into ownerId-propertyId pairs
      const excludedPairs = excludedIds.map((id) => {
        const [contactId, propertyDetailsId] = id.split("_");
        return { contactId, propertyDetailsId };
      });

      const whereClause: any = {};

      // Apply filters
      if (filter?.listName && filter.listName !== "all-lists") {
        whereClause.propertyDetails = {
          lists: {
            some: {
              name: filter.listName,
            },
          },
        };
      }

      if (filter?.dateRange?.start && filter?.dateRange?.end) {
        whereClause.updatedAt = {
          gte: new Date(filter.dateRange.start),
          lte: new Date(filter.dateRange.end),
        };
      }

      if (filter?.propertyStatus) {
        whereClause.propertyDetails = {
          ...whereClause.propertyDetails,
          OR: [
            { primaryPropertyStatusId: filter.propertyStatus },
            { secondaryPropertyStatusId: filter.propertyStatus },
          ],
        };
      }

      if (filter?.activity) {
        // Add activity filter based on your schema
        // e.g., whereClause.sms_sent = filter.activity.sms_sent;
      }

      if (limit === "all") {
        const allRecords = await prisma.pipeline.findMany({
          where: whereClause,
          select: {
            contactId: true,
            propertyDetailsId: true,
          },
        });

        // Filter out excluded records
        const recordsToDelete = allRecords.filter(
          (record) =>
            !excludedPairs.some(
              (excluded) =>
                excluded.contactId === record.contactId && excluded.propertyDetailsId === record.propertyDetailsId,
            ),
        );

        // Delete in batches
        if (recordsToDelete.length > 0) {
          const result = await prisma.pipeline.deleteMany({
            where: {
              OR: recordsToDelete.map((record) => ({
                contactId: record.contactId,
                propertyDetailsId: record.propertyDetailsId,
              })),
            },
          });
          deletedCount = result.count;
        }
      } else {
        const recordsInRange = await prisma.pipeline.findMany({
          where: whereClause,
          orderBy: {
            updatedAt: "desc", // Use same ordering as your fetchCompletedRecords
          },
          skip: startIndex,
          take: limit,
          select: {
            contactId: true,
            propertyDetailsId: true,
          },
        });

        // Filter out excluded records
        const recordsToDelete = recordsInRange.filter(
          (record) =>
            !excludedPairs.some(
              (excluded) =>
                excluded.contactId === record.contactId && excluded.propertyDetailsId === record.propertyDetailsId,
            ),
        );

        // Delete the records
        if (recordsToDelete.length > 0) {
          const result = await prisma.pipeline.deleteMany({
            where: {
              OR: recordsToDelete.map((record) => ({
                contactId: record.contactId,
                propertyDetailsId: record.propertyDetailsId,
              })),
            },
          });
          deletedCount = result.count;
        }
      }
    } else {
      // ============ MANUAL DELETE ============
      const { ids = [] } = body;

      if (ids.length === 0) {
        return res.status(400).json({ error: "No IDs provided" });
      }

      // Parse IDs into ownerId-propertyId pairs
      const pairs = ids.map((id) => {
        const [contactId, propertyDetailsId] = id.split("_");
        return { contactId, propertyDetailsId };
      });

      // Delete the records
      const result = await prisma.pipeline.deleteMany({
        where: {
          OR: pairs.map((pair) => ({
            contactId: pair.contactId,
            propertyDetailsId: pair.propertyDetailsId,
          })),
        },
      });

      deletedCount = result.count;
    }

    return res.status(200).json({
      success: true,
      deletedCount,
      message: `Successfully deleted ${deletedCount} record(s)`,
    });
  } catch (error) {
    console.error("Error deleting records:", error);
    return res.status(500).json({
      success: false,
      error: "Failed to delete records",
    });
  }
});

interface UpdateAddressBody {
  propertyDetailsId: string;
  contactId: string;
  addressType: "mailing" | "property" | "confirmed";
  address: {
    address: string;
    city: string;
    state: string;
    zip: string;
  };
}

router.patch("/update-address", async (req, res) => {
  try {
    const { propertyDetailsId, contactId, addressType, address }: UpdateAddressBody = req.body;

    // Validate required fields
    if (!addressType || !address) {
      res.status(400).json({
        success: false,
        error: "addressType and address are required",
      });
      return;
    }

    if (!["mailing", "property", "confirmed"].includes(addressType)) {
      res.status(400).json({
        success: false,
        error: "Invalid addressType. Must be 'mailing', 'property', or 'confirmed'",
      });
      return;
    }

    let result;

    switch (addressType) {
      case "mailing":
        if (!contactId) {
          res.status(400).json({
            success: false,
            error: "contactId is required for mailing address",
          });
          return;
        }

        result = await prisma.contacts.update({
          where: { id: contactId },
          data: {
            mailing_address: address.address,
            mailing_city: address.city,
            mailing_state: address.state,
            mailing_zip: address.zip,
          },
        });
        break;

      case "property":
        if (!propertyDetailsId) {
          res.status(400).json({
            success: false,
            error: "propertyDetailsId is required for property address",
          });
          return;
        }

        result = await prisma.property_details.update({
          where: { id: propertyDetailsId },
          data: {
            property_address: address.address,
            property_city: address.city,
            property_state: address.state,
            property_zip: address.zip,
          },
        });
        break;

      case "confirmed":
        if (!contactId) {
          res.status(400).json({
            success: false,
            error: "contactId is required for confirmed address",
          });
          return;
        }

        // Find the DirectSkip record by contactId
        const directSkip = await prisma.directSkip.findUnique({
          where: { contactId: contactId },
        });

        if (!directSkip) {
          res.status(404).json({
            success: false,
            error: "DirectSkip record not found for this contact",
          });
          return;
        }

        // Update the confirmedAddress JSON field
        result = await prisma.directSkip.update({
          where: { contactId: contactId },
          data: {
            confirmedAddress: {
              street: address.address,
              city: address.city,
              state: address.state,
              zip: address.zip,
            },
          },
        });
        break;
    }

    res.status(200).json({
      success: true,
      message: `${addressType} address updated successfully`,
      data: result,
    });
    return;
  } catch (error) {
    console.error("Error updating address:", error);

    if ((error as any).code === "P2025") {
      res.status(404).json({
        success: false,
        error: "Record not found",
      });
      return;
    }

    res.status(500).json({
      success: false,
      error: "Failed to update address",
    });
    return;
  }
});

// ==================== SMS Drip Types ====================

interface SmsDripMessage {
  sequence: number;
  delay_unit?: "sec" | "min" | "hr" | "day" | "week";
  delay_seconds?: number;
  wait_between_sends_sec?: number;
  template_ids?: string[];
}

interface SmsDripTimeSlot {
  start: string;
  end: string;
}

interface SmsDripTarget {
  contactId: string;
  propertyId?: string;
}

// Parse "contactId-propertyId" string format from frontend
function parseTargetString(targetStr: string): SmsDripTarget {
  const [contactId, propertyId] = targetStr.split("_");
  return { contactId, propertyId };
}

function parseTargets(targets: string[]): SmsDripTarget[] {
  return targets.map(parseTargetString).filter((t) => t.contactId);
}

interface PhoneFilter {
  selectedTags?: string[];
  selectedCallerIds?: string[];
  selectedPhoneIndex?: number[];
  excludedTags: string[];
  excludedCallerIds: string[];
  excludedPhoneIndex: number[];
  andOr: "and" | "or";
}

interface ContactFilter {
  listName?: string;
  propertyStatusId?: string;
  filterDateType?: string; // "today", "yesterday", "this_week", "last_week", "this_month", "last_month", "custom"
  startDate?: string;
  endDate?: string;
  sortBy?: "updatedAt" | "lastSold";
  sortOrder?: "asc" | "desc";
  activityType?: "smsSent" | "smsDelivered";
  activityValue?: "yes" | "no";
  search?: string;
}

interface SmsDripFilters {
  phoneFilters?: PhoneFilter;
  contactFilter?: ContactFilter;
}

interface SmsDripRequestBody {
  name: string;
  telnyxNumberIds?: string[];
  notes?: string;
  jobType?: string;
  scheduleDate?: string;
  schedule?: boolean;
  messages: SmsDripMessage[];
  timeSlots?: SmsDripTimeSlot[];
  targetIds?: string[]; // "contactId_propertyId" format
  filters?: SmsDripFilters;
}

interface SmsDripBulkRequestBody {
  name: string;
  telnyxNumberIds?: string[];
  notes?: string;
  jobType?: string;
  scheduleDate?: string;
  schedule?: boolean;
  messages: SmsDripMessage[];
  timeSlots?: SmsDripTimeSlot[];
  limit: number;
  startIndex?: number;
  excludedIds?: string[]; // "contactId_propertyId" format to exclude
  additionalIds?: string[]; // "contactId_propertyId" format to add (duplicates ignored)
  filters?: SmsDripFilters;
}

// ==================== SMS Drip Helpers ====================

const SECONDS_MAP: Record<string, number> = {
  sec: 1,
  min: 60,
  hr: 3600,
  day: 86400,
  week: 604800,
};

async function fetchTelnyxNumbers(ids: string[]) {
  if (ids.length === 0) return [];
  return prisma.telnyx_numbers.findMany({
    where: { id: { in: ids } },
    select: { id: true },
  });
}

function parseScheduleDate(scheduleDate?: string): Date | null {
  if (!scheduleDate) return null;
  return new Date(scheduleDate.split("T")[0]);
}

function parseTimeSlots(timeSlots: SmsDripTimeSlot[]) {
  return timeSlots.map((slot) => ({
    startTime: parseAmPmTime(slot.start),
    endTime: parseAmPmTime(slot.end),
  }));
}

async function createJob(params: {
  jobId: string;
  name: string;
  notes: string;
  jobType: string;
  startTime: Date | null;
  initialStatus: string;
  telnyxNumbers: { id: string }[];
  parsedTimeSlots: { startTime: Date; endTime: Date }[];
}) {
  await prisma.jobs.create({
    data: {
      id: params.jobId,
      user_id: "1", // TODO: Get from auth
      name: params.name,
      notes: params.notes,
      job_type: params.jobType,
      start_time: params.startTime,
      status: params.initialStatus as any,
      created_at: new Date(),
      job_telnyx_association: {
        create: params.telnyxNumbers.map((tn) => ({ telnyx_number_id: tn.id })),
      },
      job_time_slots: {
        create: params.parsedTimeSlots.map((slot) => ({
          id: randomUUID(),
          start_time: slot.startTime,
          end_time: slot.endTime,
        })),
      },
    },
  });
}

async function createJobMessages(jobId: string, messages: SmsDripMessage[]) {
  for (const msg of messages) {
    const delayUnit = msg.delay_unit || "sec";
    const delaySeconds = (msg.delay_seconds || 0) * (SECONDS_MAP[delayUnit] || 1);
    const templateIds = msg.template_ids || [];

    await prisma.job_messages.create({
      data: {
        id: randomUUID(),
        job_id: jobId,
        sequence: msg.sequence,
        delay_unit: delayUnit,
        delay_seconds: delaySeconds,
        wait_between_sends_sec: msg.wait_between_sends_sec || 5,
        created_at: new Date(),
        job_message_template_association: {
          create: templateIds.map((templateId) => ({ template_id: templateId })),
        },
      },
    });
  }
}

async function createJobTargets(
  jobId: string,
  targets: SmsDripTarget[],
  phoneFilters?: PhoneFilter,
): Promise<{ targetsCreated: number; contactsWithoutMatchingPhones: number }> {
  if (targets.length === 0) {
    return { targetsCreated: 0, contactsWithoutMatchingPhones: 0 };
  }

  const contactIds = targets.map((t) => t.contactId);
  const validPropertyIds = targets.map((t) => t.propertyId).filter((id): id is string => !!id);

  // Get owner contacts for these properties
  const owners =
    validPropertyIds.length > 0
      ? await prisma.propertyOwnership.findMany({
          where: { propertyId: { in: validPropertyIds } },
          select: { contactId: true, propertyId: true },
        })
      : [];

  // Merge owner contactIds with target contactIds
  const ownerContactIds = owners.map((o) => o.contactId);
  const allContactIds = [...new Set([...contactIds, ...ownerContactIds])];

  // Build phone query with filters
  const phoneWhereClause: any = { contact_id: { in: allContactIds } };

  // Build inclusion conditions
  const inclusionConditions: any[] = [];

  if (phoneFilters?.selectedTags?.length) {
    inclusionConditions.push({ phone_tags: { in: phoneFilters.selectedTags } });
  }

  if (phoneFilters?.selectedCallerIds?.length) {
    inclusionConditions.push({ telynxLookup: { caller_id: { in: phoneFilters.selectedCallerIds } } });
  }

  // Apply inclusion conditions based on andOr
  if (inclusionConditions.length > 0) {
    if (phoneFilters?.andOr === "or") {
      phoneWhereClause.OR = inclusionConditions;
    } else {
      // AND logic - all conditions must be met
      phoneWhereClause.AND = inclusionConditions;
    }
  }

  // Build exclusion conditions (always applied with AND logic)
  const exclusionConditions: any[] = [];

  if (phoneFilters?.excludedTags?.length) {
    exclusionConditions.push({
      OR: [{ phone_tags: null }, { phone_tags: { notIn: phoneFilters.excludedTags } }],
    });
  }

  if (phoneFilters?.excludedCallerIds?.length) {
    exclusionConditions.push({
      OR: [{ telynxLookup: null }, { telynxLookup: { caller_id: { notIn: phoneFilters.excludedCallerIds } } }],
    });
  }

  // Apply exclusion conditions
  if (exclusionConditions.length > 0) {
    if (phoneWhereClause.AND) {
      phoneWhereClause.AND.push(...exclusionConditions);
    } else {
      phoneWhereClause.AND = exclusionConditions;
    }
  }

  const contactPhonesRaw = await prisma.contact_phones.findMany({
    where: phoneWhereClause,
    select: { id: true, contact_id: true, phone_tags: true },
  });

  // Sort phones by DS tags (DS1, DS2, etc. come first in order)
  const contactPhones = sortContactPhonesByTag(contactPhonesRaw);

  // Group phones by contact_id for index filtering
  const phonesByContact = new Map<string, { id: string; contact_id: string; phone_tags: string | null }[]>();
  for (const phone of contactPhones) {
    if (!phone.contact_id) continue;
    const existing = phonesByContact.get(phone.contact_id) || [];
    existing.push(phone as { id: string; contact_id: string; phone_tags: string | null });
    phonesByContact.set(phone.contact_id, existing);
  }

  // Apply phone index filters (selected and excluded)
  let filteredPhones: { id: string; contact_id: string }[] = [];
  const selectedIndexSet = phoneFilters?.selectedPhoneIndex?.length ? new Set(phoneFilters.selectedPhoneIndex) : null;
  const excludedIndexSet = phoneFilters?.excludedPhoneIndex?.length ? new Set(phoneFilters.excludedPhoneIndex) : null;

  for (const [, phones] of phonesByContact) {
    for (let i = 0; i < phones.length; i++) {
      // Check if index is selected (if no filter, all are selected)
      const isSelected = selectedIndexSet ? selectedIndexSet.has(i) : true;
      // Check if index is excluded
      const isExcluded = excludedIndexSet ? excludedIndexSet.has(i) : false;

      if (isSelected && !isExcluded) {
        filteredPhones.push(phones[i]);
      }
    }
  }

  // Map contactId -> propertyId
  const propertyByContact = new Map(targets.map((t) => [t.contactId, t.propertyId]));
  for (const owner of owners) {
    if (!propertyByContact.has(owner.contactId)) {
      propertyByContact.set(owner.contactId, owner.propertyId);
    }
  }

  // Create targets - one per matching phone
  const jobTargetsData = filteredPhones.map((phone) => ({
    id: randomUUID(),
    job_id: jobId,
    contact_id: phone.contact_id,
    phone_id: phone.id,
    property_id: propertyByContact.get(phone.contact_id) || null,
    status: "pending" as const,
    last_sequence_sent: 0,
  }));

  if (jobTargetsData.length > 0) {
    await prisma.job_targets.createMany({ data: jobTargetsData });
  }

  const contactsWithPhones = new Set(filteredPhones.map((p) => p.contact_id));
  const contactsWithoutMatchingPhones = targets.filter((t) => !contactsWithPhones.has(t.contactId)).length;

  return { targetsCreated: jobTargetsData.length, contactsWithoutMatchingPhones };
}

async function finalizeJobStatus(
  jobId: string,
  parsedTimeSlots: { startTime: Date; endTime: Date }[],
): Promise<{ inSlot: boolean }> {
  const inSlot = isInTimeSlot(parsedTimeSlots);
  const newStatus = !inSlot && parsedTimeSlots.length > 0 ? "scheduled" : "processing";

  await prisma.jobs.update({
    where: { id: jobId },
    data: { status: newStatus },
  });

  return { inSlot: inSlot || parsedTimeSlots.length === 0 };
}

// ==================== SMS Drip Routes ====================

router.post("/sms-drip-bulk", async (req, res) => {
  try {
    const body: SmsDripBulkRequestBody = req.body;
    const {
      telnyxNumberIds = [],
      notes = "",
      jobType = "bulk",
      scheduleDate,
      schedule,
      messages,
      timeSlots = [],
      limit,
      startIndex = 0,
      excludedIds = [],
      additionalIds = [],
      filters,
    } = body;

    const { phoneFilters, contactFilter } = filters || {};

    if (!body.name) {
      return res.status(400).json({ error: "Job name is required" });
    }
    if (!messages?.length) {
      return res.status(400).json({ error: "At least one message is required" });
    }
    if (!limit || limit <= 0) {
      return res.status(400).json({ error: "Limit must be a positive number" });
    }

    // Validate phone filters - at least one filter must be provided
    const hasPhoneFilter =
      (phoneFilters?.selectedTags?.length ?? 0) > 0 ||
      (phoneFilters?.selectedCallerIds?.length ?? 0) > 0 ||
      (phoneFilters?.selectedPhoneIndex?.length ?? 0) > 0 ||
      (phoneFilters?.excludedTags?.length ?? 0) > 0 ||
      (phoneFilters?.excludedCallerIds?.length ?? 0) > 0 ||
      (phoneFilters?.excludedPhoneIndex?.length ?? 0) > 0;

    if (!hasPhoneFilter) {
      return res
        .status(400)
        .json({ error: "Please select at least one phone filter (tags, caller ID, or phone index)" });
    }

    // Build filters for fetchCompletedRecords from contactFilter
    const recordFilters: CompletedRecordsFilters = {};
    if (contactFilter?.listName) recordFilters.listName = contactFilter.listName;
    if (contactFilter?.propertyStatusId) recordFilters.propertyStatusId = contactFilter.propertyStatusId;
    if (contactFilter?.filterDateType || contactFilter?.startDate || contactFilter?.endDate) {
      recordFilters.dateRange = {
        type: contactFilter.filterDateType,
        startDate: contactFilter.startDate,
        endDate: contactFilter.endDate,
      };
    }
    if (contactFilter?.sortBy) recordFilters.sortBy = contactFilter.sortBy;
    if (contactFilter?.sortOrder) recordFilters.sortOrder = contactFilter.sortOrder;
    if (contactFilter?.activityType && contactFilter?.activityValue) {
      const activityBool = contactFilter.activityValue === "yes";
      recordFilters.activity = { [contactFilter.activityType]: activityBool };
    }

    // Fetch records using the same logic as GET /completed-data
    const { rows } = await fetchCompletedRecords(
      recordFilters,
      { skip: startIndex, take: limit },
      contactFilter?.search,
    );

    // Extract targets from fetched records
    const excludedSet = new Set(excludedIds);
    const seenIds = new Set<string>();

    const targetsFromRecords: SmsDripTarget[] = rows
      .map((row: any) => ({
        contactId: row.contact?.id || row.contactId,
        propertyId: row.propertyDetails?.id || row.propertyDetailsId,
      }))
      .filter((t: SmsDripTarget) => t.contactId)
      .filter((t: SmsDripTarget) => !excludedSet.has(`${t.contactId}_${t.propertyId}`));

    // Track seen IDs from records
    for (const t of targetsFromRecords) {
      seenIds.add(`${t.contactId}_${t.propertyId}`);
    }

    // Parse and add additional targets (skip duplicates and excluded)
    const additionalTargets = parseTargets(additionalIds)
      .filter((t) => !excludedSet.has(`${t.contactId}_${t.propertyId}`))
      .filter((t) => {
        const key = `${t.contactId}_${t.propertyId}`;
        if (seenIds.has(key)) return false;
        seenIds.add(key);
        return true;
      });

    const targets = [...targetsFromRecords, ...additionalTargets];

    if (targets.length === 0) {
      return res.status(400).json({ error: "No records found matching the filters" });
    }

    const jobId = randomUUID();
    const telnyxNumbers = await fetchTelnyxNumbers(telnyxNumberIds);
    const startTime = parseScheduleDate(scheduleDate);
    const parsedTimeSlots = parseTimeSlots(timeSlots);
    const initialStatus = schedule ? "scheduled" : "processing";

    await createJob({
      jobId,
      name: body.name,
      notes,
      jobType,
      startTime,
      initialStatus,
      telnyxNumbers,
      parsedTimeSlots,
    });
    await createJobMessages(jobId, messages);
    const { targetsCreated, contactsWithoutMatchingPhones } = await createJobTargets(jobId, targets, phoneFilters);
    const { inSlot } = await finalizeJobStatus(jobId, parsedTimeSlots);

    return res.status(inSlot ? 201 : 200).json({
      message: inSlot ? "Job created successfully" : "Job scheduled successfully, waiting for time slot.",
      jobId,
      targetsCreated,
      recordsMatched: rows.length,
      contactsWithoutMatchingPhones,
    });
  } catch (error) {
    console.error("Error creating SMS drip bulk job:", error);
    return res.status(500).json({
      error: "Failed to create SMS drip bulk job",
      details: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

router.post("/sms-drip-manual", async (req, res) => {
  try {
    const body: SmsDripRequestBody & { targetIds: string[] } = req.body;
    const {
      telnyxNumberIds = [],
      notes = "",
      jobType = "bulk",
      scheduleDate,
      schedule,
      messages,
      timeSlots = [],
      targetIds,
      filters,
    } = body;

    const { phoneFilters } = filters || {};

    if (!body.name) {
      return res.status(400).json({ error: "Job name is required" });
    }
    if (!messages?.length) {
      return res.status(400).json({ error: "At least one message is required" });
    }
    if (!targetIds?.length) {
      return res.status(400).json({ error: "At least one target is required" });
    }

    // Validate phone filters - at least one filter must be provided
    const hasPhoneFilter =
      (phoneFilters?.selectedTags?.length ?? 0) > 0 ||
      (phoneFilters?.selectedCallerIds?.length ?? 0) > 0 ||
      (phoneFilters?.selectedPhoneIndex?.length ?? 0) > 0 ||
      (phoneFilters?.excludedTags?.length ?? 0) > 0 ||
      (phoneFilters?.excludedCallerIds?.length ?? 0) > 0 ||
      (phoneFilters?.excludedPhoneIndex?.length ?? 0) > 0;

    if (!hasPhoneFilter) {
      return res
        .status(400)
        .json({ error: "Please select at least one phone filter (tags, caller ID, or phone index)" });
    }

    // Parse "contactId_propertyId" strings into target objects
    const targets = parseTargets(targetIds);

    const jobId = randomUUID();
    const telnyxNumbers = await fetchTelnyxNumbers(telnyxNumberIds);
    const startTime = parseScheduleDate(scheduleDate);
    const parsedTimeSlots = parseTimeSlots(timeSlots);
    const initialStatus = schedule ? "scheduled" : "processing";

    await createJob({
      jobId,
      name: body.name,
      notes,
      jobType,
      startTime,
      initialStatus,
      telnyxNumbers,
      parsedTimeSlots,
    });
    await createJobMessages(jobId, messages);
    const { targetsCreated, contactsWithoutMatchingPhones } = await createJobTargets(jobId, targets, phoneFilters);
    const { inSlot } = await finalizeJobStatus(jobId, parsedTimeSlots);

    return res.status(inSlot ? 201 : 200).json({
      message: inSlot ? "Job created successfully" : "Job scheduled successfully, waiting for time slot.",
      jobId,
      targetsCreated,
      contactsProcessed: targets.length,
      contactsWithoutMatchingPhones,
    });
  } catch (error) {
    console.error("Error creating SMS drip manual job:", error);
    return res.status(500).json({
      error: "Failed to create SMS drip manual job",
      details: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

export default router;
