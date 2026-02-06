import prisma from "../db";
import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
import mongoose from "../mongoose";

type OwnerMigrationDoc = {
  _id: mongoose.Types.ObjectId;
  propertyIds: mongoose.Types.ObjectId[];
  stage?: string | null;
  decision?: string | null;
};
export const cleanUpNewData = async () => {
  const pipelineOwners = await prisma.pipeline.findMany({
    select: {
      ownerId: true,
    },
  });
  if (pipelineOwners.length === 0) return;

  const ownerObjectIds = pipelineOwners.map((o) => new mongoose.Types.ObjectId(o.ownerId));

  // 2️⃣ Fetch owners from Mongo
  const owners = await Owner.find(
    { _id: { $in: ownerObjectIds }, stage: { $exists: true, $ne: null }, decision: { $exists: true, $ne: null } },

    {
      propertyIds: 1,
      stage: 1,
      decision: 1,
    },
  ).lean<OwnerMigrationDoc[]>();

  if (owners.length === 0) return;

  // 3️⃣ Update properties PER owner
  for (const owner of owners) {
    if (!owner.propertyIds?.length) continue;

    await PropertyData.updateMany(
      { _id: { $in: owner.propertyIds } },
      {
        $set: {
          stage: owner.stage ?? null,
          decision: owner?.decision ?? null,
        },
      },
      { strict: false },
    );
  }

  console.log("✅ cleanUpNewData completed");
};
