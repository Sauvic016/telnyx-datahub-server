import mongoose from "mongoose";

const OwnerSchema = new mongoose.Schema(
  {
    identityKey: { type: String, required: true, unique: true },

    owner_first_name: String,
    owner_last_name: String,
    company_name_full_name: String,

    mailing_address: String,
    mailing_city: String,
    mailing_state: String,
    mailing_zip_code: String,

    inPostgres: Boolean,

    bankruptcy: String,
    foreclosure: String,
    treasurer_code: String,

    // duplicated ingestion metadata
    clean: Boolean,
    jobId: String,
    botId: Number,

    delinquent_contract: String,

    syncedAt: Date,

    propertyIds: [{ type: mongoose.Schema.Types.ObjectId, ref: "PropertyData" }],
  },
  { timestamps: true }
);

OwnerSchema.index({ propertyIds: 1 });
export const Owner = mongoose.model("Owner", OwnerSchema);
