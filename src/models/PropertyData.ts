import mongoose from "mongoose";

const PropertyDataSchema = new mongoose.Schema(
  {
    identityKey: { type: String, required: true },

    parcel: String,
    land_use_code: String,
    property_address: String,
    property_city: String,
    property_state: String,
    property_zip_code: mongoose.Schema.Types.Mixed,

    currList: Array,
    prevList: Array,
    isListChanged: Boolean,

    last_buyer: String,
    last_seller: String,
    previous_buyer: String,
    previous_seller: String,

    tax_lien: String,
    tax_delinquent_amount: String,
    years_delinquent: Number,

    last_sale_date: {
      type: Date,
    },
    sale_date: {
      type: Date,
    },
    case_date: {
      type: Date,
    },
    previous_sale_date: String,
    last_sale_price: String,
    previous_sale_price: String,

    year_built: Number,
    year_remodeled: Number,
    bedrooms: Number,
    bathrooms: Number,
    square_feet: String,
    cdu: String,
    heat: String,

    general_note: String,

    pay_all_current_taxes: String,
    pay_current_installment: String,
    pay_delinquent_taxes: String,
    pay_second_installment: String,
    vacant_abandon: String,

    // duplicated metadata
    clean: Boolean,
    jobId: String,
    botId: Number,
    delinquent_contract: String,
    // Special Assessments
    special_assessment_amount_2021: String,
    special_assessment_amount_2022: String,
    special_assessment_amount_2023: String,
    special_assessment_amount_2024: String,
    special_assessment_amount_2025: String,

    special_assessments_2021: String,
    special_assessments_2022: String,
    special_assessments_2023: String,
    special_assessments_2024: String,
    special_assessments_2025: String,

    special_assessments_code_2021: String,
    special_assessments_code_2022: String,
    special_assessments_code_2023: String,
    special_assessments_code_2024: String,
    special_assessments_code_2025: String,
    special_assessments_code_2026: String,

    syncedAt: Date,
  },
  { timestamps: true },
);

// Indexes for optimized aggregation (Fast Record Getter)
PropertyDataSchema.index({ "currList.list_updated_at": -1 });
PropertyDataSchema.index({ clean: 1, "currList.list_updated_at": -1 });
PropertyDataSchema.index({ botId: 1 });
PropertyDataSchema.index({ last_sale_date: -1 });

export const PropertyData = mongoose.model("PropertyData", PropertyDataSchema);
