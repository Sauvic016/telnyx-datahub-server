import mongoose, { Schema, Document } from 'mongoose';

export interface IScrappedData extends Document {
  [key: string]: any; // Flexible schema to allow any fields
}

const ScrappedDataSchema: Schema = new Schema(
  {
    // Flexible schema - allows any fields to be stored
  },
  {
    strict: false, // Allows fields not defined in schema
    timestamps: true, // Adds createdAt and updatedAt fields
  }
);

export const ScrappedData = mongoose.model<IScrappedData>('ScrappedData', ScrappedDataSchema);
