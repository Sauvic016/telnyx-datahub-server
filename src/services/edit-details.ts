import mongoose from "mongoose";
import { Owner } from "../models/Owner";
import { PropertyData } from "../models/PropertyData";
import type { AddressUpdate, EditDetailsResponse } from "../types/edit-details.types";

export default async function editDetails(
  type: "property" | "mailing",
  ownerId: string,
  propertyId: string,
  updateData: AddressUpdate,
): Promise<EditDetailsResponse> {
  try {
    // Validate MongoDB ObjectId format
    if (!mongoose.Types.ObjectId.isValid(ownerId)) {
      return {
        success: false,
        error: "Invalid ownerId format",
        statusCode: 400,
      };
    }

    if (!mongoose.Types.ObjectId.isValid(propertyId)) {
      return {
        success: false,
        error: "Invalid propertyId format",
        statusCode: 400,
      };
    }

    // Validate owner exists
    const owner = await Owner.findById(ownerId);
    if (!owner) {
      return {
        success: false,
        error: "Owner not found",
        statusCode: 404,
      };
    }

    // Validate property exists
    const property = await PropertyData.findById(propertyId);
    if (!property) {
      return {
        success: false,
        error: "Property not found",
        statusCode: 404,
      };
    }

    // Helper function to sanitize string input
    const sanitize = (value: string | undefined): string | undefined => {
      if (!value) return undefined;
      const trimmed = value.trim();
      return trimmed.length > 0 ? trimmed : undefined;
    };

    // Process property address updates
    if (type === "property") {
      const dbUpdateData: Record<string, string> = {};
      const modifiedFields: string[] = [];

      const propertyAddress = sanitize(updateData.newPropertyAddress);
      const propertyCity = sanitize(updateData.newPropertyCity);
      const propertyState = sanitize(updateData.newPropertyState);
      const propertyZip = sanitize(updateData.newPropertyZip);

      if (propertyAddress) {
        dbUpdateData.property_address = propertyAddress;
        modifiedFields.push("property_address");
      }
      if (propertyCity) {
        dbUpdateData.property_city = propertyCity;
        modifiedFields.push("property_city");
      }
      if (propertyState) {
        dbUpdateData.property_state = propertyState;
        modifiedFields.push("property_state");
      }
      if (propertyZip) {
        dbUpdateData.property_zip_code = propertyZip;
        modifiedFields.push("property_zip_code");
      }

      if (Object.keys(dbUpdateData).length === 0) {
        return {
          success: false,
          error: "No valid property fields provided to update",
          statusCode: 400,
        };
      }

      // Perform atomic update
      const result = await PropertyData.updateOne({ _id: propertyId }, { $set: dbUpdateData });

      // Check if all property address fields are now complete and set clean field
      const updatedProperty = await PropertyData.findById(propertyId);
      if (updatedProperty) {
        const isComplete =
          updatedProperty.property_address &&
          updatedProperty.property_city &&
          updatedProperty.property_state &&
          updatedProperty.property_zip_code;

        if (isComplete && !updatedProperty.clean) {
          await PropertyData.updateOne({ _id: propertyId }, { $set: { clean: true } });
          modifiedFields.push("clean");
        }
      }

      if (result.matchedCount === 0) {
        return {
          success: false,
          error: "Property not found during update",
          statusCode: 404,
        };
      }

      return {
        success: true,
        summary: "Property details updated",
        message: "Property address updated successfully",
        modifiedFields,
      };
    }

    // Process mailing address updates
    else if (type === "mailing") {
      const dbUpdateData: Record<string, string> = {};
      const modifiedFields: string[] = [];

      const mailingAddress = sanitize(updateData.newMailingAddress);
      const mailingCity = sanitize(updateData.newMailingCity);
      const mailingState = sanitize(updateData.newMailingState);
      const mailingZip = sanitize(updateData.newMailingZip);

      if (mailingAddress) {
        dbUpdateData["mailing_address"] = mailingAddress;
        modifiedFields.push("mailing_address");
      }
      if (mailingCity) {
        dbUpdateData["mailing_city"] = mailingCity;
        modifiedFields.push("mailing_city");
      }
      if (mailingState) {
        dbUpdateData["mailing_state"] = mailingState;
        modifiedFields.push("mailing_state");
      }
      if (mailingZip) {
        dbUpdateData["mailing_zip_code"] = mailingZip;
        modifiedFields.push("mailing_zip_code");
      }

      if (Object.keys(dbUpdateData).length === 0) {
        return {
          success: false,
          error: "No valid mailing fields provided to update",
          statusCode: 400,
        };
      }

      console.log(dbUpdateData);
      // Perform atomic update
      const result = await Owner.updateOne({ _id: ownerId }, { $set: dbUpdateData });

      // Check if all mailing address fields are now complete and set clean field
      const updatedOwner = await Owner.findById(ownerId);
      if (updatedOwner) {
        const isComplete =
          updatedOwner.mailing_address &&
          updatedOwner.mailing_city &&
          updatedOwner.mailing_state &&
          updatedOwner.mailing_zip_code;

        if (isComplete && !updatedOwner.clean) {
          await Owner.updateOne({ _id: ownerId }, { $set: { clean: true } });
          modifiedFields.push("clean");
        }
      }

      if (result.matchedCount === 0) {
        return {
          success: false,
          error: "Owner not found during update",
          statusCode: 404,
        };
      }

      return {
        success: true,
        summary: "Mailing details updated",
        message: "Mailing address updated successfully",
        modifiedFields,
      };
    }

    // This should never be reached due to TypeScript typing
    return {
      success: false,
      error: "Invalid type parameter",
      statusCode: 400,
    };
  } catch (error) {
    console.error("Error in editDetails service:", error);
    return {
      success: false,
      error: error instanceof Error ? error.message : "Internal server error",
      statusCode: 500,
    };
  }
}
