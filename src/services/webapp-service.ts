import prisma from "../db";
import { ScrappedData } from "../models/ScrappedData";

export const sendRecordForJob = async (jobId: string): Promise<Record<string, unknown>[]> => {
  const job = await prisma.botJobs.findFirst({
    where: { jobId },
  });

  if (!job) {
    console.warn(`No job for jobId=${jobId}`);
    return [];
  }

  try {
    const records = await ScrappedData.find({ jobId }).lean();
    return records as unknown as Record<string, unknown>[];
  } catch (err) {
    console.error(`Failed to fetch records from MongoDB for jobId=${jobId}`, err);
    return [];
  }
};

// //Pre direct skip check and approval check

// export const getApprovalForRecords = async () => {
//   // jo contacts nhi hai already unmei directskip Pending

//   //jo present hai unmei directskip kab hua

//   return { jobId: ["1"], recordId: ["dkdlfasdfl"] };
// };
