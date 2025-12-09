import pl from "nodejs-polars";
import path from "path";
import prisma from "../db";

// export const sendJobResult = async () => {
//   const [jobs, count] = await prisma.$transaction([
//     prisma.botOutputFile.findMany({
//       orderBy: { receivedAt: "desc" },
//     }),
//     prisma.botOutputFile.count(),
//   ]);

//   return { count, jobs };
// };

export const sendRecordForJob = async (jobId: number): Promise<Record<string, unknown>[]> => {
  const job = await prisma.botJobs.findFirst({
    where: { jobId },
  });

  if (!job?.resultFilePath) {
    console.warn(`No botOutputFile found for jobId=${jobId}`);
    return [];
  }

  const filePath = path.resolve(job.resultFilePath);

  try {
    const df = pl.readCSV(filePath);

    const columns = df.columns;
    const rowsArray = df.rows(); // array of arrays

    const rows = rowsArray.map((row) => {
      const obj: Record<string, unknown> = {};
      row.forEach((value, idx) => {
        obj[columns[idx]] = value;
      });
      return obj;
    });

    return rows;
  } catch (err) {
    console.error(`Failed to read CSV for jobId=${jobId} at ${filePath}`, err);
    return [];
  }
};

// //Pre direct skip check and approval check

// export const getApprovalForRecords = async () => {
//   // jo contacts nhi hai already unmei directskip Pending

//   //jo present hai unmei directskip kab hua

//   return { jobId: ["1"], recordId: ["dkdlfasdfl"] };
// };
