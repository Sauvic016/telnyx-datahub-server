import pd from "nodejs-polars";
import path, { dirname } from "path";
import prisma from "../db";
import { fileExists } from "../utils/helper";
import { BOTMAP } from "../utils/constants";

export const sendRecordForJob = async (jobId: string): Promise<Record<string, unknown>[]> => {
  const job = await prisma.botJobs.findFirst({
    where: { jobId },
  });

  if (!job) {
    console.warn(`No job for jobId=${jobId}`);
    return [];
  }

  const filePath = path.join(process.cwd(), "job_result", `final_output_${jobId}.csv`);

  if (!(await fileExists(filePath))) {
    return [];
  }

  try {
    const df = pd.readCSV(filePath);

    const columns = df.columns;
    const rowsArray = df.rows(); // array of arrays

    // Remove bot_{botId}_ prefix from column names based on bot flow
    const botConfig = job.startedByBotId ? BOTMAP[job.startedByBotId] : null;
    const botFlow = botConfig?.flow ?? [job.startedByBotId];

    const cleanedColumns = columns.map((col) => {
      for (const botId of botFlow) {
        const prefix = `bot_${botId}_`;
        if (col.startsWith(prefix)) {
          return col.slice(prefix.length);
        }
      }
      return col;
    });

    const rows = rowsArray.map((row) => {
      const obj: Record<string, unknown> = {};
      row.forEach((value, idx) => {
        obj[cleanedColumns[idx]] = value;
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
