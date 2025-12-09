import pl, { DataFrame } from "nodejs-polars";

// ---------- Helpers ----------

export function loadCsv(filePath: string): DataFrame {
  // Synchronous read similar to pandas.read_csv
  return pl.readCSV(filePath);
}

function extractDynamicPhoneSets(columns: string[]): string[][] {
  const phoneSets: string[][] = [];
  const phonePattern = /Phone (\d+)$/;

  for (const col of columns) {
    const match = col.match(phonePattern);
    if (match) {
      const n = match[1];
      const setGroup = [`Phone ${n}`, `Phone Type ${n}`, `Phone Status ${n}`, `Phone Tags ${n}`];
      if (setGroup.every((c) => columns.includes(c))) {
        phoneSets.push(setGroup);
      }
    }
  }

  return phoneSets;
}

export function explodePhones(df: DataFrame): DataFrame {
  const columns = df.columns;
  const phoneSets = extractDynamicPhoneSets(columns);

  if (phoneSets.length === 0) {
    return df;
  }

  // Columns that are NOT phone-related
  const commonCols = columns.filter(
    (c) =>
      !(
        c.startsWith("Phone") ||
        c.startsWith("Phone Type") ||
        c.startsWith("Phone Status") ||
        c.startsWith("Phone Tags")
      )
  );

  // ✅ DataFrame -> array of row objects
  const records = df.toRecords() as Record<string, unknown>[];

  const rows: Record<string, unknown>[] = [];

  for (const row of records) {
    const base: Record<string, unknown> = {};
    for (const col of commonCols) {
      base[col] = row[col];
    }

    let hasPhone = false;

    for (const phoneSet of phoneSets) {
      const phoneRaw = row[phoneSet[0]];
      const phoneStr = phoneRaw === null || phoneRaw === undefined ? "" : String(phoneRaw).trim();

      if (phoneStr) {
        hasPhone = true;
        const newRow: Record<string, unknown> = { ...base };

        const numeric = Number(phoneStr.replace(/\D/g, ""));
        newRow["Phone"] = Number.isNaN(numeric) ? phoneStr : String(numeric);

        newRow["Phone Type"] = row[phoneSet[1]] ?? "";
        newRow["Phone Status"] = row[phoneSet[2]] ?? "";
        newRow["Phone Tags"] = row[phoneSet[3]] ?? "";

        rows.push(newRow);
      }
    }

    if (!hasPhone) {
      rows.push({
        ...base,
        Phone: "",
        "Phone Type": "",
        "Phone Status": "",
        "Phone Tags": "",
      });
    }
  }

  return pl.DataFrame(rows);
}

export function saveCsv(df: DataFrame, outputPath: string): void {
  const phoneCols = ["Phone", "Phone Type", "Phone Status", "Phone Tags"];

  // Normalize Phone to Utf8 and filter out blank phones
  let dfFiltered = df.withColumns(pl.col("Phone").cast(pl.Utf8).alias("Phone"));

  dfFiltered = dfFiltered.filter(pl.col("Phone").str.strip().neq(""));

  const allCols = dfFiltered.columns;

  // Remove dynamic phone columns except the unified phone fields
  const dynamicPhonePattern = /Phone( Type| Status| Tags)? \d+$/;
  let filteredCols = allCols.filter((col) => !dynamicPhonePattern.test(col) || phoneCols.includes(col));

  // Drop "exported from REISift.io" (case-insensitive, trimmed)
  filteredCols = filteredCols.filter((col) => col.trim().toLowerCase() !== "exported from reisift.io");

  // Find index of first column containing "zip"
  let zipIndex = filteredCols.findIndex((c) => c.toLowerCase().includes("zip"));
  if (zipIndex === -1) {
    zipIndex = filteredCols.length; // none found -> append at end
  }

  const preCols = filteredCols.slice(0, zipIndex + 1);
  const postCols = filteredCols.slice(zipIndex + 1).filter((c) => !phoneCols.includes(c));

  const finalCols = [...preCols, ...phoneCols, ...postCols];

  // IMPORTANT: select needs expressions, not raw strings
  const dfFinal = dfFiltered.select(...finalCols.map((c) => pl.col(c)));

  // Write CSV
  dfFinal.writeCSV(outputPath);
}

export function processCsv(filePath: string): void {
  const df = loadCsv(filePath);
  const exploded = explodePhones(df);
  saveCsv(exploded, filePath);
}
