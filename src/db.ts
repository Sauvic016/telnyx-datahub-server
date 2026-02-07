import { PrismaClient } from "./generated/prisma/client";

const databaseUrl = new URL(process.env.DATABASE_URL || "postgresql://localhost:5432/temp_telnyx");
if (!databaseUrl.searchParams.has("connection_limit")) {
  databaseUrl.searchParams.set("connection_limit", "20");
}

const prisma = new PrismaClient({
  datasourceUrl: databaseUrl.toString(),
});

export default prisma;
