import prisma from "../db";
import "dotenv/config";

async function main() {
  const items = [
    "Akron Deliquent Water Bill",
    "Akron Water Shutoff",
    "Preforclosure",
    "Deliquent Tax",
    "Special Assessments",
    "Tax Lien",
    "Vacant",
    "Abandoned",
  ];

  for (const name of items) {
    await prisma.list.upsert({
      where: { name },
      update: {},
      create: { name },
    });
  }

  console.log("Seed completed!");
}

main()
  .then(() => prisma.$disconnect())
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });
