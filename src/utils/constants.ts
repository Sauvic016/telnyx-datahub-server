const hour = 60 * 60 * 1000;

interface BotConfig {
  name: string;
  isStarter: boolean;
  cooldown: number;
  flow: number[] | null; // defines which bots follow this one
  errorCooldown: number;
  runAt?: string[];
  timezone?: string;
}

export const BOTMAP: Record<number, BotConfig> = {
  1: {
    name: "address-bot",
    isStarter: true,
    flow: [1, 4],
    cooldown: 3 * hour,
    errorCooldown: hour,
    runAt: [],
  },
  2: {
    name: "ssfweb-parcel-bot",
    isStarter: true,
    flow: [2, 4],
    cooldown: hour,
    errorCooldown: hour,
    runAt: ["09:00", "15:00"],
    timezone: "America/New_York"
  },
  3: {
    name: "clerkweb-parcel-bot",
    isStarter: true,
    flow: [3, 4],
    cooldown: hour,
    errorCooldown: hour,
    runAt: ["09:00", "15:00"],
    timezone: "America/New_York"
  },
  4: {
    name: "summitoh_v5",
    isStarter: false,
    flow: null,
    cooldown: hour,
    errorCooldown: hour,
    runAt: [],
  },
  5: {
    name: "summitoh_master",
    isStarter: true,
    flow: null,
    cooldown: 3 * hour,
    errorCooldown: hour,
    runAt: [],
  },
};

export const us_state = [
  "al", "alabama",
  "ak", "alaska",
  "az", "arizona",
  "ar", "arkansas",
  "ca", "california",
  "co", "colorado",
  "ct", "connecticut",
  "de", "delaware",
  "fl", "florida",
  "ga", "georgia",
  "hi", "hawaii",
  "id", "idaho",
  "il", "illinois",
  "in", "indiana",
  "ia", "iowa",
  "ks", "kansas",
  "ky", "kentucky",
  "la", "louisiana",
  "me", "maine",
  "md", "maryland",
  "ma", "massachusetts",
  "mi", "michigan",
  "mn", "minnesota",
  "ms", "mississippi",
  "mo", "missouri",
  "mt", "montana",
  "ne", "nebraska",
  "nv", "nevada",
  "nh", "new hampshire",
  "nj", "new jersey",
  "nm", "new mexico",
  "ny", "new york",
  "nc", "north carolina",
  "nd", "north dakota",
  "oh", "ohio",
  "ok", "oklahoma",
  "or", "oregon",
  "pa", "pennsylvania",
  "ri", "rhode island",
  "sc", "south carolina",
  "sd", "south dakota",
  "tn", "tennessee",
  "tx", "texas",
  "ut", "utah",
  "vt", "vermont",
  "va", "virginia",
  "wa", "washington",
  "wv", "west virginia",
  "wi", "wisconsin",
  "wy", "wyoming"
]


export const SUMMITOH_SPECIAL_CODES = new Set([
  "C05",
  "M91",
  "M26",
  "M24",
  "M87",
  "M11",
  "M84",
  "M87",
  "M29",
  "M14",
  "C05",
  "M12",
  "M92",
  "M55",
  "C12",
  "C55",
  "M31",
  "T11",
  "T87",
  "M90",
  "55 NEORSD",
  "12 NEORSD",
  "M62",
]);