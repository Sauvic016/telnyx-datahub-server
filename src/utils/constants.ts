export interface BotConfig {
  name: string;
  isStarter: boolean;
  cooldown: number;
  flow: number[] | null; // defines which bots follow this one
  errorCooldown: number;
}
const hour = 60 * 60 * 1000;
export const BOTMAP: Record<number, BotConfig> = {
  1: { name: "address-bot", isStarter: true, flow: [1, 4], cooldown: 3 * hour, errorCooldown: 48 * hour },
  2: { name: "ssfweb-parcel-bot", isStarter: true, flow: [2, 4], cooldown: 12 * hour, errorCooldown: 48 * hour },
  3: { name: "clerkweb-parcel-bot", isStarter: true, flow: [3, 4], cooldown: 12 * hour, errorCooldown: 48 * hour },
  4: { name: "summitoh_v5", isStarter: false, flow: null, cooldown: 6 * hour, errorCooldown: 48 * hour },
  5: { name: "summitoh_v5_standalone", isStarter: true, flow: null, cooldown: 6 * hour, errorCooldown: 48 * hour },
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
