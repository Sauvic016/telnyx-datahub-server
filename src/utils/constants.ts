export interface BotConfig {
  name: string;
  isStarter: boolean;
  flow: number[] | null;
}

const hour = 60 * 60 * 1000;
export const BOTMAP: Record<number, BotConfig> = {
  1: { name: "Akron Water", isStarter: true, flow: [1, 4] },
  2: { name: "Summit Foreclosure", isStarter: true, flow: [2, 4] },
  3: { name: "Summit Notice of Default", isStarter: true, flow: [3, 4] },
  4: { name: "summitoh_v5", isStarter: false, flow: null },
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
