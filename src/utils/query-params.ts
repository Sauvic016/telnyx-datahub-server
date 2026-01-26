export const isValidQueryParam = (value: any): boolean => {
  if (!value) return false;
  if (typeof value !== "string") return false;
  const trimmed = value.trim().toLowerCase();
  return trimmed !== "" && trimmed !== "null" && trimmed !== "undefined";
};

export const getValidParam = <T = string>(value: any): T | undefined => {
  return isValidQueryParam(value) ? (value as T) : undefined;
};
