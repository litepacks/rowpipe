import type { SemanticType } from "../core/types.js";

const EMAIL_REGEX = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
const URL_REGEX = /^https?:\/\/[^\s/$.?#].[^\s]*$/i;
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const IPV4_REGEX = /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
const IPV6_REGEX = /^(?:[A-F0-9]{1,4}:){7}[A-F0-9]{1,4}$/i;
const PHONE_REGEX = /^\+?[0-9\s\-().]{7,20}$/;

const ISO_COUNTRY_CODES = new Set([
  "US", "GB", "DE", "FR", "TR", "CA", "AU", "JP", "CN", "IN", "BR", "IT", "ES", "NL", "SE", "CH", "PL", "MX", "KR", "RU", "ZA", "EG", "SA", "AE", "SG", "NZ", "IE", "NO", "DK", "FI", "PT", "GR", "AT", "BE", "IL", "AR", "CL", "CO", "PE", "TH", "MY", "ID", "PH", "VN", "PK", "NG", "KE", "ZA"
]);

const ISO_CURRENCIES = new Set([
  "USD", "EUR", "GBP", "TRY", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "BRL", "RUB", "KRW", "SEK", "NOK", "MXN", "SGD", "NZD", "HKD", "PLN", "ZAR", "AED", "SAR", "THB", "IDR"
]);

/**
 * Tests if a string value matches a specific semantic pattern.
 */
export function detectSemanticType(value: unknown): SemanticType | null {
  if (value === null || value === undefined) return null;
  const str = String(value).trim();
  if (str.length === 0) return null;

  if (UUID_REGEX.test(str)) return "uuid";
  if (EMAIL_REGEX.test(str)) return "email";
  if (URL_REGEX.test(str)) return "url";
  if (IPV4_REGEX.test(str)) return "ipv4";
  if (IPV6_REGEX.test(str)) return "ipv6";
  if (str.length >= 2 && str.length <= 3 && ISO_COUNTRY_CODES.has(str.toUpperCase())) return "country-code";
  if (str.length === 3 && ISO_CURRENCIES.has(str.toUpperCase())) return "currency";
  if (PHONE_REGEX.test(str) && /\d{4,}/.test(str) && (str.startsWith("+") || str.includes("-") || str.includes("("))) return "phone";

  return null;
}
