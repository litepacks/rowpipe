import { createHash } from "node:crypto";
import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";

export type MaskingRule = "email" | "phone" | "card" | "ip" | "name" | "hash" | "redact";

export interface MaskOptions {
  email?: string | string[];
  phone?: string | string[];
  card?: string | string[];
  ip?: string | string[];
  name?: string | string[];
  hash?: string | string[];
  redact?: string | string[];
  salt?: string;
  batchSize?: number;
}

export function maskEmail(val: unknown): string | unknown {
  if (val === null || val === undefined) return val;
  const str = String(val);
  const atIdx = str.indexOf("@");
  if (atIdx <= 0) return "[REDACTED_EMAIL]";

  const localPart = str.slice(0, atIdx);
  const domainPart = str.slice(atIdx);
  const maskedLocal = localPart.length <= 1 ? "*" : `${localPart[0]}***`;
  return `${maskedLocal}${domainPart}`;
}

export function maskPhone(val: unknown): string | unknown {
  if (val === null || val === undefined) return val;
  const str = String(val).trim();
  const digits = str.replace(/\D/g, "");
  if (digits.length < 4) return "***-****";
  const lastFour = digits.slice(-4);
  return `***-***-${lastFour}`;
}

export function maskCard(val: unknown): string | unknown {
  if (val === null || val === undefined) return val;
  const str = String(val).replace(/\s|-/g, "");
  if (str.length < 4) return "**** **** **** ****";
  const lastFour = str.slice(-4);
  return `**** **** **** ${lastFour}`;
}

export function maskIp(val: unknown): string | unknown {
  if (val === null || val === undefined) return val;
  const str = String(val).trim();
  const parts = str.split(".");
  if (parts.length === 4) {
    return `${parts[0]}.${parts[1]}.***.***`;
  }
  return "***.***.***.***";
}

export function maskName(val: unknown): string | unknown {
  if (val === null || val === undefined) return val;
  const str = String(val).trim();
  return str
    .split(/\s+/)
    .map((word) => (word.length > 0 ? `${word[0]}***` : ""))
    .join(" ");
}

export function maskHash(val: unknown, salt = ""): string | unknown {
  if (val === null || val === undefined) return val;
  const hash = createHash("sha256")
    .update(salt + ":" + String(val))
    .digest("hex");
  return hash.slice(0, 16);
}

export function maskRedact(val: unknown): string | unknown {
  if (val === null || val === undefined) return val;
  return "[REDACTED]";
}

function normalizeColList(cols?: string | string[]): string[] {
  if (!cols) return [];
  if (Array.isArray(cols)) return cols;
  return cols.split(",").map((s) => s.trim()).filter(Boolean);
}

/**
 * Creates a streaming transform that redacts / masks sensitive PII columns.
 */
export function maskTransform(options: MaskOptions): TransformFunction {
  const columnRules = new Map<string, MaskingRule>();

  for (const col of normalizeColList(options.email)) columnRules.set(col, "email");
  for (const col of normalizeColList(options.phone)) columnRules.set(col, "phone");
  for (const col of normalizeColList(options.card)) columnRules.set(col, "card");
  for (const col of normalizeColList(options.ip)) columnRules.set(col, "ip");
  for (const col of normalizeColList(options.name)) columnRules.set(col, "name");
  for (const col of normalizeColList(options.hash)) columnRules.set(col, "hash");
  for (const col of normalizeColList(options.redact)) columnRules.set(col, "redact");

  const salt = options.salt || "rowpipe_default_salt";
  const batchSize = options.batchSize || 1000;

  return async function* (stream: DataStream): DataStream {
    for await (const batch of stream) {
      const outRows: Row[] = new Array(batch.rows.length);

      for (let i = 0; i < batch.rows.length; i++) {
        const row = batch.rows[i]!;
        const newRow: Row = { ...row };

        for (const [col, rule] of columnRules) {
          if (col in newRow) {
            const raw = newRow[col];
            switch (rule) {
              case "email":
                newRow[col] = maskEmail(raw);
                break;
              case "phone":
                newRow[col] = maskPhone(raw);
                break;
              case "card":
                newRow[col] = maskCard(raw);
                break;
              case "ip":
                newRow[col] = maskIp(raw);
                break;
              case "name":
                newRow[col] = maskName(raw);
                break;
              case "hash":
                newRow[col] = maskHash(raw, salt);
                break;
              case "redact":
                newRow[col] = maskRedact(raw);
                break;
            }
          }
        }

        outRows[i] = newRow;
      }

      yield { rows: outRows, offset: batch.offset };
    }
  };
}
