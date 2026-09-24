import { randomUUID } from "node:crypto";
import type { DataBatch, DataStream, Row, TabularReader } from "../core/types.js";
import { InvalidArgumentError } from "../core/errors.js";

const FIRST_NAMES = [
  "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
  "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
  "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
  "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
  "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
  "Alex", "Ali", "Can", "Zeynep", "Elif", "Emre", "Deniz", "Burak", "Ayse", "Fatma"
];

const LAST_NAMES = [
  "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
  "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
  "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
  "Yilmaz", "Kaya", "Demir", "Celik", "Sahin", "Yildiz", "Ozturk", "Aydin", "Ozdemir", "Arslan"
];

const CITIES = [
  "New York", "London", "Tokyo", "Paris", "Berlin", "Istanbul", "San Francisco",
  "Sydney", "Singapore", "Toronto", "Amsterdam", "Madrid", "Rome", "Seoul", "Dubai"
];

const COMPANIES = [
  "Acme Corp", "Globex", "Initech", "Umbrella", "Soylent", "Hooli", "Pied Piper",
  "Wayne Enterprises", "Stark Industries", "Cyberdyne", "Massive Dynamic", "Wonka Inc"
];

const DOMAINS = ["example.com", "mail.com", "corp.net", "tech.io", "cloud.org", "web.dev"];

export type FieldGenerator = (index: number) => unknown;

function randomChoice<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomFloat(min: number, max: number, decimals = 2): number {
  const val = Math.random() * (max - min) + min;
  const factor = Math.pow(10, decimals);
  return Math.round(val * factor) / factor;
}

function randomDate(startMs = new Date("2022-01-01").getTime(), endMs = new Date("2026-01-01").getTime()): string {
  const date = new Date(startMs + Math.random() * (endMs - startMs));
  return date.toISOString().split("T")[0]!;
}

export function parseFieldGenerator(spec: string): FieldGenerator {
  const trimmed = spec.trim().toLowerCase();

  if (trimmed === "seq" || trimmed === "sequence") {
    return (i: number) => i + 1;
  }

  const seqMatch = trimmed.match(/^seq(?:uence)?\((\d+)(?:,\s*(\d+))?\)$/);
  if (seqMatch) {
    const start = parseInt(seqMatch[1]!, 10);
    const step = seqMatch[2] ? parseInt(seqMatch[2]!, 10) : 1;
    return (i: number) => start + i * step;
  }

  if (trimmed === "uuid" || trimmed === "guid") {
    return () => randomUUID();
  }

  if (trimmed === "name" || trimmed === "fullname") {
    return () => `${randomChoice(FIRST_NAMES)} ${randomChoice(LAST_NAMES)}`;
  }

  if (trimmed === "firstname" || trimmed === "first_name") {
    return () => randomChoice(FIRST_NAMES);
  }

  if (trimmed === "lastname" || trimmed === "last_name") {
    return () => randomChoice(LAST_NAMES);
  }

  if (trimmed === "email") {
    return () => {
      const first = randomChoice(FIRST_NAMES).toLowerCase();
      const last = randomChoice(LAST_NAMES).toLowerCase();
      const num = randomInt(1, 99);
      const domain = randomChoice(DOMAINS);
      return `${first}.${last}${num}@${domain}`;
    };
  }

  if (trimmed === "city") {
    return () => randomChoice(CITIES);
  }

  if (trimmed === "company") {
    return () => randomChoice(COMPANIES);
  }

  if (trimmed === "boolean" || trimmed === "bool") {
    return () => Math.random() > 0.5;
  }

  if (trimmed === "ipv4" || trimmed === "ip") {
    return () => `${randomInt(1, 255)}.${randomInt(0, 255)}.${randomInt(0, 255)}.${randomInt(1, 254)}`;
  }

  if (trimmed === "phone") {
    return () => `+1-${randomInt(200, 999)}-${randomInt(100, 999)}-${randomInt(1000, 9999)}`;
  }

  if (trimmed === "date") {
    return () => randomDate();
  }

  const dateMatch = trimmed.match(/^date\(([^,]+),\s*([^)]+)\)$/);
  if (dateMatch) {
    const start = new Date(dateMatch[1]!).getTime();
    const end = new Date(dateMatch[2]!).getTime();
    return () => randomDate(start, end);
  }

  const intMatch = trimmed.match(/^int(?:eger)?\((\d+),\s*(\d+)\)$/);
  if (intMatch) {
    const min = parseInt(intMatch[1]!, 10);
    const max = parseInt(intMatch[2]!, 10);
    return () => randomInt(min, max);
  }

  if (trimmed === "int" || trimmed === "integer") {
    return () => randomInt(0, 100);
  }

  const floatMatch = trimmed.match(/^(?:float|decimal)\(([\d.]+),\s*([\d.]+)(?:,\s*(\d+))?\)$/);
  if (floatMatch) {
    const min = parseFloat(floatMatch[1]!);
    const max = parseFloat(floatMatch[2]!);
    const dec = floatMatch[3] ? parseInt(floatMatch[3]!, 10) : 2;
    return () => randomFloat(min, max, dec);
  }

  if (trimmed === "float" || trimmed === "decimal" || trimmed === "number") {
    return () => randomFloat(0, 100, 2);
  }

  const choiceMatch = spec.match(/^choice\((.+)\)$/i);
  if (choiceMatch) {
    const choices = choiceMatch[1]!.split(",").map((s) => s.trim().replace(/^["']|["']$/g, ""));
    return () => randomChoice(choices);
  }

  // Constant fallback
  return () => spec;
}

export interface GeneratedColumn {
  name: string;
  generator: FieldGenerator;
}

function splitTopLevelCommas(str: string): string[] {
  const parts: string[] = [];
  let current = "";
  let depth = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str[i]!;
    if (char === "(") depth++;
    else if (char === ")") depth--;

    if (char === "," && depth === 0) {
      if (current.trim()) parts.push(current.trim());
      current = "";
    } else {
      current += char;
    }
  }
  if (current.trim()) parts.push(current.trim());
  return parts;
}

export function parseSchemaSpec(schemaStr: string): GeneratedColumn[] {
  const parts = splitTopLevelCommas(schemaStr);
  const cols: GeneratedColumn[] = [];

  for (const part of parts) {
    let colName = part;
    let spec = "string";

    if (part.includes(":")) {
      const idx = part.indexOf(":");
      colName = part.slice(0, idx).trim();
      spec = part.slice(idx + 1).trim();
    } else if (part.includes("=")) {
      const idx = part.indexOf("=");
      colName = part.slice(0, idx).trim();
      spec = part.slice(idx + 1).trim();
    }

    cols.push({
      name: colName,
      generator: parseFieldGenerator(spec),
    });
  }

  return cols;
}

export class SyntheticDataGenerator implements TabularReader {
  private columns: GeneratedColumn[];
  private totalRows: number;
  private batchSize: number;

  constructor(schemaSpec: string | GeneratedColumn[], totalRows: number, batchSize = 1000) {
    this.columns = typeof schemaSpec === "string" ? parseSchemaSpec(schemaSpec) : schemaSpec;
    if (this.columns.length === 0) {
      throw new InvalidArgumentError("Generate requires at least one column specification");
    }
    this.totalRows = totalRows;
    this.batchSize = batchSize;
  }

  async *read(): DataStream {
    let emitted = 0;

    while (emitted < this.totalRows) {
      const currentBatchCount = Math.min(this.batchSize, this.totalRows - emitted);
      const rows: Row[] = new Array(currentBatchCount);

      for (let i = 0; i < currentBatchCount; i++) {
        const rowIndex = emitted + i;
        const row: Row = {};
        for (const col of this.columns) {
          row[col.name] = col.generator(rowIndex);
        }
        rows[i] = row;
      }

      yield { rows, offset: emitted };
      emitted += currentBatchCount;
    }
  }
}
