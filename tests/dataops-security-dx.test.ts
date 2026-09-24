import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { promises as fs } from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import {
  SyntheticDataGenerator,
  maskTransform,
  maskEmail,
  maskPhone,
  maskCard,
  maskIp,
  maskName,
  maskHash,
  runDataTests,
  startApiServer,
  generateHtmlReport,
  HttpReader,
  type DataBatch,
  type Row,
} from "../src/index.js";

const execAsync = promisify(exec);

async function collectStream(stream: AsyncIterable<DataBatch>): Promise<Row[]> {
  const rows: Row[] = [];
  for await (const batch of stream) {
    rows.push(...batch.rows);
  }
  return rows;
}

async function* createMockStream(rows: Row[]): AsyncIterable<DataBatch> {
  yield { rows, offset: 0 };
}

describe("DataOps, Security & DX Suite (Generate, Mask, Test, Serve, Report)", () => {
  let tempDir: string;
  const binPath = path.resolve(__dirname, "../dist/cli/index.js");

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "rowpipe-dataops-test-"));
  });

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true });
  });

  describe("Synthetic Data Generation", () => {
    it("should generate streaming rows with typed columns and sequence numbers", async () => {
      const generator = new SyntheticDataGenerator(
        "id:seq,name:name,email:email,age:int(20,50),score:float(1.0,10.0,2),status:choice(active,inactive)",
        25,
        10
      );

      const rows = await collectStream(generator.read());
      expect(rows.length).toBe(25);

      expect(rows[0]!["id"]).toBe(1);
      expect(rows[24]!["id"]).toBe(25);

      expect(typeof rows[0]!["name"]).toBe("string");
      expect(typeof rows[0]!["email"]).toBe("string");
      expect(String(rows[0]!["email"])).toContain("@");

      const age = Number(rows[0]!["age"]);
      expect(age).toBeGreaterThanOrEqual(20);
      expect(age).toBeLessThanOrEqual(50);

      const score = Number(rows[0]!["score"]);
      expect(score).toBeGreaterThanOrEqual(1.0);
      expect(score).toBeLessThanOrEqual(10.0);

      expect(["active", "inactive"]).toContain(rows[0]!["status"]);
    });
  });

  describe("PII Masking & Redaction", () => {
    it("should mask individual PII types accurately", () => {
      expect(maskEmail("john.doe@company.com")).toBe("j***@company.com");
      expect(maskPhone("+1 (555) 123-4567")).toBe("***-***-4567");
      expect(maskCard("4532-1234-5678-9012")).toBe("**** **** **** 9012");
      expect(maskIp("192.168.1.100")).toBe("192.168.***.***");
      expect(maskName("John Doe")).toBe("J*** D***");
      expect(maskHash("secret_id", "my_salt")).toHaveLength(16);
    });

    it("should transform streaming rows with column masking rules", async () => {
      const mockData: Row[] = [
        {
          id: 1,
          email: "alice@corp.com",
          phone: "555-987-6543",
          card: "1234-5678-9876-5432",
          ip: "10.0.0.1",
          name: "Alice Smith",
        },
      ];

      const transform = maskTransform({
        email: "email",
        phone: "phone",
        card: "card",
        ip: "ip",
        name: "name",
      });

      const result = await collectStream(transform(createMockStream(mockData)));
      expect(result.length).toBe(1);
      expect(result[0]!["email"]).toBe("a***@corp.com");
      expect(result[0]!["phone"]).toBe("***-***-6543");
      expect(result[0]!["card"]).toBe("**** **** **** 5432");
      expect(result[0]!["ip"]).toBe("10.0.***.***");
      expect(result[0]!["name"]).toBe("A*** S***");
    });
  });

  describe("Data Quality Testing & Assertions", () => {
    const data: Row[] = [
      { id: 1, amount: 100, email: "a@corp.com" },
      { id: 2, amount: 200, email: "b@corp.com" },
      { id: 3, amount: -50, email: null },
      { id: 3, amount: 300, email: "c@corp.com" },
    ];

    it("should identify assertion, not-null, and unique violations", async () => {
      const summary = await runDataTests(createMockStream(data), {
        assert: "amount > 0",
        notNull: "email",
        unique: "id",
      });

      expect(summary.allPassed).toBe(false);
      expect(summary.totalRows).toBe(4);
      expect(summary.rulesCount).toBe(3);

      const assertRule = summary.results.find((r) => r.rule.type === "assert");
      expect(assertRule!.passed).toBe(false);
      expect(assertRule!.violationsCount).toBe(1); // amount = -50

      const notNullRule = summary.results.find((r) => r.rule.type === "not_null");
      expect(notNullRule!.passed).toBe(false);
      expect(notNullRule!.violationsCount).toBe(1); // email = null

      const uniqueRule = summary.results.find((r) => r.rule.type === "unique");
      expect(uniqueRule!.passed).toBe(false);
      expect(uniqueRule!.violationsCount).toBe(1); // duplicate id = 3
    });

    it("should pass when all assertions hold", async () => {
      const cleanData: Row[] = [
        { id: 1, amount: 100, email: "a@corp.com" },
        { id: 2, amount: 200, email: "b@corp.com" },
      ];

      const summary = await runDataTests(createMockStream(cleanData), {
        assert: "amount > 0",
        notNull: "email",
        unique: "id",
        minRows: 2,
        maxRows: 10,
      });

      expect(summary.allPassed).toBe(true);
      expect(summary.failedRulesCount).toBe(0);
    });
  });

  describe("HTTP REST API Server", () => {
    class MockReader {
      async *read() {
        yield {
          rows: [
            { id: 1, name: "Alpha", score: 90 },
            { id: 2, name: "Beta", score: 80 },
            { id: 3, name: "Gamma", score: 95 },
          ],
          offset: 0,
        };
      }
    }

    it("should serve dataset via HTTP endpoints", async () => {
      const server = startApiServer({
        port: 0, // ephemeral port
        readerFactory: () => new MockReader() as any,
        filePath: "mock.csv",
      });

      await new Promise((resolve) => server.on("listening", resolve));
      const address = server.address() as any;
      const port = address.port;
      const baseUrl = `http://localhost:${port}`;

      try {
        // 1. GET / (Docs)
        const metaRes = await fetch(`${baseUrl}/`);
        const metaJson = await metaRes.json();
        expect(metaJson.service).toBe("Rowpipe Data API");

        // 2. GET /rows with limit & select
        const rowsRes = await fetch(`${baseUrl}/rows?limit=2&select=id,name`);
        const rowsJson = await rowsRes.json();
        expect(rowsJson.length).toBe(2);
        expect(rowsJson[0]).toEqual({ id: 1, name: "Alpha" });

        // 3. GET /rows as CSV
        const csvRes = await fetch(`${baseUrl}/rows?limit=1&format=csv`);
        const csvText = await csvRes.text();
        expect(csvText).toContain("id,name,score");

        // 4. GET /stats
        const statsRes = await fetch(`${baseUrl}/stats`);
        const statsJson = await statsRes.json();
        expect(statsJson.totalRows).toBe(3);
      } finally {
        await new Promise((resolve) => server.close(resolve));
      }
    });
  });

  describe("HTML Health Report Generator", () => {
    class MockReader {
      async *read() {
        yield {
          rows: [
            { id: 1, name: "Alpha", price: 10.5 },
            { id: 2, name: "Beta", price: 20.0 },
          ],
          offset: 0,
        };
      }
    }

    it("should generate comprehensive HTML report string", async () => {
      const html = await generateHtmlReport(new MockReader() as any, "catalog.csv");
      expect(html).toContain("<!DOCTYPE html>");
      expect(html).toContain("Rowpipe Health Report: catalog.csv");
      expect(html).toContain("Total Records");
      expect(html).toContain("price");
    });
  });

  describe("CLI Command Integration", () => {
    it("should execute rowpipe generate CLI command", async () => {
      const { stdout } = await execAsync(
        `node "${binPath}" generate "id:seq,name:name,score:int(10,50)" --rows 5 --json`
      );

      const parsed = JSON.parse(stdout);
      expect(parsed.length).toBe(5);
      expect(parsed[0].id).toBe(1);
      expect(parsed[4].id).toBe(5);
    });

    it("should execute rowpipe mask CLI command", async () => {
      const csvPath = path.join(tempDir, "users.csv");
      await fs.writeFile(
        csvPath,
        "id,email,phone\n1,john@corp.com,555-123-4567\n"
      );

      const { stdout } = await execAsync(
        `node "${binPath}" mask "${csvPath}" --email email --phone phone --json`
      );

      const parsed = JSON.parse(stdout);
      expect(parsed[0].email).toBe("j***@corp.com");
      expect(parsed[0].phone).toBe("***-***-4567");
    });

    it("should execute rowpipe test CLI command", async () => {
      const csvPath = path.join(tempDir, "items.csv");
      await fs.writeFile(
        csvPath,
        "id,price\n1,10\n2,20\n"
      );

      const { stdout } = await execAsync(
        `node "${binPath}" test "${csvPath}" --assert "price > 0" --unique id --json`
      );

      const parsed = JSON.parse(stdout);
      expect(parsed.allPassed).toBe(true);
      expect(parsed.totalRows).toBe(2);
    });

    it("should execute rowpipe report CLI command", async () => {
      const csvPath = path.join(tempDir, "data.csv");
      const reportPath = path.join(tempDir, "health_report.html");
      await fs.writeFile(csvPath, "id,val\n1,10\n2,20\n");

      await execAsync(
        `node "${binPath}" report "${csvPath}" --output "${reportPath}"`
      );

      const exists = await fs.stat(reportPath);
      expect(exists.isFile()).toBe(true);
      const content = await fs.readFile(reportPath, "utf-8");
      expect(content).toContain("Rowpipe Health Report");
    });
  });

  describe("Remote HTTP Streaming (HttpReader & fetch)", () => {
    it("should fetch and stream JSON array from remote server with pagination", async () => {
      let page1Requested = false;
      let page2Requested = false;

      const server = startApiServer({
        port: 0,
        readerFactory: () => ({
          read: async function* () {
            yield {
              rows: [
                { id: 101, name: "Remote Item 1" },
                { id: 102, name: "Remote Item 2" },
              ],
              offset: 0,
            };
          },
        }),
        filePath: "remote.csv",
      });

      await new Promise((resolve) => server.on("listening", resolve));
      const port = (server.address() as any).port;
      const url = `http://localhost:${port}/rows`;

      try {
        const reader = new HttpReader(url, {
          batchSize: 10,
        });

        const rows = await collectStream(reader.read());
        expect(rows.length).toBe(2);
        expect(rows[0]!["id"]).toBe(101);
      } finally {
        await new Promise((resolve) => server.close(resolve));
      }
    });

    it("should execute rowpipe fetch CLI command", async () => {
      const server = startApiServer({
        port: 0,
        readerFactory: () => ({
          read: async function* () {
            yield {
              rows: [{ id: 999, label: "Fetched CLI Record" }],
              offset: 0,
            };
          },
        }),
        filePath: "remote.csv",
      });

      await new Promise((resolve) => server.on("listening", resolve));
      const port = (server.address() as any).port;
      const url = `http://localhost:${port}/rows`;

      try {
        const { stdout } = await execAsync(
          `node "${binPath}" fetch "${url}" --json`
        );
        const parsed = JSON.parse(stdout);
        expect(parsed.length).toBe(1);
        expect(parsed[0].id).toBe(999);
      } finally {
        await new Promise((resolve) => server.close(resolve));
      }
    });
  });
});
