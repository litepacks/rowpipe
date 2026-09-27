import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { join } from "node:path";
import {
  createPipeline,
  createReader,
  createWriter,
  inferReaderFormatFromPath,
  inferWriterFormatFromPath,
  XMLReader,
  XMLWriter,
  coerceXmlPrimitive,
  escapeXml,
  filterRows,
  mapRows,
} from "../src/index.js";
import { convertCommand } from "../src/cli/commands/convert.js";
import { inspectCommand } from "../src/cli/commands/inspect.js";
import { schemaCommand } from "../src/cli/commands/schema.js";
import { statsCommand } from "../src/cli/commands/stats.js";

const TEST_DIR = join(process.cwd(), "scratch_test_xml");

describe("XML Format Support", () => {
  beforeAll(() => {
    if (!existsSync(TEST_DIR)) {
      mkdirSync(TEST_DIR, { recursive: true });
    }
  });

  afterAll(() => {
    if (existsSync(TEST_DIR)) {
      rmSync(TEST_DIR, { recursive: true, force: true });
    }
  });

  it("should infer 'xml' format from file extensions", () => {
    expect(inferReaderFormatFromPath("data.xml")).toBe("xml");
    expect(inferReaderFormatFromPath("data.xml.gz")).toBe("xml");
    expect(inferWriterFormatFromPath("output.xml")).toBe("xml");
    expect(inferWriterFormatFromPath("output.xml.gz")).toBe("xml");
  });

  it("should coerce XML text primitives cleanly", () => {
    expect(coerceXmlPrimitive("123")).toBe(123);
    expect(coerceXmlPrimitive("99.90")).toBe(99.9);
    expect(coerceXmlPrimitive("true")).toBe(true);
    expect(coerceXmlPrimitive("false")).toBe(false);
    expect(coerceXmlPrimitive("null")).toBeNull();
    expect(coerceXmlPrimitive("01234")).toBe("01234"); // Preserve leading zero zip code
    expect(coerceXmlPrimitive("Keyboard")).toBe("Keyboard");
  });

  it("should escape XML special characters on output", () => {
    expect(escapeXml('Foo & Bar <Baz> "Qux" \'Test\'')).toBe(
      "Foo &amp; Bar &lt;Baz&gt; &quot;Qux&quot; &apos;Test&apos;"
    );
  });

  it("should parse streaming XML elements into records with default path (depth 2)", async () => {
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<products>
  <product>
    <id>1</id>
    <name>Keyboard</name>
    <price>99.90</price>
  </product>
  <product>
    <id>2</id>
    <name>Mouse</name>
    <price>49.90</price>
  </product>
</products>`;
    const xmlPath = join(TEST_DIR, "products_default.xml");
    writeFileSync(xmlPath, xml, "utf8");

    const reader = new XMLReader(xmlPath);
    const rows: any[] = [];
    for await (const batch of reader.read()) {
      rows.push(...batch.rows);
    }

    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({ id: 1, name: "Keyboard", price: 99.9 });
    expect(rows[1]).toEqual({ id: 2, name: "Mouse", price: 49.9 });
  });

  it("should support custom record paths with dot notation and xpath syntax", async () => {
    const xml = `<catalog>
  <metadata><generated>2025-01-01</generated></metadata>
  <items>
    <item>
      <sku>SKU-A</sku>
      <qty>10</qty>
    </item>
    <item>
      <sku>SKU-B</sku>
      <qty>25</qty>
    </item>
  </items>
</catalog>`;
    const xmlPath = join(TEST_DIR, "catalog.xml");
    writeFileSync(xmlPath, xml, "utf8");

    // Dot notation: items.item
    const reader1 = new XMLReader(xmlPath, { path: "items.item" });
    const rows1: any[] = [];
    for await (const b of reader1.read()) rows1.push(...b.rows);
    expect(rows1).toHaveLength(2);
    expect(rows1[0].sku).toBe("SKU-A");
    expect(rows1[0].qty).toBe(10);

    // XPath notation: /catalog/items/item
    const reader2 = new XMLReader(xmlPath, { path: "/catalog/items/item" });
    const rows2: any[] = [];
    for await (const b of reader2.read()) rows2.push(...b.rows);
    expect(rows2).toHaveLength(2);
    expect(rows2[1].sku).toBe("SKU-B");
    expect(rows2[1].qty).toBe(25);

    // Leaf tag notation: item
    const reader3 = new XMLReader(xmlPath, { path: "item" });
    const rows3: any[] = [];
    for await (const b of reader3.read()) rows3.push(...b.rows);
    expect(rows3).toHaveLength(2);
  });

  it("should support attributes and configurable attribute prefix", async () => {
    const xml = `<store>
  <product id="123" active="true">
    <name>Mechanical Keyboard</name>
  </product>
  <product id="456" active="false">
    <name>Ergonomic Mouse</name>
  </product>
</store>`;
    const xmlPath = join(TEST_DIR, "attributes.xml");
    writeFileSync(xmlPath, xml, "utf8");

    // Default prefix "@"
    const reader = new XMLReader(xmlPath);
    const rows: any[] = [];
    for await (const b of reader.read()) rows.push(...b.rows);

    expect(rows).toHaveLength(2);
    expect(rows[0]["@id"]).toBe(123);
    expect(rows[0]["@active"]).toBe(true);
    expect(rows[0].name).toBe("Mechanical Keyboard");

    // Custom prefix "_"
    const readerCustom = new XMLReader(xmlPath, { attrPrefix: "_" });
    const rowsCustom: any[] = [];
    for await (const b of readerCustom.read()) rowsCustom.push(...b.rows);

    expect(rowsCustom[0]["_id"]).toBe(123);
    expect(rowsCustom[0]["_active"]).toBe(true);
  });

  it("should handle nested structures and optional flattening (--flatten)", async () => {
    const xml = `<store>
  <product>
    <name>Keyboard</name>
    <manufacturer>
      <name>Acme</name>
      <country>TR</country>
    </manufacturer>
  </product>
</store>`;
    const xmlPath = join(TEST_DIR, "nested.xml");
    writeFileSync(xmlPath, xml, "utf8");

    // Preserved nested
    const readerNested = new XMLReader(xmlPath);
    const rowsNested: any[] = [];
    for await (const b of readerNested.read()) rowsNested.push(...b.rows);

    expect(rowsNested).toHaveLength(1);
    expect(rowsNested[0].name).toBe("Keyboard");
    expect(rowsNested[0].manufacturer).toEqual({ name: "Acme", country: "TR" });

    // Flattened
    const readerFlat = new XMLReader(xmlPath, { flatten: true });
    const rowsFlat: any[] = [];
    for await (const b of readerFlat.read()) rowsFlat.push(...b.rows);

    expect(rowsFlat).toHaveLength(1);
    expect(rowsFlat[0]["name"]).toBe("Keyboard");
    expect(rowsFlat[0]["manufacturer.name"]).toBe("Acme");
    expect(rowsFlat[0]["manufacturer.country"]).toBe("TR");
  });

  it("should handle repeated child elements, CDATA, empty elements, and escaped entities", async () => {
    const xml = `<articles>
  <article>
    <id>1</id>
    <title>Hello &amp; Welcome</title>
    <tag>tech</tag>
    <tag>news</tag>
    <tag>ai</tag>
    <emptyTag/>
    <content><![CDATA[Some <b>HTML</b> content]]></content>
  </article>
</articles>`;
    const xmlPath = join(TEST_DIR, "rich.xml");
    writeFileSync(xmlPath, xml, "utf8");

    const reader = new XMLReader(xmlPath);
    const rows: any[] = [];
    for await (const b of reader.read()) rows.push(...b.rows);

    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBe(1);
    expect(rows[0].title).toBe("Hello & Welcome");
    expect(rows[0].tag).toEqual(["tech", "news", "ai"]);
    expect(rows[0].emptyTag).toBeNull();
    expect(rows[0].content).toBe("Some <b>HTML</b> content");
  });

  it("should support XML namespaces seamlessly", async () => {
    const xml = `<?xml version="1.0"?>
<feed xmlns:g="http://base.google.com/ns/1.0">
  <entry>
    <g:id>item_99</g:id>
    <g:price>19.99 USD</g:price>
    <g:title>Test Widget</g:title>
  </entry>
</feed>`;
    const xmlPath = join(TEST_DIR, "namespaces.xml");
    writeFileSync(xmlPath, xml, "utf8");

    const reader = new XMLReader(xmlPath, { path: "entry" });
    const rows: any[] = [];
    for await (const b of reader.read()) rows.push(...b.rows);

    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBe("item_99");
    expect(rows[0].price).toBe("19.99 USD");
    expect(rows[0].title).toBe("Test Widget");
  });

  it("should write incremental XML with customizable root and row tags", async () => {
    const outPath = join(TEST_DIR, "custom_write.xml");
    const writer = new XMLWriter(outPath, {
      xmlRoot: "catalog",
      xmlRow: "item",
    });

    const rows = [
      { "@id": 1, "@type": "device", name: "Laptop", price: 1200 },
      { "@id": 2, "@type": "peripheral", name: "Monitor", price: 300 },
    ];

    await writer.write(rows);
    await writer.close();

    const xmlContent = readFileSync(outPath, "utf8");
    expect(xmlContent).toContain("<catalog>");
    expect(xmlContent).toContain('item id="1" type="device"');
    expect(xmlContent).toContain("<name>Laptop</name>");
    expect(xmlContent).toContain("<price>1200</price>");
    expect(xmlContent).toContain("</catalog>");
  });

  it("should round-trip CSV -> XML -> CSV", async () => {
    const csvContent = "id,name,price\n101,Keyboard,99.9\n102,Mouse,49.5\n";
    const csvIn = join(TEST_DIR, "roundtrip_in.csv");
    const xmlMid = join(TEST_DIR, "roundtrip.xml");
    const csvOut = join(TEST_DIR, "roundtrip_out.csv");

    writeFileSync(csvIn, csvContent, "utf8");

    // CSV -> XML
    await convertCommand(csvIn, xmlMid, { quiet: true, xmlRoot: "products", xmlRow: "product" });
    expect(existsSync(xmlMid)).toBe(true);

    // XML -> CSV
    await convertCommand(xmlMid, csvOut, { quiet: true, path: "products.product" });
    expect(existsSync(csvOut)).toBe(true);

    const outContent = readFileSync(csvOut, "utf8");
    expect(outContent).toContain("Keyboard");
    expect(outContent).toContain("Mouse");
    expect(outContent).toContain("99.9");
  });

  it("should convert XML to Parquet cleanly", async () => {
    const xml = `<data>
  <row><id>1</id><city>Istanbul</city><country>Turkey</country></row>
  <row><id>2</id><city>Tokyo</city><country>Japan</country></row>
</data>`;
    const xmlPath = join(TEST_DIR, "cities.xml");
    const parquetPath = join(TEST_DIR, "cities.parquet");

    writeFileSync(xmlPath, xml, "utf8");

    await convertCommand(xmlPath, parquetPath, { quiet: true });
    expect(existsSync(parquetPath)).toBe(true);

    const reader = createReader(parquetPath);
    const rows: any[] = [];
    for await (const b of reader.read()) rows.push(...b.rows);

    expect(rows).toHaveLength(2);
    expect(rows[0].city).toBe("Istanbul");
    expect(rows[1].city).toBe("Tokyo");
  });

  it("should handle compressed XML files (.xml.gz)", async () => {
    const xml = `<records>
  <record><id>1</id><val>A</val></record>
  <record><id>2</id><val>B</val></record>
</records>`;
    const gzPath = join(TEST_DIR, "records.xml.gz");
    writeFileSync(gzPath, gzipSync(Buffer.from(xml, "utf8")));

    const reader = new XMLReader(gzPath);
    const rows: any[] = [];
    for await (const b of reader.read()) rows.push(...b.rows);

    expect(rows).toHaveLength(2);
    expect(rows[0].val).toBe("A");
    expect(rows[1].val).toBe("B");
  });

  it("should support pipelines with transforms (filter, map) on XML data", async () => {
    const xmlPath = join(TEST_DIR, "products_default.xml");
    const outJsonl = join(TEST_DIR, "filtered.jsonl");

    await createPipeline(createReader(xmlPath))
      .pipe(filterRows("price > 50"))
      .pipe(mapRows({ discountedPrice: "price * 0.9" }))
      .to(createWriter(outJsonl));

    const results = readFileSync(outJsonl, "utf8")
      .trim()
      .split("\n")
      .map((l) => JSON.parse(l));

    expect(results).toHaveLength(1);
    expect(results[0].name).toBe("Keyboard");
    expect(results[0].discountedPrice).toBeCloseTo(89.91);
  });

  it("should run CLI commands (inspect, schema, stats, convert) on XML files", async () => {
    const xmlPath = join(TEST_DIR, "products_default.xml");

    // CLI inspect
    await expect(inspectCommand(xmlPath, { json: true })).resolves.not.toThrow();

    // CLI schema
    await expect(schemaCommand(xmlPath, { json: true })).resolves.not.toThrow();

    // CLI stats
    await expect(statsCommand(xmlPath, { json: true })).resolves.not.toThrow();

    // CLI convert with --path
    const outCsv = join(TEST_DIR, "cli_products.csv");
    await expect(
      convertCommand(xmlPath, outCsv, { quiet: true, path: "products.product" })
    ).resolves.not.toThrow();
    expect(existsSync(outCsv)).toBe(true);
  });
});
