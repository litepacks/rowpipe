import type { Row, DataBatch } from "../core/types.js";

export interface TechnicalOptions {
  priceCol?: string;
  volumeCol?: string;
  highCol?: string;
  lowCol?: string;
  closeCol?: string;
  indicators?: string | string[];
}

export interface ParsedIndicator {
  type: "sma" | "ema" | "rsi" | "macd" | "bollinger" | "vwap" | "atr";
  name: string;
  period: number;
  param2?: number;
  param3?: number;
}

export function parseIndicatorSpecs(specs?: string | string[]): ParsedIndicator[] {
  if (!specs) {
    // Default indicators
    return [
      { type: "sma", name: "sma_20", period: 20 },
      { type: "rsi", name: "rsi_14", period: 14 },
      { type: "bollinger", name: "bb_20_2", period: 20, param2: 2 },
    ];
  }

  const rawStr = Array.isArray(specs) ? specs.join(",") : specs;
  const list: string[] = [];
  let depth = 0;
  let current = "";
  for (let i = 0; i < rawStr.length; i++) {
    const char = rawStr[i]!;
    if (char === "(") depth++;
    else if (char === ")") depth--;
    else if ((char === "," || char === ";") && depth === 0) {
      if (current.trim()) list.push(current.trim());
      current = "";
      continue;
    }
    current += char;
  }
  if (current.trim()) list.push(current.trim());

  const results: ParsedIndicator[] = [];

  for (const raw of list) {
    const s = raw.trim().toLowerCase();
    if (!s) continue;

    const match = s.match(/^([a-z_]+)(?:\((.*)\))?$/);
    if (!match) continue;

    const [, typeStr, argStr] = match;
    const args = argStr ? argStr.split(/[,:]/).map((a) => parseFloat(a.trim())) : [];

    switch (typeStr) {
      case "sma": {
        const period = args[0] || 20;
        results.push({ type: "sma", name: `sma_${period}`, period });
        break;
      }
      case "ema": {
        const period = args[0] || 20;
        results.push({ type: "ema", name: `ema_${period}`, period });
        break;
      }
      case "rsi": {
        const period = args[0] || 14;
        results.push({ type: "rsi", name: `rsi_${period}`, period });
        break;
      }
      case "macd": {
        const fast = args[0] || 12;
        const slow = args[1] || 26;
        const signal = args[2] || 9;
        results.push({
          type: "macd",
          name: `macd_${fast}_${slow}_${signal}`,
          period: fast,
          param2: slow,
          param3: signal,
        });
        break;
      }
      case "bollinger":
      case "bb": {
        const period = args[0] || 20;
        const k = args[1] || 2;
        results.push({ type: "bollinger", name: `bb_${period}_${k}`, period, param2: k });
        break;
      }
      case "vwap": {
        results.push({ type: "vwap", name: "vwap", period: 0 });
        break;
      }
      case "atr": {
        const period = args[0] || 14;
        results.push({ type: "atr", name: `atr_${period}`, period });
        break;
      }
    }
  }

  return results.length > 0 ? results : parseIndicatorSpecs();
}

// Indicator Calculation State Trackers

class SMATracker {
  private buffer: number[] = [];
  private sum = 0;
  constructor(public period: number) {}

  update(val: number): number | null {
    this.buffer.push(val);
    this.sum += val;
    if (this.buffer.length > this.period) {
      this.sum -= this.buffer.shift()!;
    }
    if (this.buffer.length === this.period) {
      return Math.round((this.sum / this.period) * 10000) / 10000;
    }
    return null;
  }
}

class EMATracker {
  private ema: number | null = null;
  private alpha: number;
  private count = 0;
  private initialSum = 0;

  constructor(public period: number) {
    this.alpha = 2 / (period + 1);
  }

  update(val: number): number | null {
    this.count++;
    if (this.ema === null) {
      this.initialSum += val;
      if (this.count === this.period) {
        this.ema = this.initialSum / this.period;
        return Math.round(this.ema * 10000) / 10000;
      }
      return null;
    }
    this.ema = val * this.alpha + this.ema * (1 - this.alpha);
    return Math.round(this.ema * 10000) / 10000;
  }
}

class RSITracker {
  private prevPrice: number | null = null;
  private count = 0;
  private avgGain = 0;
  private avgLoss = 0;

  constructor(public period: number) {}

  update(price: number): number | null {
    if (this.prevPrice === null) {
      this.prevPrice = price;
      return null;
    }

    const change = price - this.prevPrice;
    this.prevPrice = price;
    const gain = change > 0 ? change : 0;
    const loss = change < 0 ? -change : 0;

    this.count++;

    if (this.count <= this.period) {
      this.avgGain += gain;
      this.avgLoss += loss;

      if (this.count === this.period) {
        this.avgGain /= this.period;
        this.avgLoss /= this.period;
        return this.computeRsi();
      }
      return null;
    }

    // Wilder's smoothing
    this.avgGain = (this.avgGain * (this.period - 1) + gain) / this.period;
    this.avgLoss = (this.avgLoss * (this.period - 1) + loss) / this.period;

    return this.computeRsi();
  }

  private computeRsi(): number {
    if (this.avgLoss === 0) {
      return 100.0;
    }
    const rs = this.avgGain / this.avgLoss;
    const rsi = 100 - 100 / (1 + rs);
    return Math.round(rsi * 100) / 100;
  }
}

class MACDTracker {
  private fastEMA: EMATracker;
  private slowEMA: EMATracker;
  private signalEMA: EMATracker;

  constructor(fast = 12, slow = 26, signal = 9) {
    this.fastEMA = new EMATracker(fast);
    this.slowEMA = new EMATracker(slow);
    this.signalEMA = new EMATracker(signal);
  }

  update(price: number): { macd: number | null; signal: number | null; hist: number | null } {
    const fast = this.fastEMA.update(price);
    const slow = this.slowEMA.update(price);

    if (fast !== null && slow !== null) {
      const macdVal = fast - slow;
      const signalVal = this.signalEMA.update(macdVal);
      const histVal = signalVal !== null ? macdVal - signalVal : null;
      return {
        macd: Math.round(macdVal * 10000) / 10000,
        signal: signalVal !== null ? Math.round(signalVal * 10000) / 10000 : null,
        hist: histVal !== null ? Math.round(histVal * 10000) / 10000 : null,
      };
    }
    return { macd: null, signal: null, hist: null };
  }
}

class BollingerTracker {
  private buffer: number[] = [];
  constructor(public period = 20, public k = 2) {}

  update(price: number): { middle: number | null; upper: number | null; lower: number | null; bandwidth: number | null } {
    this.buffer.push(price);
    if (this.buffer.length > this.period) {
      this.buffer.shift();
    }
    if (this.buffer.length === this.period) {
      const mean = this.buffer.reduce((a, b) => a + b, 0) / this.period;
      const variance = this.buffer.reduce((acc, v) => acc + Math.pow(v - mean, 2), 0) / this.period;
      const std = Math.sqrt(variance);
      const upper = mean + this.k * std;
      const lower = mean - this.k * std;
      const bandwidth = mean !== 0 ? ((upper - lower) / mean) * 100 : 0;
      return {
        middle: Math.round(mean * 10000) / 10000,
        upper: Math.round(upper * 10000) / 10000,
        lower: Math.round(lower * 10000) / 10000,
        bandwidth: Math.round(bandwidth * 100) / 100,
      };
    }
    return { middle: null, upper: null, lower: null, bandwidth: null };
  }
}

class VWAPTracker {
  private cumVolume = 0;
  private cumPriceVolume = 0;

  update(price: number, volume: number): number | null {
    if (volume <= 0) return this.cumVolume > 0 ? this.cumPriceVolume / this.cumVolume : price;
    this.cumVolume += volume;
    this.cumPriceVolume += price * volume;
    return Math.round((this.cumPriceVolume / this.cumVolume) * 10000) / 10000;
  }
}

class ATRTracker {
  private prevClose: number | null = null;
  private count = 0;
  private atr = 0;

  constructor(public period = 14) {}

  update(high: number, low: number, close: number): number | null {
    let tr = high - low;
    if (this.prevClose !== null) {
      const tr1 = Math.abs(high - this.prevClose);
      const tr2 = Math.abs(low - this.prevClose);
      tr = Math.max(tr, tr1, tr2);
    }
    this.prevClose = close;
    this.count++;

    if (this.count <= this.period) {
      this.atr += tr;
      if (this.count === this.period) {
        this.atr /= this.period;
        return Math.round(this.atr * 10000) / 10000;
      }
      return null;
    }

    this.atr = (this.atr * (this.period - 1) + tr) / this.period;
    return Math.round(this.atr * 10000) / 10000;
  }
}

export async function* technicalTransform(
  batchIterator: AsyncIterable<DataBatch>,
  options: TechnicalOptions = {}
): AsyncIterable<DataBatch> {
  const parsedIndicators = parseIndicatorSpecs(options.indicators);

  let priceCol = options.priceCol || options.closeCol;
  let volumeCol = options.volumeCol;
  let highCol = options.highCol;
  let lowCol = options.lowCol;
  let closeCol = options.closeCol || options.priceCol;

  // Instantiate trackers
  const trackers = parsedIndicators.map((ind) => {
    switch (ind.type) {
      case "sma":
        return { ind, tracker: new SMATracker(ind.period) };
      case "ema":
        return { ind, tracker: new EMATracker(ind.period) };
      case "rsi":
        return { ind, tracker: new RSITracker(ind.period) };
      case "macd":
        return { ind, tracker: new MACDTracker(ind.period, ind.param2, ind.param3) };
      case "bollinger":
        return { ind, tracker: new BollingerTracker(ind.period, ind.param2) };
      case "vwap":
        return { ind, tracker: new VWAPTracker() };
      case "atr":
        return { ind, tracker: new ATRTracker(ind.period) };
    }
  });

  for await (const batch of batchIterator) {
    if (batch.rows.length === 0) {
      yield batch;
      continue;
    }

    // Auto-detect columns if not set
    if (!priceCol) {
      const firstRow = batch.rows[0]!;
      const keys = Object.keys(firstRow);
      priceCol =
        keys.find((k) => /(close|price|adj_close|rate|value)/i.test(k)) ??
        keys.find((k) => typeof firstRow[k] === "number") ??
        keys[0];
      closeCol = priceCol;
      volumeCol = keys.find((k) => /(volume|vol|qty|quantity)/i.test(k));
      highCol = keys.find((k) => /(high|max_price)/i.test(k));
      lowCol = keys.find((k) => /(low|min_price)/i.test(k));
    }

    const transformedRows: Row[] = [];

    for (const row of batch.rows) {
      const newRow: Row = { ...row };

      const rawPrice = row[priceCol!];
      const price = typeof rawPrice === "number" ? rawPrice : parseFloat(String(rawPrice)) || 0;

      const rawVol = volumeCol ? row[volumeCol] : 1;
      const volume = typeof rawVol === "number" ? rawVol : parseFloat(String(rawVol)) || 1;

      const rawHigh = highCol ? row[highCol] : price;
      const high = typeof rawHigh === "number" ? rawHigh : parseFloat(String(rawHigh)) || price;

      const rawLow = lowCol ? row[lowCol] : price;
      const low = typeof rawLow === "number" ? rawLow : parseFloat(String(rawLow)) || price;

      const rawClose = closeCol ? row[closeCol] : price;
      const close = typeof rawClose === "number" ? rawClose : parseFloat(String(rawClose)) || price;

      for (const { ind, tracker } of trackers) {
        if (tracker instanceof SMATracker) {
          newRow[ind.name] = tracker.update(price);
        } else if (tracker instanceof EMATracker) {
          newRow[ind.name] = tracker.update(price);
        } else if (tracker instanceof RSITracker) {
          newRow[ind.name] = tracker.update(price);
        } else if (tracker instanceof MACDTracker) {
          const res = tracker.update(price);
          newRow[`${ind.name}_line`] = res.macd;
          newRow[`${ind.name}_signal`] = res.signal;
          newRow[`${ind.name}_hist`] = res.hist;
        } else if (tracker instanceof BollingerTracker) {
          const res = tracker.update(price);
          newRow[`${ind.name}_upper`] = res.upper;
          newRow[`${ind.name}_mid`] = res.middle;
          newRow[`${ind.name}_lower`] = res.lower;
        } else if (tracker instanceof VWAPTracker) {
          newRow[ind.name] = tracker.update(price, volume);
        } else if (tracker instanceof ATRTracker) {
          newRow[ind.name] = tracker.update(high, low, close);
        }
      }

      transformedRows.push(newRow);
    }

    yield {
      ...batch,
      rows: transformedRows,
    };
  }
}
