import type { Row, DataBatch } from "../core/types.js";

export interface ClusterOptions {
  cols?: string | string[];
  k?: number; // Number of clusters (default: 3)
  seed?: number;
  summary?: boolean;
}

export interface CentroidInfo {
  cluster_id: number;
  size: number;
  percentage: number;
  coordinates: Record<string, number>;
  avg_distance: number;
}

export interface ClusterResult {
  k: number;
  columns: string[];
  total_samples: number;
  inertia: number;
  centroids: CentroidInfo[];
  formattedRows: Row[];
}

function euclideanDistanceSq(point: number[], centroid: number[]): number {
  let sum = 0;
  for (let i = 0; i < point.length; i++) {
    const diff = point[i]! - centroid[i]!;
    sum += diff * diff;
  }
  return sum;
}

export async function computeKMeans(
  batchIterator: AsyncIterable<DataBatch>,
  options: ClusterOptions = {}
): Promise<ClusterResult> {
  const k = Math.max(2, options.k ?? 3);

  let targetCols: string[] | undefined;
  if (options.cols) {
    targetCols = Array.isArray(options.cols)
      ? options.cols.flatMap((c) => c.split(",").map((s) => s.trim()))
      : options.cols.split(",").map((s) => s.trim());
  }

  let centroids: number[][] = [];
  let centroidCounts: number[] = [];
  let centroidDistSums: number[] = [];
  let totalSamples = 0;
  let totalInertia = 0;

  // Buffer initial samples for K-Means++ initialization
  const initBuffer: number[][] = [];
  const initSampleSize = Math.max(k * 10, 100);

  for await (const batch of batchIterator) {
    if (batch.rows.length === 0) continue;

    if (!targetCols) {
      const firstRow = batch.rows[0]!;
      targetCols = Object.keys(firstRow).filter((col) => {
        const val = firstRow[col];
        return typeof val === "number" || (!isNaN(Number(val)) && val !== null && val !== "" && typeof val !== "boolean");
      });
      if (targetCols.length === 0) {
        throw new Error("No numeric columns found for K-Means clustering.");
      }
    }

    const numericVectors: number[][] = [];
    for (const row of batch.rows) {
      const vec: number[] = [];
      let valid = true;
      for (const col of targetCols) {
        const raw = row[col];
        const num = typeof raw === "number" ? raw : parseFloat(String(raw));
        if (isNaN(num)) {
          valid = false;
          break;
        }
        vec.push(num);
      }
      if (valid) {
        numericVectors.push(vec);
      }
    }

    if (numericVectors.length === 0) continue;

    // Initialization phase (K-Means++)
    if (centroids.length < k) {
      for (const vec of numericVectors) {
        if (initBuffer.length < initSampleSize) {
          initBuffer.push(vec);
        }
      }

      if (initBuffer.length >= k) {
        // Pick first center randomly
        centroids.push([...initBuffer[0]!]);
        centroidCounts.push(1);
        centroidDistSums.push(0);

        // Pick remaining centers using D^2 weighting
        while (centroids.length < k) {
          const distances = initBuffer.map((pt) => {
            let minDistSq = Infinity;
            for (const c of centroids) {
              const dSq = euclideanDistanceSq(pt, c);
              if (dSq < minDistSq) minDistSq = dSq;
            }
            return minDistSq;
          });

          const totalDistSq = distances.reduce((a, b) => a + b, 0);
          let r = Math.random() * totalDistSq;
          let selectedIdx = 0;

          for (let i = 0; i < distances.length; i++) {
            r -= distances[i]!;
            if (r <= 0) {
              selectedIdx = i;
              break;
            }
          }
          centroids.push([...initBuffer[selectedIdx]!]);
          centroidCounts.push(1);
          centroidDistSums.push(0);
        }
      }
    }

    // Mini-Batch Gradient Updates
    if (centroids.length === k) {
      for (const vec of numericVectors) {
        totalSamples++;
        let bestCluster = 0;
        let bestDistSq = Infinity;

        for (let ci = 0; ci < k; ci++) {
          const dSq = euclideanDistanceSq(vec, centroids[ci]!);
          if (dSq < bestDistSq) {
            bestDistSq = dSq;
            bestCluster = ci;
          }
        }

        totalInertia += bestDistSq;
        const dist = Math.sqrt(bestDistSq);
        centroidDistSums[bestCluster] = (centroidDistSums[bestCluster] || 0) + dist;
        centroidCounts[bestCluster] = (centroidCounts[bestCluster] || 0) + 1;

        // Online Learning rate
        const count = centroidCounts[bestCluster]!;
        const lr = 1.0 / count;

        const center = centroids[bestCluster]!;
        for (let dim = 0; dim < vec.length; dim++) {
          center[dim] = center[dim]! * (1 - lr) + vec[dim]! * lr;
        }
      }
    }
  }

  if (centroids.length === 0 || !targetCols) {
    throw new Error("Insufficient numeric data to form clusters.");
  }

  const centroidResults: CentroidInfo[] = [];
  for (let ci = 0; ci < k; ci++) {
    const size = centroidCounts[ci] || 0;
    const pct = totalSamples > 0 ? (size / totalSamples) * 100 : 0;
    const avgDist = size > 0 ? centroidDistSums[ci]! / size : 0;

    const coords: Record<string, number> = {};
    for (let d = 0; d < targetCols.length; d++) {
      coords[targetCols[d]!] = Math.round((centroids[ci]![d] || 0) * 1000) / 1000;
    }

    centroidResults.push({
      cluster_id: ci + 1,
      size,
      percentage: Math.round(pct * 10) / 10,
      coordinates: coords,
      avg_distance: Math.round(avgDist * 1000) / 1000,
    });
  }

  // Format table rows
  const formattedRows: Row[] = centroidResults.map((c) => {
    const rowObj: Row = {
      cluster_id: `Cluster ${c.cluster_id}`,
      size: c.size,
      share: `${c.percentage.toFixed(1)}%`,
      avg_dist: c.avg_distance,
    };
    for (const [col, val] of Object.entries(c.coordinates)) {
      rowObj[col] = val;
    }
    return rowObj;
  });

  return {
    k,
    columns: targetCols,
    total_samples: totalSamples,
    inertia: Math.round(totalInertia * 100) / 100,
    centroids: centroidResults,
    formattedRows,
  };
}

export async function* clusterAssignTransform(
  batchIterator: AsyncIterable<DataBatch>,
  result: ClusterResult
): AsyncIterable<DataBatch> {
  const { k, columns, centroids } = result;
  const centroidVectors = centroids.map((c) => columns.map((col) => c.coordinates[col] || 0));

  for await (const batch of batchIterator) {
    if (batch.rows.length === 0) {
      yield batch;
      continue;
    }

    const updatedRows: Row[] = [];

    for (const row of batch.rows) {
      const newRow: Row = { ...row };
      const vec: number[] = [];
      let valid = true;

      for (const col of columns) {
        const raw = row[col];
        const num = typeof raw === "number" ? raw : parseFloat(String(raw));
        if (isNaN(num)) {
          valid = false;
          break;
        }
        vec.push(num);
      }

      if (valid) {
        let bestCluster = 0;
        let bestDistSq = Infinity;

        for (let ci = 0; ci < k; ci++) {
          const dSq = euclideanDistanceSq(vec, centroidVectors[ci]!);
          if (dSq < bestDistSq) {
            bestDistSq = dSq;
            bestCluster = ci;
          }
        }
        newRow["_cluster_id"] = bestCluster + 1;
        newRow["_cluster_distance"] = Math.round(Math.sqrt(bestDistSq) * 1000) / 1000;
      } else {
        newRow["_cluster_id"] = null;
        newRow["_cluster_distance"] = null;
      }

      updatedRows.push(newRow);
    }

    yield {
      ...batch,
      rows: updatedRows,
    };
  }
}
