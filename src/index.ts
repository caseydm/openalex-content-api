import { DynamoDBClient, QueryCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { AwsClient } from "aws4fetch";

export interface Env {
  PDFS: R2Bucket; // openalex-pdfs
  GROBID_XML: R2Bucket; // openalex-grobid-xml
  openalex_db: D1Database; // openalex-db
  AWS_ACCESS_KEY_ID: string;
  AWS_SECRET_ACCESS_KEY: string;
  AWS_REGION: string;
}

const PDF_S3_BACKUP_BUCKET = "openalex-harvested-pdfs";
const GROBID_S3_BACKUP_BUCKET = "openalex-harvested-grobid-xml";

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method !== "GET") return new Response("Method Not Allowed", { status: 405 });

    const url = new URL(req.url);
    // Expecting: /work/{id} with format query parameter
    const path = url.pathname.replace(/^\/+/, "");
    const parts = path.split("/"); // ["work", "{id}"]

    if (parts.length !== 2 || parts[0] !== "work") {
      return new Response("Not Found", { status: 404 });
    }

    // Get format from query parameter
    const format = url.searchParams.get("format");
    if (!format || (format !== "pdf" && format !== "parsed-pdf")) {
      return new Response("Bad Request: format query parameter must be 'pdf' or 'parsed-pdf'", { status: 400 });
    }

    const isGrobid = format === "parsed-pdf";

    const wantJson = (url.searchParams.get("json") || "").toLowerCase() === "true";
    const rawId = parts[1];

    // Check if it's a DOI (starts with "10.") or OpenAlex work ID
    const isDoi = rawId.startsWith("10.");
    let workId: string | null = null;
    let nativeId: string;
    let scheme: string;

    if (isDoi) {
      // For DOI, we'll skip OpenAlex lookup and go directly to DynamoDB
      nativeId = rawId;
      scheme = "doi";
    } else {
      // For work ID, normalize it first
      workId = normalizeWorkId(rawId);
      if (!workId) return jsonOrText(wantJson, 400, { error: "Invalid work id", work_id_input: rawId });
      // Initialize these for the work ID case - they'll be set after OpenAlex lookup
      nativeId = "";
      scheme = "";
    }

    // Check API key authentication
    const authResult = await checkApiKey(req, env);
    if (!authResult.valid) {
      const message = authResult.error || "Provide a valid API key in 'api_key' query parameter or 'Authorization' header";
      return jsonOrText(wantJson, 401, {
        error: "Invalid or missing API key",
        message: message
      });
    }
    if (!authResult.hasCreditCard) {
      return jsonOrText(wantJson, 403, {
        error: "Payment required",
        message: "A credit card on file is required to download files"
      });
    }

    let oa: any = null;
    let locId: string | null = null;

    if (!isDoi) {
      // 1) OpenAlex lookup (id + best_oa_location) - only for work IDs
      oa = await fetchOpenAlexWork(workId!, 300);
      if (!oa || !oa.best_oa_location) {
        return jsonOrText(wantJson, 404, {
          error: "No best_oa_location for this work",
          work_id: workId
        });
      }

      // 2) Extract native_id from best_oa_location.id (e.g., "doi:10.1063/..." or "pmh:...:id")
      locId = oa.best_oa_location.id;
      if (!locId) {
        return jsonOrText(wantJson, 502, {
          error: "Missing best_oa_location.id",
          work_id: workId
        });
      }
      const colon = locId.indexOf(":");
      if (colon <= 0 || colon === locId.length - 1) {
        return jsonOrText(wantJson, 502, {
          error: "Unrecognized best_oa_location.id format",
          work_id: workId,
          best_oa_location_id: locId
        });
      }
      scheme = locId.slice(0, colon);     // "doi", "pmh", etc.
      nativeId = locId.slice(colon + 1);  // stored in DynamoDB as native_id
    }

    // 3) DynamoDB query by native_id to get UUID
    const dynamoClient = new DynamoDBClient({
      region: env.AWS_REGION,
      credentials: {
        accessKeyId: env.AWS_ACCESS_KEY_ID,
        secretAccessKey: env.AWS_SECRET_ACCESS_KEY
      }
    });

    const tableName = isGrobid ? "grobid-xml" : "harvested-pdf";
    const fileExt = isGrobid ? ".xml.gz" : ".pdf";

    let uuid: string | undefined;
    let mappingFound = false;

    try {
      const q = new QueryCommand({
        TableName: tableName,
        IndexName: "by_native_id",
        KeyConditionExpression: "native_id = :nid",
        ExpressionAttributeValues: { ":nid": { S: nativeId } },
        Limit: 1
      });
      const result = await dynamoClient.send(q);
      if (result.Items && result.Items.length > 0) {
        const item = unmarshall(result.Items[0]);
        uuid = item.id as string | undefined;
        mappingFound = !!uuid;
      }
    } catch (err) {
      console.error("DynamoDB error:", err);
      return jsonOrText(wantJson, 500, {
        error: "Internal server error (DynamoDB)",
        work_id: workId || rawId,
        best_oa_location_id: locId,
        scheme,
        native_id: nativeId,
        table_name: tableName
      });
    }

    const r2Key = uuid ? `${uuid}${fileExt}` : undefined;

    // 4) JSON metadata path — check both R2 and S3 backup
    if (wantJson) {
      let in_r2 = false;
      let in_s3 = false;

      if (r2Key) {
        // Always check R2 (use correct bucket based on file type)
        const r2Bucket = isGrobid ? env.GROBID_XML : env.PDFS;
        const head = await r2Bucket.head?.(r2Key);
        if (head) in_r2 = true;
        else {
          const obj = await r2Bucket.get(r2Key);
          if (obj) in_r2 = true;
        }

        // Always check S3 (use correct backup bucket based on file type)
        const s3Bucket = isGrobid ? GROBID_S3_BACKUP_BUCKET : PDF_S3_BACKUP_BUCKET;
        in_s3 = await s3HeadObject(env, s3Bucket, r2Key);
      }

      // Self download URL (same endpoint) — will serve R2 or S3 based on availability
      url.searchParams.delete("json");
      const downloadUrl = (r2Key && (in_r2 || in_s3)) ? url.toString() : null;

      return json(200, {
        work_id: rawId,
        work_api: workId ? `https://api.openalex.org/works/${workId}?data-version=2` : null,
        best_oa_location_id: locId,
        native_id: nativeId,
        native_id_namespace: scheme,
        mapping_found_in_dynamodb: mappingFound,
        file_uuid: uuid || null,
        file_key: r2Key || null,
        in_r2,
        in_s3,
        download_url: downloadUrl
      });
    }

    // 5) Download path: stream from R2, else fallback to S3
    if (!uuid) {
      const fileType = isGrobid ? "Grobid XML" : "PDF";
      return new Response(`${fileType} mapping not found (native_id not in DB)`, { status: 404 });
    }

    const contentType = isGrobid ? "application/gzip" : "application/pdf";
    const safeName = `${(workId || rawId).replace(/[\/\\:*?"<>|]/g, "_")}${fileExt}`;
    const r2Bucket = isGrobid ? env.GROBID_XML : env.PDFS;
    const s3Bucket = isGrobid ? GROBID_S3_BACKUP_BUCKET : PDF_S3_BACKUP_BUCKET;

    // Try R2 first
    const r2Obj = await r2Bucket.get(r2Key!);
    if (r2Obj) {
      const headers = new Headers();
      r2Obj.writeHttpMetadata?.(headers);
      headers.set("Content-Type", contentType);
      headers.set("Content-Disposition", `attachment; filename="${safeName}"; filename*=UTF-8''${encodeURIComponent(safeName)}`);
      headers.set("Cache-Control", "private, no-store");
      return new Response(r2Obj.body, { status: 200, headers });
    }

    // Fallback to S3 backup — stream via signed GET
    const s3Resp = await s3GetObject(env, s3Bucket, r2Key!);
    if (s3Resp?.ok) {
      const headers = new Headers();
      headers.set("Content-Type", s3Resp.headers.get("content-type") || contentType);
      headers.set("Content-Disposition", `attachment; filename="${safeName}"; filename*=UTF-8''${encodeURIComponent(safeName)}`);
      headers.set("Cache-Control", "private, no-store");

      // (Optional) pass through Content-Length/ETag if you want
      const len = s3Resp.headers.get("content-length");
      if (len) headers.set("Content-Length", len);

      return new Response(s3Resp.body, { status: 200, headers });
    }

    const fileType = isGrobid ? "Grobid XML" : "PDF";
    return new Response(`${fileType} not found in R2 or S3 (${r2Key})`, { status: 404 });
  }
};

/** API Key Authentication **/
async function checkApiKey(req: Request, env: Env): Promise<{valid: boolean, hasCreditCard: boolean, error?: string}> {
  // Get API key from query parameter or Authorization header
  const url = new URL(req.url);
  let apiKey = url.searchParams.get("api_key");

  if (!apiKey) {
    const authHeader = req.headers.get("Authorization");
    if (authHeader && authHeader.startsWith("Bearer ")) {
      apiKey = authHeader.substring(7);
    }
  }

  if (!apiKey) {
    return { valid: false, hasCreditCard: false };
  }

  try {
    // First check if the API key exists at all
    const keyExists = await env.openalex_db
      .prepare("SELECT credit_card_on_file, expires_at FROM api_keys WHERE api_key = ?")
      .bind(apiKey)
      .first();

    if (!keyExists) {
      return { valid: false, hasCreditCard: false, error: "API key not found" };
    }

    // Check if the key has expired
    if (keyExists.expires_at) {
      const expiresAt = new Date(keyExists.expires_at as string);
      const now = new Date();
      if (expiresAt <= now) {
        return { valid: false, hasCreditCard: false, error: `API key expired on ${keyExists.expires_at}` };
      }
    }

    const hasCreditCard = Boolean(keyExists.credit_card_on_file);
    return { valid: true, hasCreditCard };

  } catch (error) {
    console.error("Error checking API key:", error);
    return { valid: false, hasCreditCard: false, error: "Database error" };
  }
}

/** Helpers **/

function normalizeWorkId(input: string): string | null {
  if (!input) return null;
  const trimmed = input.trim();
  // Handle W123 format
  if (/^W\d+$/i.test(trimmed)) return trimmed.toUpperCase();
  // Handle https://openalex.org/W123 format
  if (trimmed.startsWith("https://openalex.org/W")) {
    const match = trimmed.match(/https:\/\/openalex\.org\/(W\d+)/i);
    if (match) return match[1].toUpperCase();
  }
  try {
    const u = new URL(trimmed);
    const m = u.pathname.match(/\/W\d+$/i);
    if (m) return m[0].slice(1).toUpperCase();
  } catch {}
  return null;
}

async function fetchOpenAlexWork(workId: string, cacheTtlSeconds = 300): Promise<any | null> {
  const endpoint = `https://api.openalex.org/works/${encodeURIComponent(workId)}?data-version=2&select=id,best_oa_location`;
  const resp = await fetch(endpoint, { cf: { cacheTtl: cacheTtlSeconds, cacheEverything: true } });
  if (!resp.ok) return null;
  return await resp.json();
}

function json(status: number, data: unknown): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
  });
}

function jsonOrText(wantJson: boolean, status: number, obj: Record<string, unknown>): Response {
  if (wantJson) return json(status, obj);
  const msg = obj.error ? String(obj.error) : JSON.stringify(obj);
  return new Response(msg, { status });
}

/** S3 helpers (HEAD + GET) via SigV4 */
function s3Host(bucket: string, region: string): string {
  // Use virtual-hosted–style URLs
  return region === "us-east-1" ? `${bucket}.s3.amazonaws.com` : `${bucket}.s3.${region}.amazonaws.com`;
}

async function s3HeadObject(env: Env, bucket: string, key: string): Promise<boolean> {
  const host = s3Host(bucket, env.AWS_REGION);
  const url = `https://${host}/${encodeURIComponent(key)}`;
  const aws = new AwsClient({
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    region: env.AWS_REGION,
    service: "s3"
  });
  const resp = await aws.fetch(url, { method: "HEAD" });
  if (resp.status === 200) return true;
  if (resp.status === 404 || resp.status === 403) return false; // 403 often means "not found or not allowed"
  console.warn("S3 HEAD unexpected", resp.status, await resp.text().catch(() => ""));
  return false;
}

async function s3GetObject(env: Env, bucket: string, key: string): Promise<Response | null> {
  const host = s3Host(bucket, env.AWS_REGION);
  const url = `https://${host}/${encodeURIComponent(key)}`;
  const aws = new AwsClient({
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    region: env.AWS_REGION,
    service: "s3"
  });
  const resp = await aws.fetch(url, { method: "GET" }); // signed GET
  if (resp.status === 200) return resp;
  if (resp.status === 404 || resp.status === 403) return null;
  console.warn("S3 GET unexpected", resp.status, await resp.text().catch(() => ""));
  return null;
}
