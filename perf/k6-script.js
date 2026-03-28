import http from 'k6/http';
import { check, sleep } from 'k6';

const baseURL = __ENV.BASE_URL || 'http://localhost:9999';
const mode = __ENV.MODE || 'mixed';
const hotKey = __ENV.HOT_KEY || 'Tom';
const seededMax = Number(__ENV.SEEDED_MAX || 100);
const mixedIncludeSeeded = String(__ENV.MIXED_INCLUDE_SEEDED || 'false').toLowerCase() === 'true';
const validKeys = (__ENV.VALID_KEYS || hotKey)
  .split(',')
  .map((s) => s.trim())
  .filter((s) => s.length > 0);
const p95Ms = Number(__ENV.THRESHOLD_P95_MS || 10);
const p99Ms = Number(__ENV.THRESHOLD_P99_MS || 30);
const maxErrorRate = Number(__ENV.THRESHOLD_ERROR_RATE || 0.001);

export const options = {
  vus: Number(__ENV.VUS || 50),
  duration: __ENV.DURATION || '30s',
  thresholds: {
    http_req_failed: [`rate<${maxErrorRate}`],
    // k6 trend thresholds expect numeric values for duration metrics (milliseconds by default).
    http_req_duration: [`p(95)<${p95Ms}`, `p(99)<${p99Ms}`],
  },
};

function pickKey() {
  if (mode === 'hot') {
    return hotKey;
  }

  // mixed: 70% hot key + 30% optional seeded range or known valid keys
  if (Math.random() < 0.7) {
    return hotKey;
  }

  if (mixedIncludeSeeded) {
    const n = Math.floor(Math.random() * seededMax);
    return `key${n}`;
  }

  return validKeys[Math.floor(Math.random() * validKeys.length)];
}

export default function () {
  const key = pickKey();
  const res = http.get(`${baseURL}/api?key=${encodeURIComponent(key)}`);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'body is not empty': (r) => !!r.body && r.body.length > 0,
  });

  sleep(Number(__ENV.SLEEP_SECONDS || 0));
}
