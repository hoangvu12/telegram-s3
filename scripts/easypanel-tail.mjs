#!/usr/bin/env node
// One-shot Easypanel log tail. Connects to wss://<panel>/serviceLogs
// with the credentials in ~/.easypanel/config.json, streams everything
// the panel sends for `--duration` seconds (default 30), and exits.
// Use this when you want to grep a service's runtime log from the CLI
// without opening the Easypanel UI panel — the easypanel-skill tRPC
// helper can't drive WebSockets, so this stands alone.
//
// Usage:
//   node scripts/easypanel-tail.mjs --project=telegram-s3 --service=api [--duration=30] [--compose=0|1] [--grep=...]
//
// Exits 0 on clean disconnect / timeout, 1 on connection error.

import { readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const m = a.match(/^--([^=]+)=(.*)$/);
    return m ? [m[1], m[2]] : [a.replace(/^--/, ''), true];
  }),
);

const project = args.project;
const service = args.service;
if (!project || !service) {
  console.error('usage: easypanel-tail.mjs --project=NAME --service=NAME [--duration=30] [--compose=0|1] [--grep=PATTERN]');
  process.exit(2);
}
const duration = Number(args.duration ?? 30) * 1000;
const compose = args.compose ?? '0';
const grepRe = args.grep ? new RegExp(args.grep) : null;

const cfg = JSON.parse(readFileSync(join(homedir(), '.easypanel', 'config.json'), 'utf8'));
const wsUrl = new URL(cfg.url);
wsUrl.protocol = wsUrl.protocol === 'https:' ? 'wss:' : 'ws:';
// Path is /ws/serviceLogs (NOT /serviceLogs as the easypanel-skill
// SKILL.md claimed) and the `compose` flag is "true"/"false", not
// "0"/"1". The OpenAPI spec doesn't expose this endpoint at all —
// values empirically verified against panel 2.28.0.
wsUrl.pathname = '/ws/serviceLogs';
wsUrl.searchParams.set('token', cfg.token);
wsUrl.searchParams.set('service', `${project}_${service}`);
wsUrl.searchParams.set('compose', compose === '1' || compose === 'true' ? 'true' : 'false');

// The panel's SSL cert may be self-signed depending on deploy; mirror the
// easypanel.mjs helper's tolerance so this stays usable on the same
// infrastructure that helper works against.
process.env.NODE_TLS_REJECT_UNAUTHORIZED ??= '0';

const ws = new WebSocket(wsUrl.href);
let receivedAny = false;

ws.addEventListener('open', () => {
  console.error(`[tail] connected; reading for ${duration / 1000}s${grepRe ? ` (grep=${grepRe})` : ''}`);
});

ws.addEventListener('message', (ev) => {
  receivedAny = true;
  // The panel wraps each batch in {"output":"<concatenated lines>"}.
  // Older docs claimed raw text or {"data":"..."}; cover all three
  // shapes so the script keeps working if the envelope changes.
  let text = typeof ev.data === 'string' ? ev.data : ev.data?.toString?.('utf8') ?? '';
  try {
    const j = JSON.parse(text);
    if (typeof j === 'object' && j !== null) {
      if (typeof j.output === 'string') text = j.output;
      else if (typeof j.data === 'string') text = j.data;
    }
  } catch {}
  for (const line of text.split(/\r?\n/)) {
    if (!line) continue;
    if (!grepRe || grepRe.test(line)) console.log(line);
  }
});

ws.addEventListener('error', (e) => {
  console.error('[tail] ws error:', e?.message ?? e);
  process.exit(1);
});

ws.addEventListener('close', (ev) => {
  console.error(`[tail] closed code=${ev.code} reason=${ev.reason || '<none>'}; received=${receivedAny}`);
  process.exit(0);
});

setTimeout(() => {
  console.error(`[tail] duration elapsed; closing (received=${receivedAny})`);
  ws.close();
}, duration);
