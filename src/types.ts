// ─── EAST Railway Hub — shared message protocol ─────────────────────
// Railway is NOT part of consensus. It only relays:
//   Validator (L1, Vercel)  →  Railway  →  all connected Light Nodes
//   Light Node  →  Railway  →  Validator (heartbeat / sync_request / ack / tx)
//   Light Node  ⇄  Railway  ⇄  Light Node (WebRTC signaling passthrough only —
//                                           Railway never inspects SDP/ICE content)
// This file must stay in sync with src/lib/lightnode/protocol.ts in the
// Next.js app — copy changes both ways.

export type Role = "validator" | "light-node";

// Reserved on ChainList/ethereum-lists for EAST's mainnet. Used here purely
// as a network-identity check on `hello` — NOT the same guarantee as EIP-155
// tx replay protection (this hub doesn't sign/relay raw transactions). Keep
// numerically in sync with EAST_CHAIN_ID in src/lib/contracts/registry.ts.
export const EAST_CHAIN_ID = 172026;

export interface BlockHeader {
  height: number;
  hash: string;
  previousHash: string;
  merkleRoot: string;
  validator: string | null;
  timestamp: number;
  epoch: number;
  signature?: string | null; // 0x-prefixed secp256k1 EIP-191 sig, see chain-signing.ts
}

export interface HelloMessage {
  type: "hello";
  role: Role;
  nodeId: string;
  secret?: string; // required when role === "validator"
  chainId?: number; // light nodes should send EAST_CHAIN_ID; missing = pre-upgrade client, allowed through with a warning rather than dropped
}

export interface WelcomeMessage {
  type: "welcome";
  network: "EAST";
  chainId: number;
  version: string;
  role: Role;
  latestHeight: number;
}

export interface BlockNewMessage {
  type: "block:new";
  header: BlockHeader;
}

export interface BlockBackfillMessage {
  type: "block:backfill";
  headers: BlockHeader[];
}

export interface HeartbeatMessage {
  type: "heartbeat";
  nodeId: string;
  height: number;
  timestamp: number;
}

export interface SyncRequestMessage {
  type: "sync_request";
  nodeId: string;
  fromHeight: number;
}

export interface AckMessage {
  type: "ack";
  nodeId: string;
  height: number;
  timestamp: number;
}

export interface TxSubmitMessage {
  type: "tx:submit";
  nodeId: string;
  payload: unknown;
}

// ── Relay scoring & promotion ─────────────────────────────────────────
// A light node self-reports its own measured connection quality; Railway
// never trusts this alone for anything security-relevant (a lying node can
// only make ITSELF look like a better relay candidate — every header it
// ever forwards to a peer is still independently signature-verified
// client-side in verifyHeader(), so a malicious "relay" can at worst be
// useless, never a trusted data source).
export interface RelayStatsMessage {
  type: "relay_stats";
  nodeId: string;
  avgLatencyMs: number;
  participationSeconds: number;
  verifiedHeaderCount: number;
}

// Broadcast periodically to ALL light nodes with the current top-N relay
// roster, so any node can decide who to attempt a WebRTC connection to.
export interface RelayRosterMessage {
  type: "relay:roster";
  relayNodeIds: string[];
}

// Sent 1:1 to a node when it enters/leaves the top-N this round.
export interface RelayPromotedMessage { type: "relay:promoted"; }
export interface RelayDemotedMessage { type: "relay:demoted"; }

// ── WebRTC signaling passthrough ────────────────────────────────────
// Railway just forwards these by toNodeId → socket lookup, unmodified,
// same as it already does for tx:submit. It never parses sdp/candidate.
export interface WebrtcOfferMessage {
  type: "webrtc_offer";
  fromNodeId: string; // set by Railway on relay, ignored if sent by client
  toNodeId: string;
  sdp: string;
}
export interface WebrtcAnswerMessage {
  type: "webrtc_answer";
  fromNodeId: string;
  toNodeId: string;
  sdp: string;
}
export interface IceCandidateMessage {
  type: "ice_candidate";
  fromNodeId: string;
  toNodeId: string;
  candidate: string;
}

export interface PingMessage { type: "ping"; }
export interface PongMessage { type: "pong"; time: number; }
export interface ErrorMessage { type: "error"; message: string; }

export type InboundMessage =
  | HelloMessage
  | BlockNewMessage
  | HeartbeatMessage
  | SyncRequestMessage
  | AckMessage
  | TxSubmitMessage
  | RelayStatsMessage
  | WebrtcOfferMessage
  | WebrtcAnswerMessage
  | IceCandidateMessage
  | PingMessage;

