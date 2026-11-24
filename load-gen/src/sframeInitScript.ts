/**
 * Browser-side W3C WebRTC Insertable Streams (SFrame AES-GCM-128) transform.
 *
 * Zero-Trust Security Contract (CLAUDE.md Section 4):
 * - Executed strictly inside Playwright browser contexts via `addInitScript`.
 * - Key derivation, AES-GCM encryption/decryption, and key ratcheting happen
 *   exclusively on client media pipelines; NO encryption keys are ever sent to
 *   `sfu-node`, Socket.IO logs, Prometheus, or Kafka.
 * - Preserves unencrypted VP8 payload header bytes (10B keyframe / 3B delta) as
 *   authenticated additional data (AAD) so Mediasoup forwards frames blindly.
 */

export interface SframeInitOptions {
  enabled: boolean;
  keyHex: string;
  keyId: number;
}

export interface BrowserSframeStats {
  enabled: boolean;
  epoch: number;
  framesEncrypted: number;
  framesDecrypted: number;
  decryptionErrors: number;
  firstFrameDecryptedAtMs: number | null;
  lastRatchetDurationMs: number | null;
}

interface EncodedFrameLike {
  data: ArrayBuffer;
  type?: "key" | "delta" | "empty";
  getMetadata?: () => { synchronizationSource?: number };
}

interface EncodedStreamsLike {
  readable: ReadableStream<EncodedFrameLike>;
  writable: WritableStream<EncodedFrameLike>;
}

interface InsertableRtpStreamOwner {
  createEncodedStreams?: () => EncodedStreamsLike;
}

interface SframeWindowApi {
  __sfugueSframeStats?: BrowserSframeStats & {
    ratchetKey: () => Promise<number>;
  };
  RTCPeerConnection: typeof RTCPeerConnection;
}

export function sframeBrowserInitScript(options: SframeInitOptions): void {
  const win = window as unknown as SframeWindowApi;

  const stats: BrowserSframeStats & { ratchetKey: () => Promise<number> } = {
    enabled: options.enabled,
    epoch: 1,
    framesEncrypted: 0,
    framesDecrypted: 0,
    decryptionErrors: 0,
    firstFrameDecryptedAtMs: null,
    lastRatchetDurationMs: null,
    ratchetKey: async () => 0,
  };
  win.__sfugueSframeStats = stats;

  if (!options.enabled) {
    return;
  }

  function hexToBytes(hex: string): Uint8Array<ArrayBuffer> {
    const clean = hex.trim();
    const out = new Uint8Array(new ArrayBuffer(clean.length / 2));
    for (let i = 0; i < out.length; i += 1) {
      out[i] = Number.parseInt(clean.slice(i * 2, i * 2 + 2), 16);
    }
    return out;
  }

  const baseSecretBytes = hexToBytes(options.keyHex);
  const keyId = options.keyId & 0xff;
  let frameCounter = 1;
  let activeKeyPromise: Promise<CryptoKey> | null = null;

  async function deriveEpochKey(epoch: number): Promise<CryptoKey> {
    const hkdfBase = await crypto.subtle.importKey(
      "raw",
      baseSecretBytes,
      "HKDF",
      false,
      ["deriveKey"],
    );
    const info = new Uint8Array(12);
    const view = new DataView(info.buffer);
    view.setUint32(0, 0x7366726d, false); // "sfrm"
    view.setUint32(4, epoch >>> 0, false);
    view.setUint32(8, keyId >>> 0, false);

    return crypto.subtle.deriveKey(
      {
        name: "HKDF",
        hash: "SHA-256",
        salt: new Uint8Array(16),
        info,
      },
      hkdfBase,
      { name: "AES-GCM", length: 128 },
      false,
      ["encrypt", "decrypt"],
    );
  }

  function getActiveKey(): Promise<CryptoKey> {
    if (!activeKeyPromise) {
      activeKeyPromise = deriveEpochKey(stats.epoch);
    }
    return activeKeyPromise;
  }

  stats.ratchetKey = async (): Promise<number> => {
    const started = performance.now();
    stats.epoch += 1;
    frameCounter = 1;
    activeKeyPromise = deriveEpochKey(stats.epoch);
    await activeKeyPromise;
    stats.lastRatchetDurationMs = performance.now() - started;
    return stats.lastRatchetDurationMs;
  };

  function buildIv(epoch: number, counter: number): Uint8Array<ArrayBuffer> {
    const iv = new Uint8Array(new ArrayBuffer(12));
    const view = new DataView(iv.buffer);
    view.setUint32(0, epoch >>> 0, false);
    view.setUint32(4, keyId >>> 0, false);
    view.setUint32(8, counter >>> 0, false);
    return iv;
  }

  function getUnencryptedPrefixLength(
    frame: EncodedFrameLike,
    kind: "audio" | "video",
    bytes: Uint8Array,
  ): number {
    if (kind === "audio") {
      return 0;
    }
    const isKeyFrame =
      frame.type === "key" || (bytes.length > 0 && (bytes[0] & 0x01) === 0);
    const requested = isKeyFrame ? 10 : 3;
    return Math.min(requested, bytes.length);
  }

  async function encryptFrame(
    frame: EncodedFrameLike,
    kind: "audio" | "video",
    controller: TransformStreamDefaultController<EncodedFrameLike>,
  ): Promise<void> {
    const raw = new Uint8Array(frame.data);
    if (raw.byteLength === 0) {
      controller.enqueue(frame);
      return;
    }

    const prefixLen = getUnencryptedPrefixLength(frame, kind, raw);
    const unencryptedPrefix = raw.slice(0, prefixLen);
    const payload = raw.slice(prefixLen);

    const counter = frameCounter;
    frameCounter = (frameCounter + 1) >>> 0;
    const iv = buildIv(stats.epoch, counter);
    const key = await getActiveKey();

    const encryptedBuffer = await crypto.subtle.encrypt(
      {
        name: "AES-GCM",
        iv,
        additionalData: unencryptedPrefix,
      },
      key,
      payload,
    );

    const encryptedBytes = new Uint8Array(encryptedBuffer);
    const sframeHeader = new Uint8Array(5);
    const headerView = new DataView(sframeHeader.buffer);
    sframeHeader[0] = keyId;
    headerView.setUint32(1, counter, false);

    const output = new Uint8Array(
      prefixLen + sframeHeader.byteLength + encryptedBytes.byteLength,
    );
    output.set(unencryptedPrefix, 0);
    output.set(sframeHeader, prefixLen);
    output.set(encryptedBytes, prefixLen + sframeHeader.byteLength);

    frame.data = output.buffer;
    stats.framesEncrypted += 1;
    controller.enqueue(frame);
  }

  async function decryptFrame(
    frame: EncodedFrameLike,
    kind: "audio" | "video",
    controller: TransformStreamDefaultController<EncodedFrameLike>,
  ): Promise<void> {
    const raw = new Uint8Array(frame.data);
    if (raw.byteLength === 0) {
      controller.enqueue(frame);
      return;
    }

    const prefixLen = getUnencryptedPrefixLength(frame, kind, raw);
    const minPacketSize = prefixLen + 5 + 16;
    if (raw.byteLength < minPacketSize) {
      stats.decryptionErrors += 1;
      return;
    }

    const unencryptedPrefix = raw.slice(0, prefixLen);
    const sframeHeaderView = new DataView(
      raw.buffer,
      raw.byteOffset + prefixLen,
      5,
    );
    const counter = sframeHeaderView.getUint32(1, false);
    const ciphertext = raw.slice(prefixLen + 5);
    const iv = buildIv(stats.epoch, counter);

    try {
      const key = await getActiveKey();
      const plaintextBuffer = await crypto.subtle.decrypt(
        {
          name: "AES-GCM",
          iv,
          additionalData: unencryptedPrefix,
        },
        key,
        ciphertext,
      );

      const plaintext = new Uint8Array(plaintextBuffer);
      const output = new Uint8Array(prefixLen + plaintext.byteLength);
      output.set(unencryptedPrefix, 0);
      output.set(plaintext, prefixLen);

      frame.data = output.buffer;
      stats.framesDecrypted += 1;
      if (stats.firstFrameDecryptedAtMs === null) {
        stats.firstFrameDecryptedAtMs = performance.now();
      }
      controller.enqueue(frame);
    } catch {
      stats.decryptionErrors += 1;
    }
  }

  const transformedOwners = new WeakSet<object>();

  function attachSenderTransform(
    sender: RTCRtpSender,
    kind: "audio" | "video",
  ): void {
    const owner = sender as unknown as InsertableRtpStreamOwner;
    if (transformedOwners.has(sender) || typeof owner.createEncodedStreams !== "function") {
      return;
    }
    transformedOwners.add(sender);

    const { readable, writable } = owner.createEncodedStreams();
    const transformer = new TransformStream<EncodedFrameLike, EncodedFrameLike>({
      transform: (frame, controller) => encryptFrame(frame, kind, controller),
    });
    void readable.pipeThrough(transformer).pipeTo(writable).catch(() => {});
  }

  function attachReceiverTransform(
    receiver: RTCRtpReceiver,
    kind: "audio" | "video",
  ): void {
    const owner = receiver as unknown as InsertableRtpStreamOwner;
    if (transformedOwners.has(receiver) || typeof owner.createEncodedStreams !== "function") {
      return;
    }
    transformedOwners.add(receiver);

    const { readable, writable } = owner.createEncodedStreams();
    const transformer = new TransformStream<EncodedFrameLike, EncodedFrameLike>({
      transform: (frame, controller) => decryptFrame(frame, kind, controller),
    });
    void readable.pipeThrough(transformer).pipeTo(writable).catch(() => {});
  }

  const NativeRTCPeerConnection = win.RTCPeerConnection;
  if (!NativeRTCPeerConnection) {
    return;
  }

  const WrappedRTCPeerConnection = function (
    this: RTCPeerConnection,
    configuration?: RTCConfiguration,
  ): RTCPeerConnection {
    const extendedConfig = {
      ...(configuration ?? {}),
      encodedInsertableStreams: true,
    } as RTCConfiguration;

    const pc = new NativeRTCPeerConnection(extendedConfig);
    pc.addEventListener("track", (event: RTCTrackEvent) => {
      const kind = event.track.kind === "audio" ? "audio" : "video";
      attachReceiverTransform(event.receiver, kind);
    });
    return pc;
  } as unknown as typeof RTCPeerConnection;

  WrappedRTCPeerConnection.prototype = NativeRTCPeerConnection.prototype;
  Object.setPrototypeOf(WrappedRTCPeerConnection, NativeRTCPeerConnection);

  const originalAddTrack = NativeRTCPeerConnection.prototype.addTrack;
  NativeRTCPeerConnection.prototype.addTrack = function (
    this: RTCPeerConnection,
    track: MediaStreamTrack,
    ...streams: MediaStream[]
  ): RTCRtpSender {
    const sender = originalAddTrack.apply(this, [track, ...streams]);
    const kind = track.kind === "audio" ? "audio" : "video";
    attachSenderTransform(sender, kind);
    return sender;
  };

  const originalAddTransceiver = NativeRTCPeerConnection.prototype.addTransceiver;
  NativeRTCPeerConnection.prototype.addTransceiver = function (
    this: RTCPeerConnection,
    trackOrKind: MediaStreamTrack | string,
    init?: RTCRtpTransceiverInit,
  ): RTCRtpTransceiver {
    const transceiver = originalAddTransceiver.apply(this, [trackOrKind, init]);
    const kind =
      typeof trackOrKind === "string"
        ? trackOrKind === "audio"
          ? "audio"
          : "video"
        : trackOrKind.kind === "audio"
          ? "audio"
          : "video";

    if (transceiver.sender && (!init || init.direction !== "recvonly")) {
      attachSenderTransform(transceiver.sender, kind);
    }
    return transceiver;
  };

  win.RTCPeerConnection = WrappedRTCPeerConnection;
}
