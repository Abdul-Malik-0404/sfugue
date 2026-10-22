**SFrame** (short for **Secure Frame**) is an IETF draft standard and W3C-aligned API designed to provide true **End-to-End Encryption (E2EE)** for real-time media streams in WebRTC applications.
### 1. The Problem SFrame Solves
In standard WebRTC, transport-level security uses **DTLS-SRTP**. While this encrypts the stream _in transit_ between your browser and the Selective Forwarding Unit (SFU), the SFU has to decrypt the media packets to inspect, transcode, or forward them to other participants. That means the intermediate media server has access to raw audio and video plaintext.

SFrame eliminates this by moving encryption to the application layer.
### 2. How SFrame Works
SFrame works hand-in-hand with **WebRTC Insertable Streams**:
- **Client-Side Encryption:** Before a raw video or audio frame (e.g., a VP8 or Opus packet) is handed over to the WebRTC transport layer for network packetization, an SFrame transform worker encrypts the media payload using an authenticated cipher (such as **AES-GCM** or **ChaCha20-Poly1305**).
- **SFU Blind Forwarding:** Crucially, SFrame encrypts _only_ the payload while leaving the standard **RTP headers unencrypted** (or structurally intact). This means an SFU like Mediasoup can read the sequence numbers, timestamps, and SSRC headers required to route the packet without ever possessing the keys to decrypt the video or audio content.
- **Client-Side Decryption:** The receiving peer's browser receives the encrypted frame and uses its local SFrame worker to decrypt it before passing it to the decoder.
### 3. Why SFrame Matters for `sfugue`
Because SFrame relies on application-layer key distribution and rolling cryptographic counters (**ratchets**) managed directly between communicating peers, it introduces a severe stateful dependency during network disruptions:
- When an SFU node crashes or a client is forced to migrate to a standby node, the network path changes, requiring an ICE restart.
- Simultaneously, the client's local SFrame transform worker must **resynchronize its cryptographic ratchets, re-establish encryption contexts, and often wait for a fresh keyframe (IDR)** from the publisher to decrypt incoming frames.
- This hidden cryptographic overhead is precisely what creates the multi-second **E2EE recovery blackout** that `sfugue` is designed to measure and eliminate via proactive [[Kafka]] handovers.