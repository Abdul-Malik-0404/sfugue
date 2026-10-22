1. [x] **Flesh out the core SFU logic (`server.ts`):** Now that the worker process is alive, you need to create a Mediasoup **Router**, set up the WebRTC **Transports** (for sending and receiving media), and establish the basic signaling logic (like Socket.io or WebSockets) so clients can actually connect, produce, and consume tracks.

	1. [x] **The Consumer Backend:** We need to add the `createConsumerTransport` and `consume` Socket.io endpoints to your `server.ts` file so the router can broadcast the ingested video to other connected users.
	
	2. [x] **The Front-End Client:** We need a simple HTML/JS page using the `mediasoup-client` SDK to capture your webcam, trigger those Socket.io events, and render the video on your screen.

2. [ ] **Integrate Prometheus Telemetry:** Since your research revolves around detecting a "predictive death signature" for failovers, your `sfu-node` needs to expose metrics. You can start wiring up Prometheus client libraries to scrape the Mediasoup worker's internal stats (CPU usage, transport packet loss, jitter) so you have the data needed for your triggers.
    
3. [ ] **Containerization & KinD Deployment:** If you want to get the infrastructure out of the way, you can write the `Dockerfile` for this node and create the Kubernetes deployment and service manifests for your Primary and Standby pods within your KinD testbed.
    
4. [ ] **Scaffold the Playwright Load Generator:** To eventually test those failover blackout latency deltas, you will need a way to simulate SFrame-encrypted traffic. You could start setting up the Playwright automation scripts to generate that dummy load.