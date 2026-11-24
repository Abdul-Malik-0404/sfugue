**Date:** 2026-11-04
**Attendees:** Abdul Malik, Ms. Shaariqah Ismath
**Topic:** PP Slide Review, Evaluation Strategy, and Viva Preparation

## Key Discussion Points
*   **Project Name Confirmation:** Confirmed the `sfugue` naming rationale (SFU + musical fugue metaphor for seamless, uninterrupted transitions).
*   **Evaluation & Testing Constraints:**
    *   Measuring only Time to First Frame (TTFF) is insufficient.
    *   Must evaluate prediction accuracy using a **Confusion Matrix** to track False Positives (false alarms) and False Negatives (missed crashes) when differentiating UDP floods from organic load.
    *   Must measure the **Resource Overhead** (wasted CPU/RAM) if a false positive triggers an unnecessary migration.
    *   Must verify the final **E2EE Media Resumption Success Rate** (whether the stream actually resumes without packet corruption).
*   **Defending the Tech Stack (Kafka vs. Redis):**
    *   "It's easier" is not a valid academic justification. 
    *   Must defend Kafka using its architectural strengths (ordered consumer-group failovers) but have a clear fallback plan (Redis) if empirical testing shows Kafka introduces too much latency.
*   **Literature & Research Gaps:**
    *   Shaariqah demonstrated using AI (Gemini) to find papers showing neural networks *are* used for proactive blackout detection.
    *   **Correction:** Never claim "there is no research". Refine the gap to state that *lightweight pre-crash heuristics for stateful WebRTC SFUs* lack benchmarks compared to heavy ML cloud models.

## Viva Defense Strategy (Crucial Advice)
1.  **No Assumptions:** Every claim or expected result must be backed by empirical research or literature, not guesses.
2.  **Manage Expectations:** Do not present the architecture as 100% finalized or fully implemented yet, as 6 months of research remain. Frame current choices as "Candidate Technologies".
3.  **Think 3 Steps Ahead:** Always have a backup plan for what happens if the primary architecture fails.
4.  **Consistent Examiner:** IIT is changing the rules to assign the *same examiner* for the PP, IPD, and Final submissions to ensure consistent feedback throughout the year.

## Action Items
- [x] Update slides to include a dedicated "Evaluation & Testing Metrics" slide (Confusion Matrix, Overhead, TTFF, Resumption).
- [x] Soften "No Research" claims on the Research Gaps slide to acknowledge existing ML models.
- [x] Add "Candidate" to the technology stack slide to show adaptability.
- [x] Submit PP Slides and Class 1 Ethics Form by tomorrow (Nov 5).
- [ ] Contact **Ammar Raneez** to request domain expert feedback/industry consultation (fulfills RO2).
- [ ] Leverage AI tools to find more cybersecurity and WebRTC DDoS papers to bolster the literature review.