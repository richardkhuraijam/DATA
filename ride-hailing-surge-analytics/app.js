// Ride-Hailing Surge Analytics - Presentation Logic

document.addEventListener("DOMContentLoaded", () => {
  // --- Presentation Deck State ---
  let currentSlide = 1;
  const totalSlides = 6;
  let currentMode = "deck"; // "deck" or "scroll"

  // Speaker notes per slide for live presentations
  const speakerNotes = {
    1: "💡 **Speaker Note (Hero Slide):** Welcome the audience. Introduce the NYC TLC HVFHV dataset (50k sample dataset), 263 zones, sub-60s streaming SLA, and end-to-end Python/PySpark architecture.",
    2: "💡 **Speaker Note (Architecture):** Explain the Medallion Pipeline. Highlight the transition from Bronze (raw JSON) to Silver (quality expectations `@dlt.expect_or_drop`), and Gold (business aggregates). Emphasize 84% scan cost drop on Athena!",
    3: "💡 **Speaker Note (Surge Engine):** Demo the live interactive simulator! Show how demand/supply ratio steps from 1.0x to 2.5x, rain adds +0.2x, temperature drop adds +0.1x, and the EWMA smoothing caps at 3.0x max guard.",
    4: "💡 **Speaker Note (ML Demand Model):** Highlight Random Forest model performance ($R^2 = 0.9966$). Show how 30-minute forward forecasts prevent driver supply deficits before they occur.",
    5: "💡 **Speaker Note (GenAI Ops Assistant):** Click the query chips live! Emphasize security: the LLM calls deterministic read-only tools and NEVER writes free-form SQL.",
    6: "💡 **Speaker Note (Reconciliation & SLAs):** Summarize the 100% passed SLAs: Kafka-to-Gold record reconciliation, 3.9 min rider ETA, 81% driver utilization, and 16/16 passing unit tests."
  };

  // --- Element References ---
  const slides = document.querySelectorAll(".slide");
  const dotsContainer = document.getElementById("slide-indicators");
  const prevBtn = document.getElementById("prev-slide-btn");
  const nextBtn = document.getElementById("next-slide-btn");
  const slideCounter = document.getElementById("slide-counter");
  const progressBar = document.getElementById("progress-bar");
  const modeDeckBtn = document.getElementById("mode-deck-btn");
  const modeScrollBtn = document.getElementById("mode-scroll-btn");
  const notesBtn = document.getElementById("notes-btn");
  const notesDrawer = document.getElementById("notes-drawer");
  const notesText = document.getElementById("notes-text");
  const fullscreenBtn = document.getElementById("fullscreen-btn");

  // --- Create Slide Dots ---
  function initDots() {
    if (!dotsContainer) return;
    dotsContainer.innerHTML = "";
    for (let i = 1; i <= totalSlides; i++) {
      const dot = document.createElement("div");
      dot.className = `dot-indicator ${i === currentSlide ? "active" : ""}`;
      dot.setAttribute("data-slide", i);
      dot.addEventListener("click", () => goToSlide(i));
      dotsContainer.appendChild(dot);
    }
  }

  // --- Slide Navigation ---
  function updateSlideState() {
    if (currentMode === "deck") {
      slides.forEach((slide, idx) => {
        if (idx + 1 === currentSlide) {
          slide.classList.add("active");
        } else {
          slide.classList.remove("active");
        }
      });
    }

    // Update controls
    if (slideCounter) slideCounter.textContent = `0${currentSlide} / 0${totalSlides}`;
    if (progressBar) progressBar.style.width = `${(currentSlide / totalSlides) * 100}%`;

    // Update dots
    document.querySelectorAll(".dot-indicator").forEach((dot, idx) => {
      if (idx + 1 === currentSlide) {
        dot.classList.add("active");
      } else {
        dot.classList.remove("active");
      }
    });

    if (prevBtn) prevBtn.disabled = currentSlide === 1;
    if (nextBtn) nextBtn.disabled = currentSlide === totalSlides;

    // Update speaker notes
    if (notesText) notesText.innerHTML = speakerNotes[currentSlide] || "";
    
    // Draw chart if on ML slide
    if (currentSlide === 4) {
      renderMLChart();
    }
  }

  function goToSlide(slideNum) {
    if (slideNum < 1 || slideNum > totalSlides) return;
    currentSlide = slideNum;
    updateSlideState();
  }

  function nextSlide() {
    if (currentSlide < totalSlides) {
      currentSlide++;
      updateSlideState();
    }
  }

  function prevSlide() {
    if (currentSlide > 1) {
      currentSlide--;
      updateSlideState();
    }
  }

  // --- Keyboard Shortcuts ---
  document.addEventListener("keydown", (e) => {
    if (currentMode !== "deck") return;
    if (e.key === "ArrowRight" || e.key === "Space") {
      e.preventDefault();
      nextSlide();
    } else if (e.key === "ArrowLeft") {
      e.preventDefault();
      prevSlide();
    } else if (e.key.toLowerCase() === "n") {
      toggleNotes();
    } else if (e.key.toLowerCase() === "f") {
      toggleFullscreen();
    }
  });

  if (prevBtn) prevBtn.addEventListener("click", prevSlide);
  if (nextBtn) nextBtn.addEventListener("click", nextSlide);

  // --- View Mode Toggle (Deck vs Scroll) ---
  if (modeDeckBtn && modeScrollBtn) {
    modeDeckBtn.addEventListener("click", () => {
      currentMode = "deck";
      document.body.className = "mode-deck";
      modeDeckBtn.classList.add("active");
      modeScrollBtn.classList.remove("active");
      updateSlideState();
    });

    modeScrollBtn.addEventListener("click", () => {
      currentMode = "scroll";
      document.body.className = "mode-scroll";
      modeScrollBtn.classList.add("active");
      modeDeckBtn.classList.remove("active");
      updateSlideState();
    });
  }

  // --- Speaker Notes Toggle ---
  function toggleNotes() {
    if (notesDrawer) {
      notesDrawer.classList.toggle("open");
    }
  }

  if (notesBtn) notesBtn.addEventListener("click", toggleNotes);

  // --- Fullscreen Toggle ---
  function toggleFullscreen() {
    if (!document.fullscreenElement) {
      document.documentElement.requestFullscreen().catch(() => {});
    } else {
      if (document.exitFullscreen) document.exitFullscreen();
    }
  }

  if (fullscreenBtn) fullscreenBtn.addEventListener("click", toggleFullscreen);

  // ==========================================
  // ⚡ INTERACTIVE SURGE ENGINE SIMULATOR
  // ==========================================
  const zoneChips = document.querySelectorAll(".zone-chip");
  const demandInput = document.getElementById("sim-demand");
  const supplyInput = document.getElementById("sim-supply");
  const rainInput = document.getElementById("sim-rain");
  const tempInput = document.getElementById("sim-temp");

  const valDemand = document.getElementById("val-demand");
  const valSupply = document.getElementById("val-supply");
  const valRain = document.getElementById("val-rain");
  const valTemp = document.getElementById("val-temp");

  const outSurgeVal = document.getElementById("out-surge-val");
  const outRatio = document.getElementById("out-ratio");
  const outBase = document.getElementById("out-base");
  const outWeather = document.getElementById("out-weather");
  const outCapGuard = document.getElementById("out-cap-guard");

  const zonesData = {
    161: { name: "Midtown Manhattan", demand: 180, supply: 60, rain: 2.5, temp: 4 },
    132: { name: "JFK Airport", demand: 240, supply: 90, rain: 0.0, temp: 12 },
    87:  { name: "Financial District", demand: 110, supply: 70, rain: 5.0, temp: -2 },
    255: { name: "Williamsburg", demand: 95, supply: 50, rain: 1.0, temp: 15 },
    209: { name: "SoHo", demand: 160, supply: 45, rain: 3.0, temp: 8 }
  };

  zoneChips.forEach(chip => {
    chip.addEventListener("click", () => {
      zoneChips.forEach(c => c.classList.remove("active"));
      chip.classList.add("active");
      const zoneId = chip.getAttribute("data-zone");
      if (zonesData[zoneId]) {
        const z = zonesData[zoneId];
        if (demandInput) demandInput.value = z.demand;
        if (supplyInput) supplyInput.value = z.supply;
        if (rainInput) rainInput.value = z.rain;
        if (tempInput) tempInput.value = z.temp;
        calculateSurge();
      }
    });
  });

  function calculateSurge() {
    if (!demandInput || !supplyInput || !rainInput || !tempInput) return;

    const demand = parseFloat(demandInput.value);
    const supply = Math.max(1, parseFloat(supplyInput.value));
    const rain = parseFloat(rainInput.value);
    const temp = parseFloat(tempInput.value);

    // Update UI badge labels
    if (valDemand) valDemand.textContent = demand;
    if (valSupply) valSupply.textContent = supply;
    if (valRain) valRain.textContent = `${rain.toFixed(1)} mm/h`;
    if (valTemp) valTemp.textContent = `${temp}°C`;

    const ratio = demand / supply;

    // Step function for base multiplier
    let base = 1.0;
    if (ratio >= 3.0) base = 2.5;
    else if (ratio >= 2.2) base = 2.0;
    else if (ratio >= 1.6) base = 1.6;
    else if (ratio >= 1.2) base = 1.3;

    // Weather adjustments
    let weatherAdder = 0.0;
    if (rain > 2.0) weatherAdder += 0.2;
    if (temp < 0.0) weatherAdder += 0.1;

    // Raw calculated surge
    let rawSurge = base + weatherAdder;

    // Cap at 3.0x max guard
    let finalSurge = Math.min(3.0, Math.max(1.0, rawSurge));

    // Update results UI
    if (outSurgeVal) outSurgeVal.textContent = `${finalSurge.toFixed(2)}x`;
    if (outRatio) outRatio.textContent = `${ratio.toFixed(2)}`;
    if (outBase) outBase.textContent = `${base.toFixed(1)}x`;
    if (outWeather) outWeather.textContent = `+${weatherAdder.toFixed(1)}x`;
    if (outCapGuard) outCapGuard.textContent = finalSurge >= 3.0 ? "ACTIVE (3.0x Cap)" : "Normal";
  }

  [demandInput, supplyInput, rainInput, tempInput].forEach(inp => {
    if (inp) inp.addEventListener("input", calculateSurge);
  });

  // ==========================================
  // 🏗️ INTERACTIVE MEDALLION PIPELINE EXPLORER
  // ==========================================
  const nodes = document.querySelectorAll(".pipeline-node");
  const detailTitle = document.getElementById("pipeline-detail-title");
  const codeDisplay = document.getElementById("code-display");

  const pipelineDetails = {
    ingestion: {
      title: "1. Ingestion Layer (MySQL + Kafka Producers)",
      code: `# Ingestion Producer (ingestion/producers/tlc_replay.py)
event = {
    "event_id": str(uuid.uuid4()),
    "trip_id": row["trip_id"],
    "pickup_zone_id": int(row["PULocationID"]),
    "dropoff_zone_id": int(row["DOLocationID"]),
    "request_time": row["request_datetime"].isoformat(),
    "fare": float(row["base_passenger_fare"]),
    "status": "COMPLETED"
}
kafka_producer.send("trip-requests", value=event)`
    },
    medallion: {
      title: "2. PySpark Medallion Cleanse & Validation (DLT Rules)",
      code: `# DLT Pipeline Rules (lakehouse/dlt_pipeline.py)
@dlt.table(comment="Cleaned and validated silver trips table")
@dlt.expect_or_drop("valid_gps", "lat BETWEEN 40.4 AND 41.0 AND lon BETWEEN -74.3 AND -73.6")
@dlt.expect_or_drop("positive_fare", "fare > 0")
@dlt.expect_or_drop("non_null_zone", "pickup_zone_id IS NOT NULL")
def silver_trips():
    return dlt.read_stream("bronze_trips").dropDuplicates(["event_id"])`
    },
    lakehouse: {
      title: "3. Lakehouse Storage & Athena Benchmark (84% Cost Drop)",
      code: `# Athena Partitioning Benchmark (lakehouse/athena_benchmark.py)
# Partitioned by Year/Month/Day/Hour on S3 Delta Lake
Raw CSV Scan: 450.0 MB
Partitioned Parquet Scan: 72.0 MB
Scan Reduction: 84.0% (Target >= 70%: PASSED)`
    },
    genai: {
      title: "4. ML Forecast & GenAI Operations Assistant",
      code: `# GenAI Operations Assistant Tool Router (genai/assistant_app.py)
@st.cache_data
def route_query_to_tool(prompt: str) -> str:
    if "surge" in prompt.lower():
        res = get_current_surge(zone_id=161)
        return f"Current Surge Midtown: {res['surge_multiplier']}x"`
    }
  };

  nodes.forEach(node => {
    node.addEventListener("click", () => {
      nodes.forEach(n => n.classList.remove("active"));
      node.classList.add("active");
      const key = node.getAttribute("data-step");
      if (pipelineDetails[key]) {
        if (detailTitle) detailTitle.textContent = pipelineDetails[key].title;
        if (codeDisplay) codeDisplay.textContent = pipelineDetails[key].code;
      }
    });
  });

  // ==========================================
  // 🤖 GENAI ASSISTANT INTERACTIVE CHAT DEMO
  // ==========================================
  const chatBody = document.getElementById("chat-body");
  const promptChips = document.querySelectorAll(".prompt-chip");

  const chatAnswers = {
    "top_surge": "**Top 5 Zones by Surge Multiplier Right Now:**\n1. **Midtown Manhattan (Zone 161)** — Surge: `2.80x` | Demand: 180 | ETA: 3.2 min\n2. **Times Square / Theatre District (Zone 230)** — Surge: `2.50x` | Demand: 165 | ETA: 4.1 min\n3. **Financial District (Zone 87)** — Surge: `2.30x` | Demand: 140 | ETA: 3.8 min\n4. **SoHo (Zone 209)** — Surge: `2.10x` | Demand: 125 | ETA: 3.5 min\n5. **JFK Airport (Zone 132)** — Surge: `1.80x` | Demand: 210 | ETA: 4.4 min",
    "driver_util": "**Driver Utilisation Summary (Today):**\n- **Average Utilisation Rate:** `81.0%` (Target >= 78%: **PASSED**)\n- **Active Drivers Online:** 1,452 drivers\n- **Total Completed Trips Today:** 47,094 trips",
    "forecast": "**30-Minute Demand Forecast for Zone 161 (Midtown):**\n- **Predicted Demand (Next 30 min):** `194.5` trip requests\n- **Recent 30m Actual Demand:** `180.0` requests\n- **Forecast Model:** Random Forest Regressor ($R^2 = 0.9966$)\n- **Recommendation:** Reposition +25 idle drivers from Zone 230 to Zone 161.",
    "kpi_summary": "**Executive KPI Summary:**\n- **Avg Rider ETA:** `3.9 min` (Target < 4.5 min: **PASSED**)\n- **Driver Utilisation Rate:** `81.0%` (Target >= 78%: **PASSED**)\n- **Athena Query Scan Cost Drop:** `84.0%` (Target >= 70%: **PASSED**)\n- **Demand Forecast Accuracy ($R^2$):** `0.9966` (Target >= 0.70: **PASSED**)"
  };

  promptChips.forEach(chip => {
    chip.addEventListener("click", () => {
      const qKey = chip.getAttribute("data-query");
      const questionText = chip.textContent;

      if (!chatBody) return;

      // Append User message
      const userDiv = document.createElement("div");
      userDiv.className = "chat-msg user";
      userDiv.innerHTML = `<div class="msg-bubble">${questionText}</div>`;
      chatBody.appendChild(userDiv);

      // Append Assistant thinking & answer
      setTimeout(() => {
        const botDiv = document.createElement("div");
        botDiv.className = "chat-msg assistant";
        const ans = chatAnswers[qKey] || "Processing request with Ops Assistant tools...";
        botDiv.innerHTML = `
          <div class="bot-avatar">🚕</div>
          <div class="msg-bubble">${ans.replace(/\n/g, "<br>")}</div>
        `;
        chatBody.appendChild(botDiv);
        chatBody.scrollTop = chatBody.scrollHeight;
      }, 300);
    });
  });

  // ==========================================
  // 📈 ML DEMAND FORECAST CANVAS CHART
  // ==========================================
  function renderMLChart() {
    const canvas = document.getElementById("ml-chart-canvas");
    if (!canvas) return;

    const ctx = canvas.getContext("2d");
    const width = canvas.parentElement.clientWidth - 40;
    canvas.width = width;
    canvas.height = 240;

    ctx.clearRect(0, 0, width, 240);

    // Sample demand data (Hours 0 - 23)
    const actual = [25, 18, 12, 8, 15, 35, 80, 140, 190, 160, 130, 145, 165, 155, 170, 185, 210, 240, 220, 180, 140, 95, 60, 40];
    const predicted = [24, 19, 11, 9, 14, 36, 78, 142, 188, 162, 128, 146, 163, 157, 168, 187, 208, 242, 218, 182, 138, 96, 59, 41];

    const padding = 40;
    const chartW = width - padding * 2;
    const chartH = 240 - padding * 2;
    const maxVal = 260;

    // Draw Grid Lines
    ctx.strokeStyle = "rgba(255, 255, 255, 0.05)";
    ctx.lineWidth = 1;
    for (let y = 0; y <= 4; y++) {
      const yPos = padding + (chartH / 4) * y;
      ctx.beginPath();
      ctx.moveTo(padding, yPos);
      ctx.lineTo(width - padding, yPos);
      ctx.stroke();
    }

    // Function to map data to canvas X/Y
    function getX(i) { return padding + (chartW / (actual.length - 1)) * i; }
    function getY(val) { return padding + chartH - (val / maxVal) * chartH; }

    // Draw Actual Line (Cyan)
    ctx.strokeStyle = "#00f2fe";
    ctx.lineWidth = 3;
    ctx.beginPath();
    actual.forEach((val, i) => {
      if (i === 0) ctx.moveTo(getX(i), getY(val));
      else ctx.lineTo(getX(i), getY(val));
    });
    ctx.stroke();

    // Draw Predicted Line (Pink Dashed)
    ctx.strokeStyle = "#f538a0";
    ctx.lineWidth = 2.5;
    ctx.setLineDash([5, 5]);
    ctx.beginPath();
    predicted.forEach((val, i) => {
      if (i === 0) ctx.moveTo(getX(i), getY(val));
      else ctx.lineTo(getX(i), getY(val));
    });
    ctx.stroke();
    ctx.setLineDash([]);
  }

  // --- Initial Setup ---
  initDots();
  calculateSurge();
  updateSlideState();
});
