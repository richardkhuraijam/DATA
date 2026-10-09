"""GenAI Operations Assistant Streamlit Application.

Interactive chat interface for operations team to query platform status.
Calls read-only tools:
- get_current_surge(zone)
- top_zones(metric, n)
- driver_utilisation(date, zone)
- demand_forecast(zone)
- kpi_summary(date)

The LLM NEVER writes free-form SQL!
"""

from __future__ import annotations

import os
from pathlib import Path
import streamlit as st
from dotenv import load_dotenv

from genai.tools import (
    demand_forecast,
    driver_utilisation,
    get_current_surge,
    kpi_summary,
    top_zones,
)

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

st.set_page_config(page_title="RideHail Ops Assistant", page_icon="🚕", layout="wide")

st.title("🚕 Ride-Hailing Ops Assistant (GenAI)")
st.caption("Ask natural language questions about current surge, driver utilisation, demand forecasts, and SLAs.")


def route_query_to_tool(prompt: str) -> str:
    prompt_lower = prompt.lower()

    if "surge" in prompt_lower and ("current" in prompt_lower or "zone" in prompt_lower):
        # Extract zone or default to 161 (Midtown)
        zone_id = 161
        for word in prompt_lower.split():
            if word.isdigit():
                zone_id = int(word)
                break
        res = get_current_surge(zone_id)
        return (
            f"**Current Surge for Zone {res['zone_id']}:**\n"
            f"- **Surge Multiplier:** `{res['surge_multiplier']}x`\n"
            f"- **Demand:** {res['demand']} requests\n"
            f"- **Supply:** {res['supply']} available drivers\n"
            f"- **Demand/Supply Ratio:** {res['ratio']}\n"
            f"- **Weather:** {res['temperature_c']}°C, {res['rain_mm_h']} mm/h rain\n"
            f"- **Last Updated:** {res['updated_at']}"
        )

    elif "top" in prompt_lower or "highest" in prompt_lower or "busiest" in prompt_lower:
        res = top_zones(metric="surge_multiplier", n=5)
        text = "**Top 5 Zones by Surge Multiplier:**\n\n"
        for idx, z in enumerate(res, 1):
            text += f"{idx}. **{z['zone_name']} (Zone {z['zone_id']})** — Surge: `{z['surge_multiplier']}x` | Demand: {z['demand']} | ETA: {z['wait_time_min']} min\n"
        return text

    elif "utilisation" in prompt_lower or "utilization" in prompt_lower or "driver" in prompt_lower:
        res = driver_utilisation()
        return (
            f"**Driver Utilisation Summary ({res['date']}):**\n"
            f"- **Average Utilisation Rate:** `{res['avg_driver_utilisation_rate'] * 100:.1f}%` (Target >= 78%: {'PASSED' if res['target_met'] else 'FAILED'})\n"
            f"- **Active Drivers:** {res['active_drivers']:,}\n"
            f"- **Total Completed Trips:** {res['total_completed_trips']:,}"
        )

    elif "forecast" in prompt_lower or "predict" in prompt_lower or "next 30" in prompt_lower:
        zone_id = 161
        for word in prompt_lower.split():
            if word.isdigit():
                zone_id = int(word)
                break
        res = demand_forecast(zone_id)
        return (
            f"**30-Minute Forward Demand Forecast for Zone {res['zone_id']}:**\n"
            f"- **Predicted Demand (Next 30 min):** `{res['predicted_demand_next_30m']:.1f}` trip requests\n"
            f"- **Recent 30m Actual Demand:** {res['recent_lag_30m']:.1f}\n"
            f"- **Forecast Timestamp:** {res['forecast_generated_at']}"
        )

    else:
        res = kpi_summary()
        return (
            f"**Executive KPI Summary ({res['date']}):**\n"
            f"- **Average Rider ETA:** `{res['avg_rider_eta_min']} min` (Target < 4.5 min: {'PASSED' if res['eta_target_met'] else 'FAILED'})\n"
            f"- **Driver Utilisation Rate:** `{res['driver_utilisation_rate'] * 100:.1f}%` (Target >= 78%: {'PASSED' if res['utilisation_target_met'] else 'FAILED'})\n"
            f"- **Total Platform Revenue:** `${res['total_revenue_usd']:,.2f}`\n"
            f"- **Athena Query Scan Cost Drop:** `{res['athena_scan_cost_reduction_pct']}%` (Target >= 70%)\n"
            f"- **Forecast Model Accuracy (R²):** `{res['forecast_model_r2']}` (Target >= 0.70)"
        )


if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": "Hello! I am your Ride-Hailing Ops Assistant. Ask me about current zone surge, driver utilisation, top surge zones, demand forecasts, or KPI summaries.",
        }
    ]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("e.g. Which 5 zones have the highest surge right now?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    response_text = route_query_to_tool(prompt)
    st.session_state.messages.append({"role": "assistant", "content": response_text})
    with st.chat_message("assistant"):
        st.markdown(response_text)
