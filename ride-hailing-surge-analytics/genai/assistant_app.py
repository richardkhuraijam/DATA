"""Streamlit Operations Assistant for Ride-Hailing Surge Analytics (Hyderabad).

Powered exclusively by Google Gemini (gemini-2.5-flash) with tool-calling
over real pipeline outputs: HBase, Bronze Parquet, and ML Demand Forecasts.

Run with:
    streamlit run genai/assistant_app.py
"""
import datetime as dt
import os
from pathlib import Path
import sys
from typing import Any

# Ensure workspace root is in sys.path so 'genai', 'data', 'storage', 'ml' are importable
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from google import genai
from google.genai import types
import streamlit as st

# Load project-root .env
load_dotenv(PROJECT_ROOT / ".env")

try:
    from genai.tools import (
        demand_forecast,
        driver_utilisation,
        get_current_surge,
        kpi_summary,
        top_zones as _raw_top_zones,
    )
except ImportError:
    from tools import (  # fallback if executed directly inside genai directory
        demand_forecast,
        driver_utilisation,
        get_current_surge,
        kpi_summary,
        top_zones as _raw_top_zones,
    )


def top_zones(metric: str = "surge_multiplier", n: int = 5) -> dict:
    """Top N zones right now by surge_multiplier, demand, or ratio."""
    res = _raw_top_zones(metric=metric, n=n)
    if isinstance(res, dict):
        return res
    return {"zones": res}


SYSTEM_PROMPT = (
    f"You are the operations assistant for a Hyderabad ride-hailing platform. "
    f"Today is {dt.date.today()}. "
    "You must answer user questions ONLY by calling the provided analytics tools. "
    "If a tool returns an error or no data, explain that clearly to the operator. "
    "Never invent or hallucinate metrics, fares, or numbers. "
    "Currency is Indian Rupees (INR). Be concise, actionable, and professional."
)

GEMINI_TOOLS = [
    get_current_surge,
    top_zones,
    driver_utilisation,
    demand_forecast,
    kpi_summary,
]


def extract_response_text(response: Any) -> str:
    """Extract final text output from Gemini response or candidate parts."""
    if hasattr(response, "text") and response.text:
        return response.text.strip()

    if hasattr(response, "candidates") and response.candidates:
        text_parts = []
        for cand in response.candidates:
            if cand.content and cand.content.parts:
                for part in cand.content.parts:
                    if getattr(part, "text", None):
                        text_parts.append(part.text)
        if text_parts:
            return "\n".join(text_parts).strip()

    return "Query executed successfully, but no text summary was generated."


def init_chat_session(api_key: str, model: str):
    """Create a persistent Gemini client and chat with automatic tool calling."""
    client = genai.Client(api_key=api_key)
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        tools=GEMINI_TOOLS,
        temperature=0.2,
    )
    # Ensure smooth fallback if Google retires older model versions
    candidates = [model, "gemini-3.5-flash-lite", "gemini-3.8-flash"]
    seen = set()
    last_exc = None
    for m in candidates:
        if not m or m in seen:
            continue
        seen.add(m)
        try:
            chat = client.chats.create(model=m, config=config)
            return client, chat, m
        except Exception as exc:
            last_exc = exc
    raise last_exc or RuntimeError("Could not initialize chat with candidate models")


def main() -> None:
    st.set_page_config(
        page_title="Hyderabad Surge Ops Assistant",
        page_icon="🚕",
        layout="centered",
    )

    st.title("🚕 Hyderabad Surge Ops Assistant")
    st.caption("Powered by **Google Gemini Flash** with real-time operational tool calling")

    # Read API Key from environment or allow manual entry
    raw_env_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or ""
    clean_env_key = raw_env_key.strip().strip("[]'\"")
    configured_model = os.getenv("LLM_MODEL", "gemini-3.5-flash-lite").strip() or "gemini-3.5-flash-lite"

    with st.sidebar:
        st.header("⚙️ Gemini Configuration")
        api_key_input = st.text_input(
            "Gemini API Key",
            value=clean_env_key,
            type="password",
            help="From Google AI Studio (https://aistudio.google.com/). You can also set GEMINI_API_KEY in .env",
        )
        api_key = api_key_input.strip().strip("[]'\"")

        model_name = st.text_input(
            "Model",
            value=configured_model,
            help="Default: gemini-3.5-flash-lite (or gemini-3.8-flash)",
        ).strip()

        st.markdown("---")
        st.markdown("### 🛠️ Active Operational Tools")
        st.markdown(
            "- `get_current_surge(zone)`: Live multiplier, demand, supply from HBase\n"
            "- `top_zones(metric, n)`: Ranked hotspots by surge/demand/ratio\n"
            "- `driver_utilisation(date, zone)`: On-trip driver ping percentage\n"
            "- `demand_forecast(zone)`: 30-min ML demand predictions\n"
            "- `kpi_summary(date)`: Completed trips, revenue (INR), ETAs"
        )

        if st.button("🗑️ Clear Chat History"):
            st.session_state.chat_history = []
            st.session_state.pop("gemini_chat", None)
            st.session_state.pop("gemini_client", None)
            st.rerun()

    if not api_key:
        st.warning("🔑 **Gemini API Key Required**")
        st.info(
            "Please enter your Gemini API Key in the sidebar or add `GEMINI_API_KEY=your_key` "
            "to your `.env` file to chat with the assistant."
        )
        return

    # Initialize chat history
    st.session_state.setdefault("chat_history", [])

    # Re-initialize Gemini chat if key or model changed
    if (
        "gemini_chat" not in st.session_state
        or "gemini_client" not in st.session_state
        or st.session_state.get("active_key") != api_key
        or st.session_state.get("active_model") != model_name
    ):
        try:
            client, chat, chosen_model = init_chat_session(api_key, model_name)
            st.session_state.gemini_client = client
            st.session_state.gemini_chat = chat
            st.session_state.active_key = api_key
            st.session_state.active_model = chosen_model
        except Exception as exc:
            st.error(f"Failed to initialize Gemini client: {exc}")
            return

    # Render previous conversation
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    # Example question chips when chat is empty
    if not st.session_state.chat_history:
        st.markdown("**Quick Prompts:**")
        cols = st.columns(3)
        prompt_chosen = None
        if cols[0].button("🔥 Top 5 Surge Zones"):
            prompt_chosen = "Which 5 zones have the highest surge multiplier right now?"
        if cols[1].button("📈 Hitech City Forecast"):
            prompt_chosen = "What is the demand forecast for Hitech City over the next 30 minutes?"
        if cols[2].button("📊 Operations KPIs"):
            prompt_chosen = "Show me the latest platform KPI summary including revenue and driver utilisation."
    else:
        prompt_chosen = None

    # Handle chat input
    user_query = st.chat_input("Ask about surge, forecasts, KPIs, or driver utilisation...") or prompt_chosen

    if user_query:
        st.session_state.chat_history.append({"role": "user", "content": user_query})
        with st.chat_message("user"):
            st.write(user_query)

        with st.chat_message("assistant"):
            with st.spinner("Executing operational queries via Gemini..."):
                try:
                    response = st.session_state.gemini_chat.send_message(user_query)
                    answer = extract_response_text(response)
                except Exception as exc:
                    answer = f"⚠️ **Gemini Error:** {exc}\n\n*Please verify your API key and network connection.*"

            st.write(answer)

        st.session_state.chat_history.append({"role": "assistant", "content": answer})



if __name__ == "__main__":
    main()
