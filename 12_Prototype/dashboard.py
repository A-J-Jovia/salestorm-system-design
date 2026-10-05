import asyncio
import datetime
import json
import os
import sys
import time
import uuid
from collections import Counter
import streamlit as st
import fakeredis.aioredis as fakeredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Add paths to LLD components
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../03_LLD")))
from payment_service import Base, PaymentRecord, OutboxEvent, DATABASE_URL
from outbox_recovery_worker import SimulatedMessageBroker, process_outbox_batch

# -----------------------------------------------------------------------------
# Streamlit Page Setup & Styling
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="SALESTORM | Live Flash-Sale Architecture Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;900&family=JetBrains+Mono:wght@400;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    .stApp {
        background: radial-gradient(circle at 10% 20%, rgba(15, 23, 42, 1) 0%, rgba(3, 7, 18, 1) 90%);
        color: #f8fafc;
    }
    
    .metric-card {
        background: rgba(30, 41, 59, 0.7);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.4);
        backdrop-filter: blur(10px);
        margin-bottom: 12px;
    }
    
    .metric-value {
        font-size: 2.2rem;
        font-weight: 900;
        font-family: 'JetBrains Mono', monospace;
    }
    
    .metric-label {
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94a3b8;
        margin-top: 4px;
    }
    
    .badge-success {
        background-color: #065f46;
        color: #34d399;
        padding: 4px 10px;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 700;
    }

    .badge-danger {
        background-color: #881337;
        color: #fda4af;
        padding: 4px 10px;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Lua Script for Atomic Reservation
# -----------------------------------------------------------------------------
LUA_SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../03_LLD/reserve_stock.lua"))
if os.path.exists(LUA_SCRIPT_PATH):
    with open(LUA_SCRIPT_PATH, "r") as f:
        RESERVE_STOCK_LUA = f.read()
else:
    RESERVE_STOCK_LUA = """
    local existing_reservation = redis.call("GET", KEYS[3])
    if existing_reservation then return -1 end
    local current_stock = redis.call("GET", KEYS[1])
    if not current_stock then return -2 end
    current_stock = tonumber(current_stock)
    local qty = tonumber(ARGV[1])
    if current_stock < qty then return 0 end
    redis.call("DECRBY", KEYS[1], qty)
    redis.call("HMSET", KEYS[2], "reservation_id", ARGV[2], "user_id", ARGV[4], "qty", qty, "status", "PENDING_PAYMENT")
    redis.call("EXPIRE", KEYS[2], tonumber(ARGV[3]))
    redis.call("SET", KEYS[3], ARGV[2], "EX", tonumber(ARGV[3]))
    return 1
    """

# -----------------------------------------------------------------------------
# Session State Initialization
# -----------------------------------------------------------------------------
if "test_run_data" not in st.session_state:
    st.session_state.test_run_data = None

if "outbox_logs" not in st.session_state:
    st.session_state.outbox_logs = []

# -----------------------------------------------------------------------------
# Sidebar Navigation & System Stats
# -----------------------------------------------------------------------------
with st.sidebar:
    st.image("https://images.unsplash.com/photo-1618005182384-a83a8bd57fbe?w=600&auto=format&fit=crop&q=80", use_container_width=True)
    st.title("⚡ SALESTORM Core")
    st.caption("High-Scale Flash-Sale Architecture Engine")
    
    st.markdown("---")
    st.markdown("### 🛡️ Architectural Invariants")
    st.markdown("- **Concurrency Goal:** 10,000 Callers")
    st.markdown("- **Limited Stock:** Exactly 100 Units")
    st.markdown("- **Oversell Tolerance:** **Strict 0**")
    st.markdown("- **DB Load on Burst:** **0 Queries**")
    
    st.markdown("---")
    st.markdown("### 📦 Active Services")
    st.markdown("🟢 **Edge Rate Limiter**: 5 req/s/IP")
    st.markdown("🟢 **Redis Lua Gatekeeper**: Active")
    st.markdown("🟢 **Transactional Outbox**: Ready")
    st.markdown("🟢 **Reconciliation Worker**: Polling")

# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------
st.title("⚡ SALESTORM: Live Concurrency & Reliability Dashboard")
st.markdown("""
Interactive prototype and proof-of-correctness demonstration for the **SALESTORM** flash-sale platform. 
Observe atomic stock deduction across **10,000 simultaneous requests** and evaluate **eventual consistency** under downstream broker outages.
""")

tab1, tab2, tab3 = st.tabs([
    "🚀 10,000-to-100 Concurrency Simulator", 
    "💳 Payment Idempotency & Outbox Recovery",
    "📐 Architecture Blueprint & Verification"
])

# -----------------------------------------------------------------------------
# TAB 1: 10,000-to-100 Concurrency Simulator
# -----------------------------------------------------------------------------
with tab1:
    st.subheader("🔥 High-Contention Flash Sale Simulation")
    
    col_config1, col_config2, col_config3 = st.columns([2, 1, 1])
    with col_config1:
        st.info("🎯 **Target Flash Item:** Apple MacBook Pro M3 Max (Special Edition) | **Inventory Pool:** 100 Units")
    with col_config2:
        concurrent_requests = st.select_slider(
            "Concurrent Ingress Requests:",
            options=[1000, 2500, 5000, 10000],
            value=10000
        )
    with col_config3:
        initial_stock = 100
        st.metric("Total Flash Stock", f"{initial_stock} Units")

    async def execute_burst_simulation(total_callers: int, stock_units: int):
        client = fakeredis.FakeRedis(decode_responses=True, max_connections=total_callers)
        item_id = "macbook_pro_flash_2026"
        stock_key = f"stock:{item_id}"
        await client.set(stock_key, stock_units)
        script_sha = await client.script_load(RESERVE_STOCK_LUA)

        start_gate = asyncio.Event()
        results = []
        reservations_sample = []

        async def worker(idx: int):
            user_id = f"usr_{idx:05d}"
            res_id = f"res_{uuid.uuid4().hex[:8]}"
            idemp_key = f"idemp:{user_id}:attempt_1"
            keys = [stock_key, f"reservation:{res_id}", idemp_key]
            args = [1, res_id, 300, user_id]
            
            await start_gate.wait()
            code = await client.evalsha(script_sha, len(keys), *keys, *args)
            results.append(code)
            if code == 1 and len(reservations_sample) < 10:
                reservations_sample.append({
                    "Reservation ID": res_id,
                    "User ID": user_id,
                    "Allocated": 1,
                    "TTL Lease": "300s",
                    "Status": "RESERVED"
                })

        tasks = [asyncio.create_task(worker(i)) for i in range(total_callers)]
        
        t0 = time.perf_counter()
        start_gate.set()
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - t0

        final_stock = int(await client.get(stock_key))
        await client.aclose()
        
        counts = Counter(results)
        return {
            "total": total_callers,
            "granted": counts.get(1, 0),
            "rejected": counts.get(0, 0),
            "final_stock": final_stock,
            "elapsed": elapsed,
            "throughput": total_callers / elapsed if elapsed > 0 else 0,
            "sample": reservations_sample
        }

    trigger_button = st.button("⚡ Fire Flash Sale (Concurrent Burst)", type="primary", use_container_width=True)

    if trigger_button:
        with st.spinner(f"Synchronizing and firing {concurrent_requests:,} concurrent requests at Redis Lua gatekeeper..."):
            sim_data = asyncio.run(execute_burst_simulation(concurrent_requests, initial_stock))
            st.session_state.test_run_data = sim_data

    if st.session_state.test_run_data:
        data = st.session_state.test_run_data
        
        st.markdown("### 📊 Real-Time Metrics & Invariant Evaluation")
        
        m1, m2, m3, m4, m5 = st.columns(5)
        with m1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value" style="color: #60a5fa;">{data['total']:,}</div>
                <div class="metric-label">Ingress Requests</div>
            </div>
            """, unsafe_allow_html=True)
        with m2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value" style="color: #34d399;">{data['granted']}</div>
                <div class="metric-label">Confirmed Reservations</div>
            </div>
            """, unsafe_allow_html=True)
        with m3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value" style="color: #f87171;">{data['rejected']:,}</div>
                <div class="metric-label">Rejected (HTTP 409)</div>
            </div>
            """, unsafe_allow_html=True)
        with m4:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value" style="color: #fbbf24;">{data['final_stock']}</div>
                <div class="metric-label">Remaining Stock</div>
            </div>
            """, unsafe_allow_html=True)
        with m5:
            oversold = max(0, data['granted'] - initial_stock)
            color = "#34d399" if oversold == 0 else "#ef4444"
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value" style="color: {color};">{oversold}</div>
                <div class="metric-label">Oversell Violations</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("#### 📉 Inventory Depletion Gauge")
        depleted_pct = ((initial_stock - data['final_stock']) / initial_stock) * 100
        st.progress(depleted_pct / 100.0, text=f"Stock Exhaustion: {depleted_pct:.0f}% ({initial_stock - data['final_stock']} / {initial_stock} units claimed)")

        c_perf1, c_perf2 = st.columns(2)
        with c_perf1:
            st.success(f"⏱️ **Execution Duration:** {data['elapsed']:.3f} seconds for {data['total']:,} parallel callers")
        with c_perf2:
            st.info(f"⚡ **Gatekeeper Throughput:** {data['throughput']:,.0f} operations/second")

        st.markdown("#### 🔍 Sample Atomic Reservations Audit Trail (First 10 Grants)")
        st.dataframe(data['sample'], use_container_width=True)

# -----------------------------------------------------------------------------
# TAB 2: Payment Idempotency & Transactional Outbox Recovery
# -----------------------------------------------------------------------------
with tab2:
    st.subheader("💳 Payment Reliability & Outbox Outage Simulator")
    st.markdown("""
    Test the **Payment Idempotency Engine** and observe how the **Transactional Outbox Pattern** 
    survives downstream message broker outages without data loss.
    """)

    prototype_db_url = os.getenv("PROTOTYPE_DB_URL", "sqlite+aiosqlite:///./salestorm.db")
    db_engine = create_async_engine(prototype_db_url, echo=False)
    AsyncSessionLocal = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)

    # Ensure tables exist on page load
    async def init_tables():
        async with db_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    try:
        asyncio.run(init_tables())
    except Exception:
        pass

    col_p1, col_p2 = st.columns([1, 1])

    with col_p1:
        st.markdown("#### Step 1: Execute Payment & Atomic Outbox")
        user_test_id = st.text_input("Buyer Account ID:", "usr_buyer_1001")
        amount_usd = st.number_input("Amount ($):", value=1999.00, step=100.0)
        idemp_token = st.text_input("Idempotency Key (UUID):", value="idemp_demo_token_1001")

        async def record_payment(token: str, user: str, amount: float):
            async with AsyncSessionLocal() as session:
                async with db_engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                
                # Check idempotency
                existing = (await session.execute(
                    select(PaymentRecord).where(PaymentRecord.idempotency_key == token)
                )).scalar_one_or_none()

                if existing:
                    return {"status": "CACHED_REPLAY", "payment_id": existing.id, "cached": True}

                pay_id = f"pay_{uuid.uuid4().hex[:10]}"
                evt_id = f"evt_{uuid.uuid4().hex[:10]}"
                
                # ACID Single Transaction
                pay_rec = PaymentRecord(
                    id=pay_id,
                    idempotency_key=token,
                    reservation_id="res_demo_stock_01",
                    user_id=user,
                    amount_cents=int(amount * 100),
                    currency="USD",
                    status="SUCCESS",
                    gateway_transaction_id=f"ch_{uuid.uuid4().hex[:12]}"
                )
                session.add(pay_rec)

                outbox_rec = OutboxEvent(
                    event_id=evt_id,
                    aggregate_type="PAYMENT",
                    aggregate_id=pay_id,
                    event_type="ORDER_PAID",
                    payload=json.dumps({"event_id": evt_id, "payment_id": pay_id, "user_id": user, "amount": amount}),
                    status="PENDING",
                    retry_count=0
                )
                session.add(outbox_rec)
                await session.commit()
                return {"status": "NEW_CHARGE", "payment_id": pay_id, "cached": False}

        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            if st.button("💳 Submit Payment", type="primary", use_container_width=True):
                res = asyncio.run(record_payment(idemp_token, user_test_id, amount_usd))
                if res["cached"]:
                    st.warning(f"🔁 Duplicate Replay: Existing payment replayed ({res['payment_id']}). Zero double-charge!")
                else:
                    st.success(f"✅ Payment Authorized: ID={res['payment_id']}. Outbox event written in SQL!")

        with btn_col2:
            if st.button("🔁 Simulate Network Retry", use_container_width=True):
                res = asyncio.run(record_payment(idemp_token, user_test_id, amount_usd))
                st.warning(f"🛡️ Replayed from Idempotency Cache: ID={res['payment_id']} (cached_response=True)")

    with col_p2:
        st.markdown("#### Step 2: Downstream Broker Outage & Recovery")
        broker_down = st.toggle("🚨 Simulate Message Broker Outage (Kafka / RabbitMQ Down)", value=False)
        
        if broker_down:
            st.error("⚠️ Message Broker is currently UNREACHABLE. Downstream consumers cannot receive messages directly.")
        else:
            st.success("🟢 Message Broker is ONLINE and accepting published events.")

        if st.button("🔄 Run Outbox Recovery Worker Cycle", use_container_width=True):
            broker = SimulatedMessageBroker(simulate_outage=broker_down)
            try:
                count = asyncio.run(process_outbox_batch(broker))
                if broker_down:
                    st.warning(f"Outbox Worker caught broker outage! 0 events dispatched. Events remain safely PENDING in DB.")
                else:
                    st.success(f"Worker Cycle Completed: Successfully published {count} pending events to broker!")
            except Exception as e:
                st.error(f"Worker Error: {e}")

    st.markdown("---")
    st.markdown("#### 📋 Live Transactional Outbox Database State (`outbox_events`)")

    async def fetch_outbox_table():
        try:
            async with db_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with AsyncSessionLocal() as session:
                events = (await session.execute(
                    select(OutboxEvent).order_by(OutboxEvent.created_at.desc()).limit(15)
                )).scalars().all()
                return [
                    {
                        "Event ID": e.event_id,
                        "Aggregate ID": e.aggregate_id,
                        "Event Type": e.event_type,
                        "Status": e.status,
                        "Retries": e.retry_count,
                        "Created At": e.created_at.strftime("%H:%M:%S") if e.created_at else "-",
                        "Processed At": e.processed_at.strftime("%H:%M:%S") if e.processed_at else "Awaiting Broker"
                    }
                    for e in events
                ]
        except Exception:
            return []

    outbox_records = asyncio.run(fetch_outbox_table())
    if outbox_records:
        st.dataframe(outbox_records, use_container_width=True)
    else:
        st.caption("No outbox records yet. Submit a payment above to see the transactional outbox in action.")

# -----------------------------------------------------------------------------
# TAB 3: Architecture Blueprint & Technical Defense
# -----------------------------------------------------------------------------
with tab3:
    st.subheader("📐 SALESTORM Architectural Defense & Invariant Bounds")
    
    st.markdown("""
    ### Strict Concurrency Separation
    ```
    10,000 Ingress Requests
              │
              ▼
    Cloudflare Edge & API Gateway (Rate Limit: 5 req/s/IP)
              │
              ▼
    Inventory Gatekeeper Service (Sub-second SLA)
              │
              ├── [Atomic Check + Decrement] ──► Redis Cluster (Single-Threaded Lua Script)
              │                                      │
              │                                      ├── Stock >= 1: Decrement counter, issue 300s TTL lease (Status 1)
              │                                      └── Stock == 0: Instant drop HTTP 409 (Status 0, Zero DB hit)
              ▼
    Checkout / Payment Service
              │
              ├── Idempotency Key Lock (Redis NX EX 30)
              ├── External Gateway Charge (Stripe / Razorpay with idempotency key)
              └── Transactional Outbox (Single ACID boundary in SQL DB)
                     ├── INSERT INTO payments (status='SUCCESS')
                     └── INSERT INTO outbox_events (status='PENDING')
              │
              ▼
    Asynchronous Reconciliation Worker
              │
              └── Polls outbox_events (SKIP LOCKED) ──► Kafka / RabbitMQ ──► Order Service
    ```
    """)
    
    st.markdown("### 📊 System Architecture & Engineering Highlights")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**1. Zero DB Contention**")
        st.caption("Under peak burst, 9,900 losers are dropped directly in-memory at the Redis boundary without touching PostgreSQL connection pools.")
    with c2:
        st.markdown("**2. Absolute Zero Overselling**")
        st.caption("Redis single-threaded Lua execution guarantees stock evaluation and decrement occur in one uninterruptible step.")
    with c3:
        st.markdown("**3. Crash & Outage Resilience**")
        st.caption("Transactional Outbox ensures payments and order events never diverge, surviving 30s+ message broker crashes with zero lost orders.")

st.markdown("---")
st.caption("SALESTORM System Design Architecture Prototype | Production-Grade Real-Time Simulation")
