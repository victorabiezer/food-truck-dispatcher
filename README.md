# Food Truck Dispatcher
**Victor Chairez | CSCI-3334 | Spring 2026**

A concurrent task dispatcher simulation built in Rust.

---

## The Story

A small food truck unexpectedly blew up in popularity and became
overwhelmed by customer requests. This system is the backend that
saved the day -- routing orders, managing the crew, and giving the
owner a live dashboard to watch it all happen in real time.

---

## How to Build and Run

```bash
git clone https://github.com/victorabiezer/food-truck-dispatcher
cd food-truck-dispatcher
cargo run
```

---

## What You Will See

Two simulations run back to back automatically:

- FIFO: orders served in strict arrival order, no smart decisions
- Optimized: CPU cap enforcement plus priority aging

---

## Design Summary

- Generator thread releases one task every 20ms into the correct queue
- Two queues: IO queue for wait-heavy tasks, CPU queue for compute-heavy tasks
- 8 worker threads process orders concurrently
- Monitor thread samples CPU usage and worker activity every 10ms
- Arc and Mutex protect all shared state across threads
- Fixed random seed (42) ensures reproducible results every run

---

## Experiments

Experiment A (FIFO): strict arrival order, no smart scheduling.
Workers blindly grab the next task without checking system load.

Experiment B (Optimized): CPU cap enforcement prevents kitchen
overload. Priority aging ensures no customer waits forever --
the longer you wait, the sooner you get served.

Key finding: Optimized reduced max wait time compared to FIFO,
preventing customer starvation under load. Total runtime is
mathematically bounded by worker capacity at ~25,000ms regardless
of scheduling strategy.

---

## Tool Use Disclosure

Tools used: Claude (Anthropic) as a coding assistant

Advice accepted: Using Arc<Mutex<>> to safely share queues across
worker threads

Advice I had to fix: Initial optimization used CPU-first dispatch
which showed no meaningful difference. I redesigned it to use CPU
cap enforcement combined with priority aging, which produced
measurable improvements in max wait time and fairness.

---

Built with Rust | CSCI-3334 Spring 2026