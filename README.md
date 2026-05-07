# Food Truck Dispatcher

A concurrent task dispatcher simulation built in Rust for CSCI-3334.

## What it does

Simulates a food truck backend overwhelmed by customer orders. 
1000 orders arrive every 20ms and are routed to two queues 
(IO and CPU), processed by 8 concurrent workers, and monitored 
by a live dashboard thread.

## How to build and run

```bash
cargo build
cargo run
```

## What you will see

Two simulations run back to back:
- FIFO: orders served in strict arrival order
- Optimized: CPU cap enforcement + priority aging

## Design summary

- Generator thread releases one task every 20ms
- Two queues: IO queue and CPU queue
- 8 worker threads process tasks concurrently
- Monitor thread samples CPU usage every 10ms
- Arc and Mutex protect all shared state

## Experiments

Experiment A (FIFO): strict arrival order, no smart scheduling
Experiment B (Optimized): CPU cap enforcement prevents kitchen 
overload, priority aging ensures no customer waits forever

Key finding: Optimized reduced max wait time by ~30% compared 
to FIFO, preventing customer starvation under load.

## Tool Use Disclosure

Tools used: Claude (Anthropic) as a coding assistant

Advice accepted: Using Arc<Mutex<>> to safely share queues 
across worker threads

Advice I had to fix: Initial optimization used CPU-first 
dispatch which showed no meaningful difference. I redesigned 
it to use CPU cap enforcement combined with priority aging, 
which produced measurable improvements in max wait time.