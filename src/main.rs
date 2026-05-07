use std::thread;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::fs::File;
use std::io::Write;

// =====================================
// BACKEND SYSTEM (EXPLAINED)
// a small food truck blew up in popularity and needed a smarter
// way to handle the rush of incoming customer orders.
// this system simulates how orders are received, queued, and processed
// by a crew of 8 workers — and compares two scheduling strategies.
// =====================================

// every order is either a simple wait-heavy request (IO)
// or a complex compute-heavy request (CPU)
#[derive(PartialEq, Clone)]
enum TaskKind
{
    IO,  // menu lookups, SMS notifications (not real), simple pickups .... mostly waiting
    CPU, // custom orders, price calculations, receipt generation .... mostly computing
}



// a single customer order coming into the system
#[derive(Clone)]
struct Task
{
    id: u32,           // the order number — every customer gets a unique ticket
    arrival_time: u64, // when the customer showed up (ms from opening time)
    kind: TaskKind,    // is this a simple or complex order?
    duration: u64,     // how long it takes to fulfill (200ms per order)
    priority: u32,     // urgency level — higher means serve sooner (aging system ready for future)
}



// once an order is completed, we record how it went
struct CompletedTask
{
    kind: TaskKind,       // was it IO or CPU)
    wait_time: u64,       // how long did the customer wait in line
    turnaround_time: u64, // total time from arrival to completion
}



// Bbefore the truck opens, we  1000 customer orders are generated 
// 70% are simple IO orders, 30% are complex CPU orders
// orders are spaced 20ms apart (based on instryctions) simulating a steady stream of customers
fn make_tasks() -> Vec<Task>
{
    let mut tasks = Vec::new();
    for i in 0..1000u32
    {
        tasks.push(Task
        {
            id: i,
            arrival_time: i as u64 * 20, // one customer every 20ms
            kind: if rand::random::<u8>() < 179 { TaskKind::IO } else { TaskKind::CPU }, // 70/30 split
            duration: 200, // every order takes 200ms to fulfill
            priority: 0,   // everyone starts equal — aging can change this later
        });
    }
    tasks
}



fn run_simulation(mode: &str, arrival_interval: u64)
{
    let label = if mode == "fifo" { "FIFO simulation" } else { "Optimized simulation" };
    println!("== {} ==", label);
    println!("1000 tasks, 70% IO / 30% CPU, 8 workers, cap 100%\n");

    let tasks = make_tasks();

    // the two lines outside the truck (one for simple orders, one for complex)
    // wrapped in Arc<Mutex<>> so all 8 workers can safely share access
    let io_queue: Arc<Mutex<VecDeque<Task>>> = Arc::new(Mutex::new(VecDeque::new()));
    let cpu_queue: Arc<Mutex<VecDeque<Task>>> = Arc::new(Mutex::new(VecDeque::new()));

    // the kitchen load tracker (so how many workers are handling each type NOW)
    let active_io: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    let active_cpu: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    // the completed orders board (every finished order gets recorded here)
    let completed: Arc<Mutex<Vec<CompletedTask>>> = Arc::new(Mutex::new(Vec::new()));

    // the owner's dashboard log, sampled every 10ms
    let monitor_data: Arc<Mutex<Vec<(u64, f64, u32)>>> = Arc::new(Mutex::new(Vec::new()));

    // signal to tell the dashboard to stop recording when service is over
    let all_done: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let sim_start = Instant::now();

    // THE CUSTOMER ARRIVAL THREAD 
    // one customer walks up every 20ms and joins the right line
    // this runs concurrently (customers keep arriving while workers are busy)
    let io_q_gen = Arc::clone(&io_queue);
    let cpu_q_gen = Arc::clone(&cpu_queue);
    let gen_handle = thread::spawn(move ||
    {
        for task in tasks
        {
            thread::sleep(Duration::from_millis(arrival_interval));
            match task.kind
            {
                TaskKind::IO => io_q_gen.lock().unwrap().push_back(task),
                TaskKind::CPU => cpu_q_gen.lock().unwrap().push_back(task),
            }
        }
    });

    // THE OWNER'S DASHBOARD THREAD 
    // every 10ms, the owner glances at the kitchen:
    // how busy is the CPU? how many workers are active?
    // all snapshots get saved to monitor_log.csv
    let mon_io = Arc::clone(&active_io);
    let mon_cpu = Arc::clone(&active_cpu);
    let mon_data = Arc::clone(&monitor_data);
    let mon_done = Arc::clone(&all_done);
    let monitor_handle = thread::spawn(move ||
    {
        loop
        {
            thread::sleep(Duration::from_millis(10));
            let io_a = *mon_io.lock().unwrap();
            let cpu_a = *mon_cpu.lock().unwrap();
            // IO tasks use 10% CPU, CPU tasks use 35% CPU
            let cpu_usage = ((io_a as f64 * 10.0) + (cpu_a as f64 * 35.0)).min(100.0);
            let workers_active = io_a + cpu_a;
            let time_ms = sim_start.elapsed().as_millis() as u64;
            mon_data.lock().unwrap().push((time_ms, cpu_usage, workers_active));
            if *mon_done.lock().unwrap()
            {
                break;
            }
        }
    });

    // THE CREW (8 WORKER) ===
    // each worker is a crew member inside the truck
    // they keep grabbing the next order and fulfilling it until all 1000 are done
    let mut worker_handles = vec![];

    for _worker_id in 0..8u32
    {
        let io_q = Arc::clone(&io_queue);
        let cpu_q = Arc::clone(&cpu_queue);
        let active_io_w = Arc::clone(&active_io);
        let active_cpu_w = Arc::clone(&active_cpu);
        let completed_w = Arc::clone(&completed);
        let mode_str = mode.to_string();

        let handle = thread::spawn(move ||
        {
            loop
            {
                // "clock out' when all 1000 orders are done
                if completed_w.lock().unwrap().len() >= 1000
                {
                    break;
                }

                let task = if mode_str == "fifo"
                {
                    // FIFO STRATEGY: serve whoever has been waiting longest
                    // IO line first, then CPU line — strict arrival order, no smart decisions
                    let mut io = io_q.lock().unwrap();
                    if let Some(t) = io.pop_front()
                    {
                        Some(t)
                    }
                    else
                    {
                        drop(io);
                        cpu_q.lock().unwrap().pop_front()
                    }
                }
                else
                {
                    // OPTIMIZED STRATEGY: SMART kitchen management
                    // first check how loaded the kitchen is right now
                    // if CPU usage is getting high, switch to IO tasks to prevent choking
                    // if there is room, pick whoever has been waiting the longest (priority aging)

                    // v1 commented out: CPU first blindly
                    // let mut cpu = cpu_q.lock().unwrap();
                    // if let Some(t) = cpu.pop_front() { Some(t) }
                    // else { drop(cpu); io_q.lock().unwrap().pop_front() }

                    // v2 commented out: pure priority aging without cap check
                    // caused no runtime difference because cap was never enforced

                    // v3 current: CPU cap enforcement + priority aging
                    let current_io = *active_io_w.lock().unwrap();
                    let current_cpu = *active_cpu_w.lock().unwrap();
                    let current_usage = (current_io as f64 * 10.0) + (current_cpu as f64 * 35.0);

                    let mut io = io_q.lock().unwrap();
                    let mut cpu = cpu_q.lock().unwrap();

                    if current_usage >= 65.0
                    {
                        // kitchen is getting full (route to a simple IO order instead)
                        if let Some(t) = io.pop_front()
                        {
                            Some(t)
                        }
                        else
                        {
                            cpu.pop_front()
                        }
                    }
                    else
                    {
                        // kitchen has room (serve whoever has waited the longest)
                        let now = sim_start.elapsed().as_millis() as u64;
                        let io_oldest = io.iter().enumerate()
                            .max_by_key(|(_, t)| now.saturating_sub(t.arrival_time))
                            .map(|(i, _)| i);
                        let cpu_oldest = cpu.iter().enumerate()
                            .max_by_key(|(_, t)| now.saturating_sub(t.arrival_time))
                            .map(|(i, _)| i);

                        match (io_oldest, cpu_oldest)
                        {
                            (Some(i), Some(j)) =>
                            {
                                let io_wait = now.saturating_sub(io[i].arrival_time);
                                let cpu_wait = now.saturating_sub(cpu[j].arrival_time);
                                if io_wait >= cpu_wait { io.remove(i) } else { cpu.remove(j) }
                            }
                            (Some(i), None) => io.remove(i),
                            (None, Some(j)) => cpu.remove(j),
                            (None, None) => None,
                        }
                    }
                };

                match task
                {
                    Some(t) =>
                    {
                        let now = sim_start.elapsed().as_millis() as u64;
                        // how long did this customer wait in line?
                        let wait_time = now.saturating_sub(t.arrival_time);

                        // worker clocks in on this order
                        match t.kind
                        {
                            TaskKind::IO => *active_io_w.lock().unwrap() += 1,
                            TaskKind::CPU => *active_cpu_w.lock().unwrap() += 1,
                        }

                        // fulfill the order (200ms of work)
                        thread::sleep(Duration::from_millis(t.duration));

                        // worker clocks out --> order complete
                        match t.kind
                        {
                            TaskKind::IO => *active_io_w.lock().unwrap() -= 1,
                            TaskKind::CPU => *active_cpu_w.lock().unwrap() -= 1,
                        }

                        let turnaround = wait_time + t.duration;
                        completed_w.lock().unwrap().push(CompletedTask
                        {
                            kind: t.kind,
                            wait_time,
                            turnaround_time: turnaround,
                        });
                    }
                    None =>
                    {
                        // no orders in either line yet --> worker waits a (1) moment!!!
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        });
        worker_handles.push(handle);
    }

    // wait for all customers to arrive
    gen_handle.join().unwrap();

    // wait for all workers to finish every order
    for h in worker_handles
    {
        h.join().unwrap();
    }

    let total_runtime = sim_start.elapsed().as_millis() as u64;

    // tell the dashboard to stop recording (service is over)
    *all_done.lock().unwrap() = true;
    monitor_handle.join().unwrap();

    // END OF DAY REPORT 
    let results = completed.lock().unwrap();
    let total_completed = results.len() as u32;
    let io_completed = results.iter().filter(|r| r.kind == TaskKind::IO).count() as u32;
    let cpu_completed = results.iter().filter(|r| r.kind == TaskKind::CPU).count() as u32;

    let avg_wait = results.iter().map(|r| r.wait_time as f64).sum::<f64>() / total_completed as f64;
    let avg_turnaround = results.iter().map(|r| r.turnaround_time as f64).sum::<f64>() / total_completed as f64;
    let max_wait = results.iter().map(|r| r.wait_time).max().unwrap_or(0);

    let samples = monitor_data.lock().unwrap();
    let sample_count = samples.len();
    let avg_cpu = samples.iter().map(|(_, c, _)| c).sum::<f64>() / sample_count as f64;
    let avg_active = samples.iter().map(|(_, _, a)| *a as f64).sum::<f64>() / sample_count as f64;

    // save the dashboard log to CSV for the repo
    {
        let mut file = File::create("monitor_log.csv").unwrap();
        writeln!(file, "time_ms,cpu_usage,workers_active").unwrap();
        for (t, c, a) in samples.iter()
        {
            writeln!(file, "{},{:.2},{}", t, c, a).unwrap();
        }
    }

    // print the end of day summary (how prof wanted it from the brightspace link) 
    println!("-- results --");
    println!("total runtime        : {} ms", total_runtime);
    println!("makespan             : {} ms", total_runtime.saturating_sub(3));
    println!("tasks completed      : {}  (IO={}, CPU={})", total_completed, io_completed, cpu_completed);
    println!("avg wait time        : {:.2} ms", avg_wait);
    println!("avg turnaround time  : {:.2} ms", avg_turnaround);
    println!("max wait time        : {} ms", max_wait);
    println!("avg CPU usage        : {:.2} %", avg_cpu);
    println!("avg workers active   : {:.2} / 8", avg_active);
    println!("monitor samples      : {}", sample_count);
    println!("monitor csv          : monitor_log.csv\n");
}

fn main()
{
    println!("Food truck is open!\n");
    // Experiment A: baseline FIFO --> serve orders strictly in arrival order
    run_simulation("fifo", 20);
    // Experiment B: optimized --> smart CPU cap enforcement + priority aging
    run_simulation("optimized", 20);
    println!("Food truck is closed! All orders complete.");
}



// ============================================================
// Mt original work before aligning and story telling (keeping it for reference and hstory of terminal runs and progress, 
// but the final version above is the one that includes all the metrics and monitoring)
// ============================================================


// use std::thread;
// use std::time::Duration;
// use std::collections::VecDeque;
// use std::sync::{Arc, Mutex};


// #[derive(PartialEq)]
// enum TaskKind 
// {
//     IO,
//     CPU,
// }

// struct Task
// {
//     id: u32, // id is a unique identifier for the task
//     arrival_time: u32, // arrival_time is the time at which the task arrives in the system
//     kind: TaskKind, // kind is the type of task, so is it IO or CPU
//     duration: u64, // duration is the time it takes to complete the task
//     priority: u32, // priority is the priority of the task --> "first come first serve"
// }


// // imprortat note: from 0 .. 255 --> 179 is 70% and 180 .. 255 is 30% 
// fn generate_tasks() -> Vec<Task> {
//     // first create an empty list
//     let mut tasks: Vec<Task> = Vec::new();
//     // then loop 1000 times
//     for i in 0..1000 {
//         // create a task
//         let task = Task {
//             id: i,
//             arrival_time: i * 20, // increases by 20 x task 
//             kind: if rand::random::<u8>() < 179 { 
//                 TaskKind::IO   // 70% of the time 
//             } else { 
//                 TaskKind::CPU  // 30% of the time
//             },
//             duration: 200,    // fixed at 200ms per professor notes
//             priority: 0,      // everyone starts equal
//         };
//         // add the task to the list
//         tasks.push(task);
//     }
//     // return the full list
//     tasks
// }




// fn main() 
// {
//     let tasks = generate_tasks(); // generate the tasks and store them in a variable called tasks
//     println!("Food truck is open!"); // print message 
//     println!("Total tasks queued: {}", tasks.len()); // print the total number of tasks generated to confirm that they are being created correctly
    
//     for task in tasks.iter().take(3) // take the first 3 tasks to print out 
//     { 
//         thread::sleep(Duration::from_millis(500)); // simulate time 
//         println!("Order #{} | {}", // print 
//             task.id, 
//             match task.kind { TaskKind::IO => "IO", TaskKind::CPU => "CPU" },
//         );
//     }

//     // create empty IO queue
//     let mut io_queue: VecDeque<Task> = VecDeque::new();

//     // create empty CPU queue
//     let mut cpu_queue: VecDeque<Task> = VecDeque::new();

//     // for each task in tasks if io --> push to o queue, if cpu --> push to cpu queue 
//     for task in tasks 
//     {
//         if task.kind == TaskKind::IO 
//         {
//             io_queue.push_back(task);
//         } else 
//         {
//             cpu_queue.push_back(task);
//         }
//     }

//     println!("IO queue: {} orders", io_queue.len());
//     println!("CPU queue: {} orders", cpu_queue.len());

//     // wrap both queues in Arc<Mutex<>> so all 8 workers can safely share them
//     let io_queue = Arc::new(Mutex::new(io_queue));
//     let cpu_queue = Arc::new(Mutex::new(cpu_queue));

//     let mut handles = vec![]; // list to keep track of all worker threads

//     for worker_id in 0..8 {
//         // clone the Arc pointers so each worker gets its own reference
//         // think of it like giving each crew member their own copy of the queue ticket system
//         let io_q = Arc::clone(&io_queue);
//         let cpu_q = Arc::clone(&cpu_queue);

//         let handle = thread::spawn(move || {
//             loop {
//                 // try to grab an order from IO queue first
//                 let task = {
//                     let mut queue = io_q.lock().unwrap(); // lock the queue (one person at the window)
//                     if let Some(t) = queue.pop_front() {
//                         Some(t) // grabbed an IO order
//                     } else {
//                         let mut queue = cpu_q.lock().unwrap(); // try CPU queue instead
//                         queue.pop_front() // grab a CPU order or nothing
//                     }
//                 }; // lock is released here automatically

//                 match task {
//                     Some(t) => {
//                         println!("Worker {} cooking order #{} | {}", 
//                             worker_id, 
//                             t.id,
//                             match t.kind { TaskKind::IO => "IO", TaskKind::CPU => "CPU" }
//                         );
//                         thread::sleep(Duration::from_millis(t.duration)); // simulate cooking time
//                     }
//                     None => {
//                         // no orders left, worker goes home
//                         println!("Worker {} is done for the day!", worker_id);
//                         break;
//                     }
//                 }
//             }
//         });

//         handles.push(handle); // save the thread handle
//     }

//     // wait for ALL workers to finish before closing the truck
//     for handle in handles {
//         handle.join().unwrap();
//     }

//     println!("Food truck is closed! All orders complete.");
// }       





// // first cargo run terminal response 
// // Food truck is open!

// // second cargo run after adding generate_tasks() response 
// // error: could not compile `food-truck-dispatcher` (bin "food-truck-dispatcher") due to 1 previous error
// // second attempt: 
// // Food truck is open!
// // ^^ this is before updaing main to call the function!

// // third cargo run after updaying main to print out and see that the orders are not empty ! (something i couldnt see from  prior print)
// // Food truck is open!
// // Total tasks queued: 1000
// // Order #0 | IO
// // Order #1 | IO
// // Order #2 | CPU

// // fourth cargo run after adding the queues and pushing the tasks into IO/CPU queues
// // Food truck is open!
// // Total tasks queued: 1000
// // Order #0 | IO
// // Order #1 | IO
// // Order #2 | CPU
// // IO queue: 709 orders
// // CPU queue: 291 orders

// // fifth cargo run after adding the worker threads to process the orders (lengthy...) 
// // Food truck is open!
// // Total tasks queued: 1000
// // Order #0 | IO
// // Order #1 | IO
// // Order #2 | IO
// // IO queue: 699 orders
// // CPU queue: 301 orders
// // Worker 0 cooking order #0 | IO
// // Worker 1 cooking order #1 | IO
// // Worker 3 cooking order #4 | IO
// // Worker 2 cooking order #2 | IO
// // ......
// // .....
// // ...
// // .
// // Worker 4 cooking order #999 | CPU
// // Worker 6 is done for the day!
// // Worker 5 is done for the day!
// // Worker 2 is done for the day!
// // Worker 1 is done for the day!
// // Worker 0 is done for the day!
// // Worker 3 is done for the day!
// // Worker 7 is done for the day!
// // Worker 4 is done for the day!
// // Food truck is closed! All orders complete.