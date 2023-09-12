//! Defines the functions for spawning async tasks.
//! # Transaction example
//! We can spawn a transaction using the following code:
//! ```
//! use async_runtime::spawning::spawn_transaction;
//! use futures_lite::future;
//! 
//! async fn transaction() {
//!    // Do some async work here
//! }
//! 
//! let task_handle = spawn_transaction(transaction());
//! let outcome = future::block_on(task_handle);
//! ```
//! # Commit example
//! We can spawn a commit using the following code:
//! ```
//! use async_runtime::spawning::send_to_commit_pipeline;
//! use futures_lite::future;
//! 
//! async fn commit() {
//!   // Do some async work here
//! }
//! 
//! let task_handle = send_to_commit_pipeline(commit());
//! let outcome = future::block_on(task_handle);
//! ```
//! # Background tasks
//! We can spawn a task and never block a thread to await for the result with the following:
//! ```
//! use async_runtime::spawning::spawn_transaction;
//! use futures_lite::future;
//! 
//! async fn transaction() {
//!    // Do some async work here
//! }
//! 
//! spawn_transaction(transaction()).detach();
//! ```
//! We can also spawn a commit task within the transaction task so we never have to await for the results and block threads for them. With our own custom poll
//! functions we can have a lot of control on how tasks are processed, when we drop them, or when we await for them.
use std::{future::Future, panic::catch_unwind, thread};

use async_task::{Runnable, Task};
use once_cell::sync::Lazy;


/// Spawns a new async task for a transaction and returns a handle to it. The number of threads processing transactions in a queue is
/// defined by the `THREAD_NUM` environment variable. If this variable is not set, then the default number of threads is 1.
/// 
/// # Arguments
/// * `future` - The future to be spawned. These can be async functions or structs that implement the Future trait and have their own poll function
/// which we can use to create our own async logic for blocking code that we want to run asynchronously.
/// 
/// # Returns
/// * A handle to the spawned task where you can block your current thread to await for the result.
pub fn spawn_transaction<F, T>(future: F) -> Task<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    static QUEUE: Lazy<flume::Sender<Runnable>> = Lazy::new(|| {
        // create the queue for the transaction threads
        let (tx, rx) = flume::unbounded::<Runnable>();

        // extract the number of threads we want to spawn for the transaction queue
        let thread_num: usize = match std::env::var("THREAD_NUM") {
            Ok(thread_num) => thread_num.parse::<usize>().unwrap(),
            Err(_) => 1,
        };

        // spawn the threads that will process the transactions from the queue
        for _ in 0..thread_num {
            let reciever = rx.clone();
            thread::spawn(move || {
                while let Ok(runnable) = reciever.recv() {
                    let _ = catch_unwind(|| runnable.run());
                }
            });
        }
        tx

    });

    // create the schedule function that will be used to send the task to the queue
    let schedule = |runnable| QUEUE.send(runnable).unwrap();

    // spawn the task and return the handle to it
    let (runnable, task) = async_task::spawn(future, schedule);

    // schedule the task on the queue (this will now be processed by one of the threads in the queue even if we do not wait for it)
    runnable.schedule();
    // return the handle to the task to block a thread to wait for a result
    return task

}


/// This is a special function for sending a task to the commit pipeline. This is used for sending the commit task to the commit pipeline.
/// There will only be one thread processing this queue. This is because we want to ensure that the commit task is processed in the order.
/// 
/// # Arguments
/// * `future` - The future to be spawned. These can be async functions or structs that implement the Future trait and have their own poll function which
/// can help us with the ordering or ditching of commits if we need to.
/// 
/// # Returns
/// * A handle to the spawned task where you can block your current thread to await for the result.
pub fn send_to_commit_pipeline<F, T>(future: F) -> Task<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    static QUEUE: Lazy<flume::Sender<Runnable>> = Lazy::new(|| {
        let (tx, rx) = flume::unbounded::<Runnable>();

        thread::spawn(move || {
            while let Ok(runnable) = rx.recv() {
                let _ = catch_unwind(|| runnable.run());
            }
        });
        tx

    });

    let schedule = |runnable| QUEUE.send(runnable).unwrap();
    let (runnable, task) = async_task::spawn(future, schedule);

    runnable.schedule();
    return task
}
