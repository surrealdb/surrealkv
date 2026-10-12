//! The canary: tests that make "encrypted at rest" a claim that can fail.
//!
//! `scan` walks a directory for markers and is tested on its own, `files` checks what a scan for
//! markers cannot and takes the image of a directory, and `matrix` runs a database through every
//! place that its data can be.

mod files;
mod matrix;
mod refusal;
mod scan;
