use criterion::{criterion_group, criterion_main, Criterion};

use rand::Rng;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;

fn create_large_file<P: AsRef<Path>>(path: P, size: u64) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;

    let buffer = vec![0u8; 1024 * 1024]; // 1 MB buffer
    let mut written = 0;

    while written < size {
        let to_write = std::cmp::min(size - written, buffer.len() as u64);
        file.write_all(&buffer[..to_write as usize])?;
        written += to_write;
    }

    Ok(())
}

const ADVICE_RANDOM: i32 = 1;
const ADVICE_SEQUENTIAL: i32 = 2;

#[cfg(target_os = "macos")]
fn advise_file(fd: i32, advice: i32) -> io::Result<()> {
    use libc::{fcntl, F_NOCACHE, F_RDAHEAD};
    match advice {
        ADVICE_RANDOM => {
            // Enable read-ahead
            unsafe {
                let result = fcntl(fd, F_NOCACHE, 1);
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(result))
                }
            }
        }
        ADVICE_SEQUENTIAL => {
            // Disable read-ahead
            unsafe {
                let result = fcntl(fd, F_RDAHEAD, 1);
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(result))
                }
            }
            // Additionally, you might consider enabling F_NOCACHE to avoid caching reads,
            // which can be beneficial for random access patterns to prevent cache pollution.
            // However, use this with caution as it can have other performance implications.
            // unsafe { fcntl(fd, F_NOCACHE, 1); }
        }
        _ => Ok(()), // Handle all other cases without doing anything
    }
}

#[cfg(target_os = "linux")]
fn advise_file(fd: i32, advice: i32) -> io::Result<()> {
    use libc::{posix_fadvise, POSIX_FADV_RANDOM, POSIX_FADV_SEQUENTIAL};
    match advice {
        ADVICE_RANDOM => {
            // Enable read-ahead
            unsafe {
                let result = posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(result))
                }
            }
        }
        ADVICE_SEQUENTIAL => {
            // Enable read-ahead
            unsafe {
                let result = posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(result))
                }
            }
        }
        _ => Ok(()), // Handle all other cases without doing anything
    }
}

fn sequential_read(file: &mut File, buffer: &mut [u8], use_fadvise: bool) -> io::Result<()> {
    if use_fadvise {
        advise_file(file.as_raw_fd(), ADVICE_SEQUENTIAL)?;
    }

    while file.read(buffer)? > 0 {}

    Ok(())
}

fn random_read(file: &mut File, buffer: &mut [u8], use_fadvise: bool) -> io::Result<()> {
    let file_size = file.metadata()?.len();
    let mut rng = rand::thread_rng();

    if use_fadvise {
        advise_file(file.as_raw_fd(), ADVICE_RANDOM)?;
    }

    for _ in 0..10000 {
        // Adjust the number of reads based on your benchmarking needs
        let offset = rng.gen_range(0..file_size);
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(buffer)?;
    }

    Ok(())
}

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("posix_fadvise");
    let file_path = "large_file.dat";
    let file_size = 512 * 1024 * 1024;
    // Create a large file
    create_large_file(file_path, file_size).unwrap();

    // Open the file for reading
    let mut file = File::open(file_path).unwrap();

    let mut buffer = vec![0; 4096]; // Adjust buffer size as needed

    group.bench_function("Sequential Read with fadvise", |b| {
        b.iter(|| sequential_read(&mut file, &mut buffer, true).unwrap())
    });
    group.bench_function("Sequential Read without fadvise", |b| {
        b.iter(|| sequential_read(&mut file, &mut buffer, false).unwrap())
    });
    group.bench_function("Random Read with fadvise", |b| {
        b.iter(|| random_read(&mut file, &mut buffer, true).unwrap())
    });
    group.bench_function("Random Read without fadvise", |b| {
        b.iter(|| random_read(&mut file, &mut buffer, false).unwrap())
    });

    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
