mod append_only_store;

use append_only_store::AppendOnlyStore;
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};
use rand::prelude::*;

const N_THREADS: usize = 16;
const OPS: usize = 2_000_000;
const MIX_FREQ: usize = 3; // 2 reads per write
const VAL_SIZE: usize = 2048;

fn main() {
    {
        let t = std::time::Instant::now();
        let store = Arc::new(AppendOnlyStore::new(10_000));
        for _ in 0..N_THREADS {
            let store = store.clone();
            std::thread::spawn(move || append_only_write_heavy(store));
        }

        println!("append_only write: {}us", t.elapsed().as_micros());
    }
    {
        let t = std::time::Instant::now();
        let vec = Arc::new(Mutex::new(Vec::with_capacity(10_000)));
        for _ in 0..N_THREADS {
            let vec = vec.clone();
            std::thread::spawn(move || mutex_vec_write_heavy(vec));
        }

        println!("mutex vec write: {}us", t.elapsed().as_micros());
    }
    {
        let t = std::time::Instant::now();
        let store = Arc::new(AppendOnlyStore::new(10_000));
        for _ in 0..N_THREADS {
            let store = store.clone();
            std::thread::spawn(move || append_only_mixed(store));
        }

        println!("append_only mixed: {}us", t.elapsed().as_micros());
    }
    {
        let t = std::time::Instant::now();
        let vec = Arc::new(Mutex::new(Vec::with_capacity(10_000)));
        for _ in 0..N_THREADS {
            let vec = vec.clone();
            std::thread::spawn(move || mutex_vec_mixed(vec));
        }

        println!("mutex vec mixed: {}us", t.elapsed().as_micros());
    }
    {
        let t = std::time::Instant::now();
        let vec = Arc::new(RwLock::new(Vec::with_capacity(10_000)));
        for _ in 0..N_THREADS {
            let vec = vec.clone();
            std::thread::spawn(move || rwlock_vec_mixed(vec));
        }

        println!("rwlock vec mixed: {}us", t.elapsed().as_micros());
    }
}

fn append_only_write_heavy(store: Arc<AppendOnlyStore<[u8; VAL_SIZE]>>) {
    for _ in 0..OPS {
        store.push(value());
    }
}

fn mutex_vec_write_heavy(vec: Arc<Mutex<Vec<[u8; VAL_SIZE]>>>) {
    for _ in 0..OPS {
        vec.lock().push(value());
    }
}

fn append_only_mixed(store: Arc<AppendOnlyStore<[u8; VAL_SIZE]>>) {
    let mut max = None;
    for op in 0..OPS {
        if max.is_some() && op % MIX_FREQ != 0 {
            let idx = rand::thread_rng().gen_range(0..max.unwrap() + 1);
            store.get(idx);
        } else {
            max = Some(store.push(value()));
        }
    }
}

fn mutex_vec_mixed(vec: Arc<Mutex<Vec<[u8; VAL_SIZE]>>>) {
    let mut max = None;
    for op in 0..OPS {
        if max.is_some() && op % MIX_FREQ != 0 {
            let idx = rand::thread_rng().gen_range(0..max.unwrap() + 1);
            let _ = vec.lock()[idx];
        } else {
            let mut vec = vec.lock();
            vec.push(value());
            max = Some(vec.len() - 1);
        }
    }
}

fn rwlock_vec_mixed(vec: Arc<RwLock<Vec<[u8; VAL_SIZE]>>>) {
    let mut max = None;
    for op in 0..OPS {
        if max.is_some() && op % MIX_FREQ != 0 {
            let idx = rand::thread_rng().gen_range(0..max.unwrap() + 1);
            let _ = vec.read()[idx];
        } else {
            let mut vec = vec.write();
            vec.push(value());
            max = Some(vec.len() - 1);
        }
    }
}

fn value() -> [u8; 2048] {
    std::hint::black_box([0u8; 2048])
}