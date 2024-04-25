mod append_only_store;

use append_only_store::AppendOnlyStore;
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};
use rand::prelude::*;
use dashmap::DashMap;

const N_THREADS: usize = 16;
const OPS: usize = 200_000;
const MIX_READS_PER_WRITE: usize = 4;
const VAL_SIZE: usize = 2048;

fn main() {
    // WRITE

    {
        let t = std::time::Instant::now();
        let store = Arc::new(AppendOnlyStore::new(10_000));
        let mut threads = Vec::with_capacity(N_THREADS);
        for _ in 0..N_THREADS {
            let store = store.clone();
            threads.push(std::thread::spawn(move || append_only_write_heavy(store)));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        println!("append_only write: {}us", t.elapsed().as_micros());
    }
    {
        let t = std::time::Instant::now();
        let vec = Arc::new(Mutex::new(Vec::with_capacity(10_000)));
        let mut threads = Vec::with_capacity(N_THREADS);

        for _ in 0..N_THREADS {
            let vec = vec.clone();
            threads.push(std::thread::spawn(move || mutex_vec_write_heavy(vec)));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        println!("mutex vec write: {}us", t.elapsed().as_micros());
    }
    {
        let t = std::time::Instant::now();
        let map = Arc::new(DashMap::with_capacity(10_000));
        let mut threads = Vec::with_capacity(N_THREADS);

        for t_id in 0..N_THREADS {
            let map = map.clone();
            threads.push(std::thread::spawn(move || dashmap_write_heavy(t_id, map)));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        println!("dashmap write: {}us", t.elapsed().as_micros());
    }

    // MIXED 
    {
        let t = std::time::Instant::now();
        let store = Arc::new(AppendOnlyStore::new(10_000));
        let mut threads = Vec::with_capacity(N_THREADS);

        for _ in 0..N_THREADS {
            let store = store.clone();
            threads.push(std::thread::spawn(move || append_only_mixed(store)));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        println!("append_only mixed: {}us", t.elapsed().as_micros());
    }
    {
        let t = std::time::Instant::now();
        let vec = Arc::new(Mutex::new(Vec::with_capacity(10_000)));
        let mut threads = Vec::with_capacity(N_THREADS);

        for _ in 0..N_THREADS {
            let vec = vec.clone();
            threads.push(std::thread::spawn(move || mutex_vec_mixed(vec)));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        println!("mutex vec mixed: {}us", t.elapsed().as_micros());
    }
    {
        let t = std::time::Instant::now();
        let vec = Arc::new(RwLock::new(Vec::with_capacity(10_000)));
        let mut threads = Vec::with_capacity(N_THREADS);

        for _ in 0..N_THREADS {
            let vec = vec.clone();
            threads.push(std::thread::spawn(move || rwlock_vec_mixed(vec)));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        println!("rwlock vec mixed: {}us", t.elapsed().as_micros());
    }
    {
        let t = std::time::Instant::now();
        let map = Arc::new(DashMap::with_capacity(10_000));
        let mut threads = Vec::with_capacity(N_THREADS);

        for t_id in 0..N_THREADS {
            let map = map.clone();
            threads.push(std::thread::spawn(move || dashmap_mixed(t_id, map)));
        }

        for thread in threads {
            thread.join().unwrap();
        }

        println!("dashmap mixed: {}us", t.elapsed().as_micros());
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

fn dashmap_write_heavy(t_id: usize, map: Arc<DashMap<(usize, usize), [u8; VAL_SIZE]>>) {
    for op in 0..OPS {
        map.insert((t_id, op), value());
    }
}

fn append_only_mixed(store: Arc<AppendOnlyStore<[u8; VAL_SIZE]>>) {
    let mut max = None;
    for op in 0..OPS {
        if max.is_some() && mix_read(op) {
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
        if max.is_some() && mix_read(op) {
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
        if max.is_some() && mix_read(op) {
            let idx = rand::thread_rng().gen_range(0..max.unwrap() + 1);
            let _ = vec.read()[idx];
        } else {
            let mut vec = vec.write();
            vec.push(value());
            max = Some(vec.len() - 1);
        }
    }
}

fn dashmap_mixed(t_id: usize, map: Arc<DashMap<(usize, usize), [u8; VAL_SIZE]>>) {
    let mut max = 0;
    for op in 0..OPS {
        if max > 0 && mix_read(op) {
            let t_id = rand::thread_rng().gen_range(0..N_THREADS);
            let idx = rand::thread_rng().gen_range(0..max + 1);
            let _ = map.get(&(t_id, idx));
            max += 1;
        } else {
            map.insert((t_id, op), value());
        }
    }
}

fn mix_read(i: usize) -> bool {
    i % (MIX_READS_PER_WRITE + 1) != 0
}

fn value() -> [u8; 2048] {
    std::hint::black_box([0u8; 2048])
}