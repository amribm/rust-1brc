#![feature(slice_split_once)]
use std::collections::BTreeMap;
use std::fmt::Display;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Take};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use std::{fs::File, io::Result};

type V = i32;

fn main() {
    let start = Instant::now();

    let path = Path::new("/Users/ameer/proj/1brc/measurements.txt");

    let file = File::open(path).expect("unable to read file");
    let file_size = file.metadata().unwrap().size();

    let reader = BufReader::new(file);

    let available_cpus = std::thread::available_parallelism().unwrap();

    let intervals = get_intervals_for_cpus(available_cpus.into(), file_size, reader).unwrap();

    let result = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();

    for interval in intervals {
        let results = Arc::clone(&result);

        let handle = thread::spawn(move || {
            let station_metrics = process_chunk(interval);
            results.lock().unwrap().push(station_metrics);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("thread panicked");
    }

    let result = result
        .lock()
        .unwrap()
        .iter()
        .fold(StationsMap::default(), |a, b| merge_map(a, &b));
    print_shared_map(result);
    println!("\n Execution time: {:?}", start.elapsed());
}

type StationsMap = BTreeMap<u64, City>;

fn process_chunk(interval: Interval) -> StationsMap {
    let path = Path::new("/Users/ameer/proj/1brc/measurements.txt");
    let file = File::open(path).expect("unable to read file");
    let mut reader = BufReader::new(file);

    _ = reader.seek(SeekFrom::Start(interval.start));

    let chunk_reader = reader.take(interval.end - interval.start);

    create_shared_map(chunk_reader).unwrap()
}

fn get_intervals_for_cpus(
    cpus: usize,
    file_size: u64,
    mut file: BufReader<File>,
) -> Result<Vec<Interval>> {
    let mut start = 0;
    let interval_size = file_size / cpus as u64;
    let mut intervals = Vec::new();
    let mut buf = String::new();

    for _ in 0..cpus {
        let end: u64 = start + interval_size;
        _ = file.seek(SeekFrom::Start(end));

        let bytes_from_end_to_newline = file.read_line(&mut buf)?;
        if bytes_from_end_to_newline == 0 {
            break;
        }
        // we dont want to include \n
        let end = end + (bytes_from_end_to_newline - 1) as u64;

        intervals.push(Interval { start, end });
        start = end + 1;
        buf.clear();
    }
    Ok(intervals)
}

#[derive(Debug)]
struct Interval {
    start: u64,
    end: u64,
}

struct City {
    min: V,
    max: V,
    total: V,
    count: u64,
    city: String,
}

impl Default for City {
    fn default() -> Self {
        City {
            min: V::MAX,
            max: V::MIN,
            total: 0,
            count: 0,
            city: "".to_string(),
        }
    }
}

impl City {
    fn update(&mut self, temp: V) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.total += temp;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.total += other.total;
        self.count += other.count;
    }
}

impl Display for City {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let avg_temp = format_tempreture(self.total / self.count as V);
        write!(
            f,
            "{:.1}{avg_temp}{:.1}",
            format_tempreture(self.min),
            format_tempreture(self.max)
        )
    }
}

fn create_shared_map(mut reader: Take<BufReader<File>>) -> Result<StationsMap> {
    let mut shared_map = StationsMap::new();
    let mut line = Vec::new();
    while reader.read_until(b'\n', &mut line)? != 0 {
        if line.last() == Some(&b'\n') {
            line.pop();
        }

        let (city, temp) = &line.split_once(|&c| c == b';').unwrap();

        let parsed_temp = parse_tempreture(temp);
        // let parsed_temp: f32 = temp.parse().unwrap();

        shared_map
            .entry(to_key(&city))
            .or_insert(City {
                city: str::from_utf8(&city).unwrap().to_string(),
                ..City::default()
            })
            .update(parsed_temp);

        line.clear();
    }
    Ok(shared_map)
}

fn print_shared_map(map: StationsMap) {
    print!("{}", "{");
    for (i, (_, city)) in map.into_iter().enumerate() {
        let name = &city.city;
        if i == 0 {
            print!("{name}={city}");
        } else {
            print!(", {name}={city}");
        }
    }
    print!("{}", "}");
}

fn format_tempreture(temp: V) -> String {
    format!("{:.1}", temp as f64 / 10.0)
}

fn merge_map(a: StationsMap, b: &StationsMap) -> StationsMap {
    let mut merged_map = a;

    for (k, v) in b {
        merged_map
            .entry(*k)
            .or_insert(City {
                city: v.city.clone(),
                ..City::default()
            })
            .merge(v);
    }
    merged_map
}

fn parse_tempreture(mut s: &[u8]) -> V {
    let neg = if s[0] == b'-' {
        s = &s[1..];
        true
    } else {
        false
    };

    let (a, b, c) = match s {
        [a, b, b'.', c] => (a - b'0', b - b'0', c - b'0'),
        [a, b'.', b] => (0, a - b'0', b - b'0'),
        _ => panic!("unknown pattern {:?}", s),
    };

    let v = (a as V) * 100 + (b as V) * 10 + (c as V);

    if neg {
        -v
    } else {
        v
    }
}

fn to_key(data: &[u8]) -> u64 {
    let mut hash = 0u64;

    let len = data.len();

    unsafe {
        if len > 8 {
            hash = *(data.as_ptr() as *const u64);
        } else {
            for i in 0..len {
                hash |= (*data.get_unchecked(i) as u64) << (i * 8);
            }
        }
    }

    hash ^= len as u64;
    hash
}
