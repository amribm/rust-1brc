use std::collections::BTreeMap;
use std::fmt::Display;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Take};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use std::{fs::File, io::Result};

fn main() {
    let start = Instant::now();

    let path = Path::new("/Users/ameer/proj/1brc/measurements.txt");

    let file = File::open(path).expect("unable to read file");
    let file_size = file.metadata().unwrap().size();

    let reader = BufReader::new(file);

    let available_cpus = std::thread::available_parallelism().unwrap();

    let intervals = get_intervals_for_cpus(available_cpus.into(), file_size, reader).unwrap();

    let mut result = Arc::new(Mutex::new(Vec::new()));

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

type StationsMap = BTreeMap<String, City>;

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
    min: f64,
    max: f64,
    total: f64,
    count: u64,
}

impl Default for City {
    fn default() -> Self {
        City {
            min: f64::MAX,
            max: f64::MIN,
            total: 0.0,
            count: 0,
        }
    }
}

impl City {
    fn update(&mut self, temp: f64) {
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
        let avg_temp = self.total / (self.count as f64);
        write!(f, "{:.1}{avg_temp}{:.1}", self.min, self.max)
    }
}

fn create_shared_map(reader: Take<BufReader<File>>) -> Result<StationsMap> {
    let mut shared_map: BTreeMap<String, City> = BTreeMap::new();
    for line in reader.lines() {
        let line = line?;
        let (city, temp) = line.split_once(";").unwrap();

        let parsed_temp: f32 = temp.parse().unwrap();

        shared_map
            .entry(city.to_string())
            .or_default()
            .update(parsed_temp as f64);
    }
    Ok(shared_map)
}

fn print_shared_map(map: StationsMap) {
    print!("{}", "{");
    for (i, (name, city)) in map.into_iter().enumerate() {
        if i == 0 {
            print!("{name}={city}");
        } else {
            print!(", {name}={city}");
        }
    }
    print!("{}", "}");
}

fn merge_map(a: StationsMap, b: &StationsMap) -> StationsMap {
    let mut merged_map = a;

    for (k, v) in b {
        merged_map.entry(k.into()).or_default().merge(v);
    }
    merged_map
}
