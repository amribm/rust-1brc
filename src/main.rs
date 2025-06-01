use std::collections::BTreeMap;
use std::fmt::Display;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;
use std::{
    fs,
    fs::File,
    io::{Read, Result},
};

fn main() {
    let start = Instant::now();

    let path = Path::new("/Users/ameer/proj/1brc/measurements.txt");

    let file = File::open(path).expect("unable to read file");

    let reader = BufReader::new(file);

    let shared_map = create_shared_map(reader).unwrap();

    print_shared_map(shared_map);

    println!("{:?}", start.elapsed());
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
}

impl Display for City {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let avg_temp = self.total / (self.count as f64);
        write!(f, "{:.1}{avg_temp}{:.1}", self.min, self.max)
    }
}

fn create_shared_map(reader: BufReader<File>) -> Result<BTreeMap<String, City>> {
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

fn print_shared_map(map: BTreeMap<String, City>) {
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
