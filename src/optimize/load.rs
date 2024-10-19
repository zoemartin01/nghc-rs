use super::util::get_children;
use cached::proc_macro::cached;
use duckdb::{params, params_from_iter, types::Value, Config, Connection, Rows, Statement};
use hashbrown::{HashMap, HashSet};
use polars::prelude::*;
use std::path::PathBuf;

pub trait Load {
    fn get_count(&self, n: usize) -> usize;
    fn get_slice(&self, limit: usize, offset: usize, n: u8) -> HashMap<String, [f64; 201]>;
    fn get_frequencies(&self, ngrams: &HashMap<String, [f64; 201]>) -> HashMap<String, [f64; 201]>;
}

pub(crate) struct ParquetLoader {
    pub input: PathBuf,
}

pub(crate) struct DuckDBLoader {
    pub input: PathBuf,
}

pub struct Loader {
    pub loader: Box<dyn Load>,
}

unsafe impl Sync for Loader {}

impl Loader {
    pub fn new(loader: Box<dyn Load>) -> Self {
        Loader { loader }
    }
}

impl Load for Loader {
    fn get_count(&self, n: usize) -> usize {
        self.loader.get_count(n)
    }

    fn get_slice(&self, limit: usize, offset: usize, n: u8) -> HashMap<String, [f64; 201]> {
        self.loader.get_slice(limit, offset, n)
    }

    fn get_frequencies(&self, ngrams: &HashMap<String, [f64; 201]>) -> HashMap<String, [f64; 201]> {
        self.loader.get_frequencies(ngrams)
    }
}

impl Load for DuckDBLoader {
    fn get_slice(&self, limit: usize, offset: usize, n: u8) -> HashMap<String, [f64; 201]> {
        let conn: Connection;
        let mut query: Statement;

        conn = Connection::open_with_flags(
            &self.input.to_str().unwrap(),
            Config::default()
                .access_mode(duckdb::AccessMode::ReadOnly)
                .unwrap(),
        )
        .unwrap();

        query = conn
            .prepare(&format!(
                "SELECT ngram, frequency FROM ngrams WHERE n == {} LIMIT {} OFFSET {}",
                n, limit, offset
            ))
            .unwrap();

        query
            .query_map([], row_map)
            .unwrap()
            .map(|x| x.unwrap())
            .collect::<HashMap<_, _>>()
    }

    fn get_count(&self, n: usize) -> usize {
        let conn: Connection;
        let mut query: Statement;
        let mut result: Rows;

        conn = Connection::open_with_flags(
            &self.input.to_str().unwrap(),
            Config::default()
                .access_mode(duckdb::AccessMode::ReadOnly)
                .unwrap(),
        )
        .unwrap();

        query = conn
            .prepare(&format!("SELECT count(*) FROM ngrams WHERE n == ?",))
            .unwrap();

        result = query.query(params![n]).unwrap();
        result.next().unwrap().unwrap().get(0).unwrap()
    }

    fn get_frequencies(&self, ngrams: &HashMap<String, [f64; 201]>) -> HashMap<String, [f64; 201]> {
        let wanted = ngrams
            .iter()
            .map(|(ngram, _)| get_children(ngram, false, &vec![]))
            .flatten()
            .collect::<HashSet<String>>();

        let conn = Connection::open_with_flags(
            &self.input.to_str().unwrap(),
            Config::default()
                .access_mode(duckdb::AccessMode::ReadOnly)
                .unwrap(),
        )
        .unwrap();

        let mut map = wanted
            .iter()
            .fold(HashMap::new(), |mut acc, ngram| {
                let n = ngram.split_ascii_whitespace().count();
                let range = acc.entry(n).or_insert_with(|| vec![]);

                range.push(ngram.to_string());

                return acc;
            })
            .iter()
            .flat_map(|(n, ngrams)| {
                let mut stmt = conn
                    .prepare(
                        format!(
                            "SELECT ngram, frequency FROM ngrams WHERE n == {} AND ngram in ({})",
                            n,
                            (0..ngrams.len()).map(|_| "?").collect::<Vec<_>>().join(",")
                        )
                        .as_str(),
                    )
                    .unwrap();

                return stmt
                    .query_map(params_from_iter(ngrams.iter()), row_map)
                    .unwrap()
                    .map(|x| x.unwrap())
                    .collect::<HashMap<String, [f64; 201]>>();
            })
            .collect::<HashMap<String, [f64; 201]>>();

        map.extend(ngrams.clone());

        return map;
    }
}

impl Load for ParquetLoader {
    fn get_count(&self, n: usize) -> usize {
        let conn: Connection;
        let mut query: Statement;
        let mut result: Rows;

        conn = Connection::open_in_memory().unwrap();

        query = conn
            .prepare(&format!(
                "SELECT count(*) FROM read_parquet('{}/*/*.parquet') WHERE n == {}",
                &self.input.to_str().unwrap(),
                n,
            ))
            .unwrap();

        result = query.query([]).unwrap();

        result.next().unwrap().unwrap().get(0).unwrap()
    }

    fn get_slice(&self, limit: usize, offset: usize, n: u8) -> HashMap<String, [f64; 201]> {
        let conn: Connection;
        let mut query: Statement;

        conn = Connection::open_in_memory().unwrap();

        query = conn
                .prepare(&format!(
                    "SELECT ngram, frequency FROM read_parquet('{}/*/*.parquet') WHERE n == {} LIMIT {} OFFSET {}",
                    &self.input.to_str().unwrap(),
                    n,
                    limit,
                    offset
                ))
                .unwrap();

        query
            .query_map([], row_map)
            .unwrap()
            .map(|x| x.unwrap())
            .collect::<HashMap<_, _>>()
    }

    fn get_frequencies(&self, ngrams: &HashMap<String, [f64; 201]>) -> HashMap<String, [f64; 201]> {
        #[cached]
        fn start_values(input: PathBuf) -> HashMap<String, Vec<(String, PathBuf)>> {
            println!("Scanning input directory");
            input
                .read_dir()
                .unwrap()
                .map(|e| {
                    let entry = e.unwrap();
                    let dir = entry.path();
                    let mut firsts = dir
                        .read_dir()
                        .unwrap()
                        .map(|f| {
                            let path = f.unwrap().path();

                            let lazy = LazyFrame::scan_parquet(&path, Default::default()).unwrap();

                            return (
                                lazy.select(&[col("ngram")])
                                    .limit(1)
                                    .collect()
                                    .unwrap()
                                    .column("ngram")
                                    .unwrap()
                                    .str()
                                    .unwrap()
                                    .iter()
                                    .next()
                                    .unwrap()
                                    .unwrap()
                                    .to_owned(),
                                path,
                            );
                        })
                        .collect::<Vec<_>>();

                    firsts.sort_by_cached_key(|(ngram, _)| ngram.clone());
                    return (entry.file_name().to_str().unwrap().to_string(), firsts);
                })
                .collect::<HashMap<String, Vec<_>>>()
        }

        let wanted = ngrams
            .iter()
            .map(|(ngram, _)| get_children(ngram, false, &vec![]))
            .flatten()
            .collect::<HashSet<String>>();

        let conn = Connection::open_in_memory().unwrap();

        let file_ranges = start_values(self.input.clone());

        let files = wanted
            .iter()
            .map(|ngram| {
                let n = ngram.split_ascii_whitespace().count();
                let range = &file_ranges[format!("n={}", n).as_str()];
                let idx = match range.binary_search_by_key(&ngram, |(ngram, _)| ngram) {
                    Ok(i) => i,
                    Err(i) => match range.len() - 1 {
                        0 => 0,
                        _ => i - 1,
                    },
                };

                return (ngram.to_string(), range[idx].1.clone());
            })
            .collect::<Vec<_>>();

        let mut map = files
            .iter()
            .map(|(ngram, file)| {
                let mut query = conn
                    .prepare(&format!(
                        "SELECT ngram, frequency FROM read_parquet('{}') WHERE ngram == ?",
                        file.to_str().unwrap()
                    ))
                    .unwrap();

                query
                    .query_map(params![&ngram], row_map)
                    .unwrap()
                    .map(|x| x.unwrap())
                    .next()
                    .unwrap()
            })
            .collect::<HashMap<String, [f64; 201]>>();

        map.extend(ngrams.clone());

        return map;
    }
}

fn row_map(row: &duckdb::Row) -> Result<(String, [f64; 201]), duckdb::Error> {
    let freq = match row.get(1).unwrap() {
        Value::List(vec) => vec
            .iter()
            .map(|x| match x {
                Value::UBigInt(i) => *i as f64,
                _ => 0.,
            })
            .collect(),
        _ => vec![0.],
    };
    let ngram: String = row.get(0)?;
    let frequency: [f64; 201] = freq.try_into().unwrap();
    Ok((ngram, frequency))
}
