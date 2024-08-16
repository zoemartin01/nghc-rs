use polars::prelude::*;
use std::{collections::HashMap, fs, io::Read, path::PathBuf};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "preprocess", about = "Preprocess input files")]
pub struct Preprocess {
    #[structopt(short = "i", parse(from_os_str), name = "input root directory")]
    pub input: PathBuf,
    #[structopt(short = "o", parse(from_os_str))]
    pub output: PathBuf,
    #[structopt(name = "input gzipped", short = "g")]
    pub gzip: bool,
    #[structopt(name = "continue", short = "c", long = "continue")]
    pub cont: bool,
    #[structopt(name = "duckdb", short = "d", long = "duckdb")]
    pub duckdb: bool,
}

pub fn preprocess(input: PathBuf, output: PathBuf, gzip: bool, cont: bool, duckdb: bool) {
    for n in 1..6 {
        let files = fs::read_dir(input.join(n.to_string())).unwrap();

        let outdir = output.join(format!("n={}", n));
        fs::create_dir_all(&outdir).unwrap();

        for file in files {
            let path = file.unwrap().path();
            let outpath = outdir.join(path.with_extension("parquet").file_name().unwrap());

            if outpath.exists() && cont {
                continue;
            }

            let mut buf = String::new();
            if gzip {
                let mut fd = flate2::read::GzDecoder::new(fs::File::open(&path).unwrap());

                fd.read_to_string(&mut buf).unwrap();
            } else {
                let mut fd = fs::File::open(&path).unwrap();

                fd.read_to_string(&mut buf).unwrap();
            }

            let mapped: (Vec<String>, Vec<Series>) = buf.lines().map(process_line).unzip();

            let ngrams = Series::new("ngram", mapped.0);
            let frequencies = Series::new("frequency", mapped.1);
            let mut df = DataFrame::new(vec![ngrams, frequencies]).unwrap();

            let mut f = fs::File::create(&outpath).unwrap();
            ParquetWriter::new(&mut f)
                .with_compression(ParquetCompression::Uncompressed)
                .finish(&mut df)
                .expect("writing parquet file");
        }
    }

    if duckdb {
        let conn = duckdb::Connection::open(&output.with_extension("db")).unwrap();

        let mut stmt = conn
            .prepare(
                format!(
                    "CREATE TABLE ngrams AS SELECT * FROM read_parquet('{}/*/*.parquet')",
                    output.display()
                )
                .as_str(),
            )
            .unwrap();

        stmt.execute([]).unwrap();
    }
}

fn process_line(line: &str) -> (String, Series) {
    let mut iter = line.split('\t');
    let ngram = iter.next().unwrap().to_string();

    let frequencies = iter
        .map(|x| x.split(','))
        .filter_map(|mut x| {
            let year = x.next().unwrap().parse::<usize>().unwrap();

            if year < 1800 || year > 2000 {
                return None;
            }

            let freq = x.next().unwrap();

            Some((year - 1800, freq.parse::<u64>().unwrap()))
        })
        .collect::<HashMap<usize, u64>>();

    let mut arr: [u64; 201] = [0; 201];

    for (year, freq) in frequencies {
        arr[year] = freq;
    }

    return (ngram, Series::new("frequency", arr));
}
