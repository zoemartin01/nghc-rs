use super::load::{DuckDBLoader, Load, Loader, ParquetLoader};
use super::math::{l1_dist, linf_dist, rmse, z_normalize};
use super::solution::{Coefficient, Solution};
use super::util::get_children;
use cfg_if::cfg_if;
use hashbrown::HashMap;
use highs::{RowProblem, Sense};
use ndarray::{arr1, arr2};
use polars::prelude::*;
use rayon::prelude::*;
use std::ffi::OsStr;
use std::mem::size_of_val;
use std::{fs, path::PathBuf};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "optimize", about = "Optimize a file")]
pub struct Optimize {
    #[structopt(short = "i", parse(from_os_str))]
    pub input: PathBuf,
    #[structopt(short = "o", parse(from_os_str))]
    pub output: PathBuf,
    #[structopt(short = "c", default_value = "2500000")]
    pub chunk_size: u64,
    #[structopt(short = "b", default_value = "0.5")]
    pub error_bound: f64,
    #[structopt(short = "V")]
    pub verbose_output: bool,
    #[structopt(short = "a", long = "output-all")]
    pub output_all: bool,
    #[structopt(short = "C", long = "cores")]
    pub core_count: Option<usize>,
}

pub fn optimize(
    input: PathBuf,
    output: PathBuf,
    chunk_size: u64,
    error_bound: f64,
    verbose_output: bool,
    output_all: bool,
    core_count: Option<usize>,
) {
    cfg_if! {
        if #[cfg(any(feature = "non-selective", feature = "direct-children"))] {
            let mut compressed_frequencies_map: HashMap<String, [f64; 201]> = HashMap::new();
            let compressed_frequencies: Vec<String> = Vec::new();
        } else {
            let mut compressed_frequencies: Vec<String> = Vec::new();
        }
    }

    let loader = Loader::new(match input.extension().and_then(OsStr::to_str) {
        Some("db") => Box::new(DuckDBLoader { input }),
        _ => Box::new(ParquetLoader { input }),
    });

    let compressed_schema = Schema::from_iter(
        vec![
            Field::new("ngram", DataType::String),
            Field::new(
                "coefficients",
                DataType::List(Box::new(DataType::Struct(vec![
                    Field::new("token", DataType::String),
                    Field::new("coefficient", DataType::Float64),
                ]))),
            ),
            Field::new("error", DataType::Float64),
            Field::new("rmse", DataType::Float64),
            Field::new("summed_error", DataType::Float64),
        ][..(if verbose_output { 5 } else { 2 })]
            .to_vec(),
    );

    let uncompressed_schema = Schema::from_iter(vec![
        Field::new("ngram", DataType::String),
        Field::new(
            "frequency",
            DataType::Array(Box::new(DataType::Float64), 201),
        ),
    ]);

    for n in 1..6 {
        let count = loader.get_count(n);
        let cpu_count = core_count.unwrap_or_else(|| num_cpus::get() / 2);

        let rel_chunk_size = (count as u64).min(chunk_size) / cpu_count as u64;

        let outdir_compressed = output.join("compressed").join(format!("n={}", n));
        let outdir_uncompressed = output.join("uncompressed").join(format!("n={}", n));
        std::fs::create_dir_all(&outdir_compressed).unwrap();
        std::fs::create_dir_all(&outdir_uncompressed).unwrap();

        for i in (0..count).step_by(chunk_size as usize) {
            let solutions = (0..=cpu_count)
                .into_par_iter()
                .map(|j| {
                    let chunk = loader.get_slice(
                        rel_chunk_size as usize,
                        i + j * rel_chunk_size as usize,
                        n as u8,
                    );

                    cfg_if! {
                        if #[cfg(any(feature = "non-selective", feature = "direct-children"))] {
                            let mut frequencies = loader.get_frequencies(&chunk);
                            frequencies.extend(compressed_frequencies_map.clone());
                        } else if #[cfg(feature = "highly-selective")] {
                            let frequencies = loader.get_frequencies(&chunk);
                        } else {
                            let mut frequencies = loader.get_frequencies(&chunk);
                            frequencies.extend(compressed_frequencies.clone().iter()
                                .map(|x| (x.to_string(), [0.; 201]))
                                .collect::<HashMap<_, _>>());
                        }
                    }

                    return chunk
                        .into_par_iter()
                        .map(|(ngram, _)| {
                            minimize_abs_error(&ngram, &frequencies, &compressed_frequencies)
                        })
                        .collect::<Vec<_>>();
                })
                .flatten()
                .collect::<Vec<_>>();

            cfg_if! {
                if #[cfg(any(feature = "non-selective", feature = "direct-children"))] {
                    compressed_frequencies_map.extend(
                        solutions
                            .clone()
                            .into_par_iter()
                            .map(|sol| match sol.error <= error_bound {
                                true => Some((sol.ngram.clone(), sol.calculated)),
                                false => None,
                            })
                            .filter_map(|x| x)
                            .collect::<HashMap<_, _>>(),
                    );
                } else {
                    compressed_frequencies.extend(
                        solutions
                            .clone()
                            .into_par_iter()
                            .map(|sol| match sol.error <= error_bound {
                                true => Some(sol.ngram),
                                false => None,
                            })
                            .filter_map(|x| x)
                            .collect::<Vec<_>>(),
                    );
                }
            }

            let compressed = solutions.clone().into_par_iter()
                .filter(|sol| output_all || sol.error <= error_bound)
                .map(|sol|polars::frame::row::Row::new(vec![
                    AnyValue::StringOwned(sol.ngram.clone().into()),
                    AnyValue::List(
                        df![
                            "token" => sol.coefficients.iter().map(|x| x.token.clone()).collect::<Vec<_>>(),
                            "coefficient" => sol.coefficients.iter().map(|x| x.coefficient).collect::<Vec<_>>(),
                            ].unwrap().into_struct("coefficients").into_series(),
                        ),
                    AnyValue::Float64(sol.error),
                    AnyValue::Float64(sol.rmse),
                    AnyValue::Float64(sol.summed_error),
                ][..(if verbose_output { 5 } else { 2 })].to_vec()))
                .collect::<Vec<_>>();

            write(
                &compressed,
                &compressed_schema,
                outdir_compressed.join(format!("{}.parquet", i)),
            );

            let uncompressed = solutions
                .into_par_iter()
                .filter(|sol| sol.error > error_bound)
                .map(|sol| {
                    polars::frame::row::Row::new(vec![
                        AnyValue::StringOwned(sol.ngram.clone().into()),
                        AnyValue::Array(sol.original.iter().collect(), 201),
                    ])
                })
                .collect::<Vec<_>>();

            write(
                &uncompressed,
                &uncompressed_schema,
                outdir_uncompressed.join(format!("{}.parquet", i)),
            );

            println!(
                "compressed_frequencies size: {}B",
                size_of_val(&*compressed_frequencies)
            );
        }
    }
}

fn write(rows: &Vec<polars::frame::row::Row>, schema: &Schema, path: PathBuf) {
    let mut f = fs::File::create(path).unwrap();
    let mut df = DataFrame::from_rows_and_schema(&rows, schema).unwrap();
    ParquetWriter::new(&mut f)
        .with_compression(ParquetCompression::Uncompressed)
        .finish(&mut df)
        .expect("writing parquet file");
}

fn minimize_abs_error(
    ngram: &str,
    frequencies: &HashMap<String, [f64; 201]>,
    compressed_frequencies: &Vec<String>,
) -> Solution {
    let children: Vec<String>;

    cfg_if! {
        if #[cfg(feature = "highly-selective")] {
            let _children = get_children(ngram, false, &vec![]);
            let filtered_compressed = compressed_frequencies.iter().filter(|x| _children.contains(x)).map(|x| x.to_string()).collect();
            children = get_children(ngram, false, &filtered_compressed);
        } else {
            children = get_children(ngram, false, &vec![]);
        }
    }

    let y: &[f64; 201] = frequencies.get(ngram).unwrap();

    if children.len() < 2 {
        return Solution::unsolved(ngram, y);
    }

    let child_freqs = children
        .iter()
        .map(|child| frequencies.get(child).unwrap_or_else(|| &[0.; 201]))
        .collect::<Vec<_>>();

    let (children, child_freqs): (Vec<String>, Vec<&[f64; 201]>) = children
        .iter()
        .zip(child_freqs.iter())
        .filter_map(|(child, freq)| {
            if freq.iter().all(|x| *x == 0.) {
                return None;
            }
            return Some((child.to_owned(), freq));
        })
        .unzip();

    if child_freqs.len() == 0 {
        return Solution::unsolved(ngram, y);
    }

    let mut pb = RowProblem::new();
    let mut c = child_freqs
        .iter()
        .map(|_| pb.add_column(0., 0..))
        .collect::<Vec<_>>();

    c.push(pb.add_column(1., 0..));

    for (idx, y_i) in y.iter().enumerate() {
        let mut a_i = child_freqs
            .iter()
            .enumerate()
            .map(|(i, x)| (c[i], x[idx] as f64))
            .collect::<Vec<_>>();
        a_i.push((*c.last().unwrap(), -1.));
        pb.add_row(..(*y_i as f64), a_i);
    }

    for (idx, y_i) in y.iter().enumerate() {
        let mut a_i = child_freqs
            .iter()
            .enumerate()
            .map(|(i, x)| (c[i], x[idx] as f64 * -1.))
            .collect::<Vec<_>>();
        a_i.push((*c.last().unwrap(), -1.));
        pb.add_row(..(*y_i as f64 * -1.), a_i);
    }

    let mut model = pb.optimise(Sense::Minimise);
    model.set_option("presolve", "off");
    model.set_option("simplex_scale_strategy", "4");

    let model = model.try_solve();
    if model.is_err() {
        return Solution::unsolved(ngram, y);
    }

    let sol = model.unwrap().get_solution();

    let coefs = sol
        .columns()
        .iter()
        .map(|x| x.to_owned())
        .zip(children)
        .map(|(x, y)| Coefficient {
            token: y.to_string(),
            coefficient: x,
        })
        .filter(|x| x.coefficient != 0.)
        .collect::<Vec<_>>();

    if coefs.is_empty() {
        return Solution::unsolved(ngram, y);
    }

    let x: Vec<[f64; 201]> = child_freqs
        .iter()
        .map(|x| x.to_owned().to_owned())
        .collect::<Vec<_>>();
    let c = arr1(&sol.columns()[..sol.columns().len() - 1]);
    let y_pred = c.dot(&arr2(&x));
    let y = arr1(y);

    let (y_norm, y_pred_norm) = z_normalize(&y, &y_pred);

    return Solution {
        ngram: ngram.to_string(),
        coefficients: coefs,
        error: linf_dist(&y_norm, &y_pred_norm),
        summed_error: l1_dist(&y_norm, &y_pred_norm),
        rmse: rmse(&y_norm, &y_pred_norm),
        original: y.to_vec().try_into().unwrap(),
        calculated: y_pred.to_vec().try_into().unwrap(),
    };
}
