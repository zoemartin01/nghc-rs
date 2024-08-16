mod optimize;
mod preprocessing;

use crate::optimize::Optimize;
use preprocessing::Preprocess;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum Opt {
    Preprocess(Preprocess),
    Optimize(Optimize),
}
fn main() {
    match Opt::from_args() {
        Opt::Preprocess(preprocess) => {
            preprocessing::preprocess(
                preprocess.input,
                preprocess.output,
                preprocess.gzip,
                preprocess.cont,
                preprocess.duckdb,
            );
        }
        Opt::Optimize(optimize) => {
            optimize::optimize(
                optimize.input,
                optimize.output,
                optimize.chunk_size,
                optimize.error_bound,
                optimize.verbose_output,
                optimize.output_all,
                optimize.core_count,
            );
        }
    }
}
