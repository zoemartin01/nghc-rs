use core::f64;

#[derive(Debug)]
pub(crate) struct Solution {
    pub ngram: String,
    pub coefficients: Vec<Coefficient>,
    pub original: [f64; 201],
    pub calculated: [f64; 201],
    pub error: f64,
    pub rmse: f64,
    pub summed_error: f64,
}

impl Solution {
    pub fn unsolved(ngram: &str, original: &[f64; 201]) -> Self {
        return Solution {
            ngram: ngram.to_string(),
            coefficients: Vec::new(),
            original: original.clone(),
            calculated: [0.0; 201],
            error: f64::INFINITY,
            rmse: f64::INFINITY,
            summed_error: f64::INFINITY,
        };
    }
}

impl Clone for Solution {
    fn clone(&self) -> Self {
        return Solution {
            ngram: self.ngram.clone(),
            coefficients: self.coefficients.clone(),
            original: self.original.clone(),
            calculated: self.calculated,
            error: self.error,
            rmse: self.rmse,
            summed_error: self.summed_error,
        };
    }
}

#[derive(Debug)]
pub(crate) struct Coefficient {
    pub token: String,
    pub coefficient: f64,
}

impl Clone for Coefficient {
    fn clone(&self) -> Self {
        return Coefficient {
            token: self.token.clone(),
            coefficient: self.coefficient,
        };
    }
}
