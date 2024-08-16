use ndarray::Array1;

pub(crate) fn z_normalize(x: &Array1<f64>, y: &Array1<f64>) -> (Array1<f64>, Array1<f64>) {
    let x_mean = x.mean().unwrap();
    let x_std = *x.std_axis(ndarray::Axis(0), 0.).iter().next().unwrap();

    return ((x - x_mean) / x_std, (y - x_mean) / x_std);
}

pub(crate) fn linf_dist(x: &Array1<f64>, y: &Array1<f64>) -> f64 {
    let v = (x - y).mapv(f64::abs);
    return v.fold(f64::NEG_INFINITY, |acc, elem| acc.max(*elem));
}

pub(crate) fn l1_dist(x: &Array1<f64>, y: &Array1<f64>) -> f64 {
    return (x - y).mapv(f64::abs).sum();
}
