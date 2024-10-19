use cfg_if::cfg_if;

pub fn get_children(ngram: &str, _child: bool, _compressed: &Vec<String>) -> Vec<String> {
    if ngram.split_ascii_whitespace().count() == 1 {
        return vec![ngram.to_string()];
    }

    cfg_if! {
        if #[cfg(feature = "direct-children")] {
            return vec![
                ngram.rsplit_once(' ').unwrap().0.to_string(),
                ngram.split_once(' ').unwrap().1.to_string(),
            ];
        }
    }

    cfg_if! {
        if #[cfg(feature = "highly-selective")] {
            fn expand(ngram: &str, compressed: &Vec<String>, expanded: &mut Vec<String>) {
                if ngram.split_ascii_whitespace().count() == 1 {
                    expanded.push(ngram.to_string());
                    return;
                }

                let right = ngram.split_once(' ').unwrap().1;
                if !compressed.contains(&right.to_string()) {
                    expanded.push(right.to_string());
                } else {
                    expand(right, compressed, expanded);
                }

                let left = ngram.rsplit_once(' ').unwrap().0;
                if !compressed.contains(&left.to_string()) {
                    expanded.push(left.to_string());
                } else {
                    expand(left, compressed, expanded);
                }
            }

            if _compressed.len() != 0 {
                let mut expanded: Vec<String> = Vec::new();
                expand(ngram, _compressed, &mut expanded);
                return expanded;
            }
        }
    }

    if !_child {
        return [
            get_children(ngram.rsplit_once(' ').unwrap().0, true, _compressed),
            get_children(ngram.split_once(' ').unwrap().1, true, _compressed),
        ]
        .concat();
    }

    return [
        get_children(ngram.rsplit_once(' ').unwrap().0, true, _compressed),
        get_children(ngram.split_once(' ').unwrap().1, true, _compressed),
        vec![ngram.to_string()],
    ]
    .concat();
}
