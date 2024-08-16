pub fn get_children(ngram: &str, child: bool) -> Vec<String> {
    if ngram.split_ascii_whitespace().count() == 1 {
        return vec![ngram.to_string()];
    }

    if !child {
        return [
            get_children(ngram.rsplit_once(' ').unwrap().0, true),
            get_children(ngram.split_once(' ').unwrap().1, true),
        ]
        .concat();
    }

    return [
        get_children(ngram.rsplit_once(' ').unwrap().0, true),
        get_children(ngram.split_once(' ').unwrap().1, true),
        vec![ngram.to_string()],
    ]
    .concat();
}
